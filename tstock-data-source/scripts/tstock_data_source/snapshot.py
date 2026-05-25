"""快照编排器：从各数据源获取数据，组装完整快照。"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from tstock.utils import normalize_code

from tstock_data_source.cache import load_cache, save_cache
from tstock_data_source.providers.akshare import (
    get_basic_from_ak, get_market_from_ak, get_valuation, get_financial,
)
from tstock_data_source.providers.baostock import get_basic_from_bs, get_market_from_bs, get_baostock_financial
from tstock_data_source.providers.iwencai import get_iwencai_enrichment
from tstock_data_source.valuation import get_valuation_stable
from tstock_data_source.transform import transform_snapshot

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "china_stock_data_v1"


def completeness_score(snapshot: Dict[str, Any]) -> float:
    blocks = ["basic", "market", "valuation", "financial"]
    ok = 0
    for b in blocks:
        if snapshot.get(b):
            ok += 1
    return round(ok / len(blocks), 2)


def fetch_stock_snapshot(code: str, data_type: str = "core", years: int = 3, use_cache: bool = True) -> Dict[str, Any]:
    code = normalize_code(code)

    if use_cache:
        cached = load_cache(code, data_type)
        if cached:
            return cached

    errors: List[str] = []
    sources: List[str] = []

    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": f"{code}-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "as_of": datetime.now().isoformat(),
        "code": code,
        "data_type": data_type,
    }

    # basic
    basic = {}
    try:
        basic = get_basic_from_ak(code)
        if basic:
            sources.append("akshare")
    except Exception as e:
        errors.append(f"basic.akshare: {e}")
        logger.warning("basic.akshare failed for %s: %s", code, e)
    if not basic:
        try:
            basic = get_basic_from_bs(code)
            if basic:
                sources.append("baostock")
        except Exception as e:
            errors.append(f"basic.baostock: {e}")
            logger.warning("basic.baostock failed for %s: %s", code, e)
    snapshot["basic"] = basic

    # market
    market = {}
    try:
        market = get_market_from_ak(code)
        if market and "akshare" not in sources:
            sources.append("akshare")
    except Exception as e:
        errors.append(f"market.akshare: {e}")
        logger.warning("market.akshare failed for %s: %s", code, e)
    if not market:
        try:
            market = get_market_from_bs(code)
            if market and "baostock" not in sources:
                sources.append("baostock")
        except Exception as e:
            errors.append(f"market.baostock: {e}")
            logger.warning("market.baostock failed for %s: %s", code, e)
    snapshot["market"] = market

    if data_type in ("core", "all"):
        try:
            snapshot["valuation"] = get_valuation(code)
            if snapshot["valuation"] and "akshare" not in sources:
                sources.append("akshare")
        except Exception as e:
            errors.append(f"valuation.akshare: {e}")
            logger.warning("valuation.akshare failed for %s: %s", code, e)

        try:
            snapshot["valuation_stable"] = get_valuation_stable(code, basic.get("industry", ""))
            for src in snapshot.get("valuation_stable", {}).get("meta", {}).get("source_used", []):
                if src.startswith("akshare") and "akshare" not in sources:
                    sources.append("akshare")
                if src.startswith("tencent") and "tencent" not in sources:
                    sources.append("tencent")
        except Exception as e:
            errors.append(f"valuation.stable: {e}")
            logger.warning("valuation.stable failed for %s: %s", code, e)

        # 同花顺数据接入（可选增强）
        try:
            iw = get_iwencai_enrichment(
                code,
                company_name=basic.get("name", ""),
                industry_name=basic.get("industry", "")
            )
            if iw.get("_status") == "available":
                snapshot["iwencai"] = iw
                sources.append("iwencai")
        except Exception as e:
            errors.append(f"iwencai.enrichment: {e}")
            logger.info("iwencai enrichment unavailable for %s: %s", code, e)

    if data_type in ("financial", "all"):
        try:
            snapshot["financial"] = get_financial(code, years=years)
            if snapshot["financial"] and "akshare" not in sources:
                sources.append("akshare")
        except Exception as e:
            errors.append(f"financial.akshare: {e}")
            logger.warning("financial.akshare failed for %s: %s", code, e)

        try:
            bao = get_baostock_financial(code)
            if bao:
                snapshot["baostock"] = bao
                if "baostock" not in sources:
                    sources.append("baostock")
        except Exception as e:
            errors.append(f"financial.baostock: {e}")
            logger.warning("financial.baostock failed for %s: %s", code, e)

    snapshot["quality"] = {
        "completeness": completeness_score(snapshot),
        "errors": errors,
        "sources_used": sorted(list(set(sources))),
    }

    # Transform 层
    snapshot = transform_snapshot(snapshot)

    if use_cache:
        save_cache(code, data_type, snapshot)
    return snapshot
