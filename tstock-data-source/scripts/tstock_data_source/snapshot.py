"""快照编排器：从各数据源获取数据，组装完整快照。"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from tstock_lib.utils import normalize_code

from tstock_data_source.cache import load_cache, save_cache
from tstock_data_source.providers.akshare import (
    get_basic_from_ak, get_market_from_ak, get_valuation, get_financial,
)
from tstock_data_source.providers.astock import (
    fetch_realtime_quote, get_basic_from_astock, get_market_quote_from_astock,
    fetch_concept_blocks, fetch_holder_num_change, fetch_dividend_history,
    fetch_margin_trading, fetch_dragon_tiger_board, fetch_chip_distribution,
    fetch_financial_reports,
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

    # 1. basic
    basic = {}
    # 首选 a-stock-data（腾讯直连行情 + 东财概念板块，毫秒级，不封IP）
    try:
        basic = get_basic_from_astock(code)
        if basic and basic.get("name"):
            sources.append("astock")
    except Exception as e:
        errors.append(f"basic.astock: {e}")
        logger.warning("basic.astock failed for %s: %s", code, e)

    # 备选 AkShare
    if not basic:
        try:
            basic = get_basic_from_ak(code)
            if basic:
                sources.append("akshare")
        except Exception as e:
            errors.append(f"basic.akshare: {e}")
            logger.warning("basic.akshare failed for %s: %s", code, e)

    # 兜底 Baostock
    if not basic:
        try:
            basic = get_basic_from_bs(code)
            if basic:
                sources.append("baostock")
        except Exception as e:
            errors.append(f"basic.baostock: {e}")
            logger.warning("basic.baostock failed for %s: %s", code, e)
    snapshot["basic"] = basic

    # 2. market
    market = {}
    # 首选 AkShare 获取完整历史 price_data（若网络可用）
    try:
        market = get_market_from_ak(code)
        if market and "akshare" not in sources:
            sources.append("akshare")
    except Exception as e:
        errors.append(f"market.akshare: {e}")
        logger.warning("market.akshare failed for %s: %s", code, e)

    # 次选 Baostock
    if not market:
        try:
            market = get_market_from_bs(code)
            if market and "baostock" not in sources:
                sources.append("baostock")
        except Exception as e:
            errors.append(f"market.baostock: {e}")
            logger.warning("market.baostock failed for %s: %s", code, e)

    # 兜底 astock 实时行情（当 akshare 与 baostock 均超时或无历史时确保行情不为空）
    if not market:
        try:
            market = get_market_quote_from_astock(code)
            if market and "astock" not in sources:
                sources.append("astock")
        except Exception as e:
            errors.append(f"market.astock: {e}")
            logger.warning("market.astock failed for %s: %s", code, e)
    snapshot["market"] = market

    # 筹码分布计算 (若已有日K线数据)
    if market.get("price_data"):
        try:
            import pandas as pd
            raw_bars = market["price_data"]
            df_bars = pd.DataFrame(raw_bars)
            col_map = {
                "日期": "date", "最高": "high", "最低": "low", "收盘": "close", "换手率": "turn",
                "date": "date", "high": "high", "low": "low", "close": "close", "turn": "turn",
            }
            df_renamed = df_bars.rename(columns=col_map)
            if "turn" not in df_renamed.columns and "成交量" in df_bars.columns and basic.get("float_shares"):
                df_renamed["turn"] = df_bars["成交量"] / basic["float_shares"] * 100.0
            if {"date", "high", "low", "close", "turn"}.issubset(df_renamed.columns):
                chip_info = fetch_chip_distribution(df_renamed)
                if chip_info:
                    snapshot["chip_distribution"] = chip_info
                    if "astock" not in sources:
                        sources.append("astock")
        except Exception as e:
            logger.debug("chip_distribution calculation skipped for %s: %s", code, e)

    if data_type in ("core", "all"):
        # a-stock-data 实时行情与微观结构数据
        try:
            rt = fetch_realtime_quote(code)
            if rt:
                snapshot["realtime_quote"] = rt
                if "astock" not in sources:
                    sources.append("astock")
        except Exception as e:
            logger.debug("fetch_realtime_quote failed for %s: %s", code, e)

        try:
            micro = {}
            cb = fetch_concept_blocks(code)
            if cb.get("concept_tags"):
                micro["concept_tags"] = cb.get("concept_tags")
                micro["industry"] = cb.get("industry")
            holders = fetch_holder_num_change(code, page_size=5)
            if holders:
                micro["holder_num_history"] = holders
            margins = fetch_margin_trading(code, page_size=5)
            if margins:
                micro["margin_trading"] = margins
            divs = fetch_dividend_history(code, page_size=5)
            if divs:
                micro["dividend_history"] = divs

            if micro:
                snapshot["microstructure"] = micro
                if "astock" not in sources:
                    sources.append("astock")
        except Exception as e:
            errors.append(f"microstructure.astock: {e}")
            logger.info("astock microstructure unavailable for %s: %s", code, e)
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
        financial = {}
        # 首选 a-stock-data 新浪财报（单次请求，毫秒级响应，避免 AkShare 历史全量翻页）
        try:
            financial = fetch_financial_reports(code, years=years)
            if financial and (financial.get("income_statement") or financial.get("balance_sheet")):
                if "astock" not in sources:
                    sources.append("astock")
            else:
                financial = {}
        except Exception as e:
            errors.append(f"financial.astock: {e}")
            logger.warning("financial.astock failed for %s: %s", code, e)

        # 备选 AkShare
        if not financial:
            try:
                financial = get_financial(code, years=years)
                if financial and "akshare" not in sources:
                    sources.append("akshare")
            except Exception as e:
                errors.append(f"financial.akshare: {e}")
                logger.warning("financial.akshare failed for %s: %s", code, e)

        snapshot["financial"] = financial

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
