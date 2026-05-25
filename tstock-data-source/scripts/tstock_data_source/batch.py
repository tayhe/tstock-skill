"""批量获取与指数成分股查询。"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List

import akshare as ak

from tstock.utils import normalize_code
from tstock.constants import BATCH_SLEEP

from tstock_data_source.snapshot import SCHEMA_VERSION, fetch_stock_snapshot

logger = logging.getLogger(__name__)


def get_scope_codes(scope: str) -> List[str]:
    scope = scope.lower()
    if scope == "all":
        try:
            df = ak.stock_zh_a_spot_em()
            return df["代码"].astype(str).tolist() if df is not None and not df.empty else []
        except Exception:
            return []

    mp = {
        "hs300": "000300",
        "zz500": "000905",
        "zz1000": "000852",
        "cyb": "399006",
        "kcb": "000688",
    }
    if scope not in mp:
        return []
    try:
        df = ak.index_stock_cons(symbol=mp[scope])
        if df is None or df.empty:
            return []
        return df["品种代码"].astype(str).tolist()
    except Exception:
        return []


def fetch_batch(codes: List[str], data_type: str, years: int, use_cache: bool) -> Dict[str, Any]:
    out = {
        "schema_version": SCHEMA_VERSION,
        "batch_as_of": datetime.now().isoformat(),
        "count": len(codes),
        "success": 0,
        "failed": 0,
        "items": []
    }
    for i, c in enumerate(codes, 1):
        c = normalize_code(c)
        try:
            snap = fetch_stock_snapshot(c, data_type=data_type, years=years, use_cache=use_cache)
            out["items"].append(snap)
            out["success"] += 1
        except Exception as e:
            out["failed"] += 1
            out["items"].append({"code": c, "error": str(e)})
        if i < len(codes):
            time.sleep(BATCH_SLEEP)
    return out
