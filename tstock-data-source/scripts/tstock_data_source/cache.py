"""缓存管理：日级别文件缓存。"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from tstock_lib.utils import normalize_code

logger = logging.getLogger(__name__)


def cache_dir() -> str:
    d = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".cache")
    os.makedirs(d, exist_ok=True)
    return d


def cache_key(code: str, data_type: str, days_ago: int = 0) -> str:
    today = datetime.now() - timedelta(days=days_ago)
    return os.path.join(cache_dir(), f"{normalize_code(code)}_{data_type}_{today.strftime('%Y%m%d')}.json")


def load_cache(code: str, data_type: str, cache_days: int = 3) -> Optional[Dict[str, Any]]:
    for days_ago in range(cache_days):
        p = cache_key(code, data_type, days_ago)
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                continue
    return None


def save_cache(code: str, data_type: str, data: Dict[str, Any]):
    p = cache_key(code, data_type)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    os.replace(tmp, p)
