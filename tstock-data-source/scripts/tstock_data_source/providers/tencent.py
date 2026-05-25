"""腾讯财经数据源：PE/PB 兜底。"""

import requests

from tstock.utils import safe_float
from tstock.constants import HTTP_TIMEOUT_TENCENT


def get_valuation_from_tencent(code: str) -> dict:
    """从腾讯行情接口获取 PE/PB 兜底数据。"""
    prefix = "sh" if str(code).startswith("6") else "sz"
    url = f"https://qt.gtimg.cn/q={prefix}{code}"
    txt = requests.get(url, timeout=HTTP_TIMEOUT_TENCENT).text
    arr = txt.split('"')[1].split('~')
    pe = safe_float(arr[39]) if len(arr) > 39 else None
    pb = safe_float(arr[46]) if len(arr) > 46 else None
    return {"pe_ttm": pe, "pb": pb, "source": "tencent.qt"}
