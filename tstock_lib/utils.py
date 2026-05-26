"""通用工具函数。"""

from typing import Optional, Union


def safe_float(v: object, default: Optional[float] = None) -> Optional[float]:
    """将任意值安全转换为 float，无法转换时返回 default。

    处理：None、空字符串、"--"、"nan"、"None"、含 "%" / "," / "倍" 的字符串。
    """
    if v is None:
        return default
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").replace("倍", "").strip()
            if v in ("", "--", "nan", "None"):
                return default
        return float(v)
    except (ValueError, TypeError):
        return default


def normalize_code(code: str) -> str:
    """去除交易所前缀，返回 6 位纯数字代码。"""
    code = code.strip().upper()
    if code.startswith(("SH", "SZ")):
        code = code[2:]
    return code


def with_exchange_prefix(code: str) -> str:
    """返回带交易所前缀的代码，如 'SH600000' / 'SZ300308'。"""
    code = normalize_code(code)
    return ("SH" + code) if code.startswith("6") else ("SZ" + code)


def to_bs_code(code: str) -> str:
    """返回 baostock 格式的代码，如 'sh.600000' / 'sz.300308'。"""
    code = normalize_code(code)
    return ("sh." + code) if code.startswith("6") else ("sz." + code)
