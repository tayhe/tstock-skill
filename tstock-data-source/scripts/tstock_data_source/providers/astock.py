"""
a-stock-data 业务适配器模块
==========================
封装上游 a-stock-data 的高质量实现，作为 tstock-data-source 的首选数据提供者之一。
包含：
1. 极速实时行情（tencent_quote，不封IP，GBK，毫秒级响应）
2. 筹码分布估算（chip_distribution，时序三角分布衰减）
3. 股东户数变动（holder_num_change）
4. 分红派息历史（dividend_history）
5. 融资融券明细（margin_trading）
6. 龙虎榜明细（dragon_tiger_board）
7. 概念板块标签（eastmoney_concept_blocks）
8. K线行情获取与降级保护（mootdx）
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from tstock_data_source.providers.astock_adapter import call_astock, get_astock_function
from tstock_lib.utils import normalize_code, safe_float

logger = logging.getLogger(__name__)


def fetch_realtime_quote(
    code_or_codes: Union[str, List[str]]
) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """批量或单只获取腾讯财经实时行情。

    Args:
        code_or_codes: 6位代码、带前缀代码，或代码列表。如 "600519", "sh600519", ["600519", "000001"]

    Returns:
        若输入为单只代码，返回该代码的行情字典；
        若输入为列表，返回以代码为键的行情字典映射。
    """
    is_single = isinstance(code_or_codes, str)
    raw_list = [code_or_codes] if is_single else list(code_or_codes)

    clean_codes = [normalize_code(c) for c in raw_list]
    quotes_raw = call_astock("tencent_quote", clean_codes)

    result: Dict[str, Dict[str, Any]] = {}
    for code, q in quotes_raw.items():
        std_code = normalize_code(code)
        result[std_code] = {
            "code": std_code,
            "name": q.get("name", ""),
            "price": safe_float(q.get("price")),
            "last_close": safe_float(q.get("last_close")),
            "open": safe_float(q.get("open")),
            "high": safe_float(q.get("high")),
            "low": safe_float(q.get("low")),
            "change_amt": safe_float(q.get("change_amt")),
            "change_pct": safe_float(q.get("change_pct")),
            "amount_wan": safe_float(q.get("amount_wan")),
            "turnover_pct": safe_float(q.get("turnover_pct")),
            "pe_ttm": safe_float(q.get("pe_ttm")),
            "pb": safe_float(q.get("pb")),
            "pe_static": safe_float(q.get("pe_static")),
            "mcap_yi": safe_float(q.get("mcap_yi")),
            "float_mcap_yi": safe_float(q.get("float_mcap_yi")),
            "amplitude_pct": safe_float(q.get("amplitude_pct")),
            "limit_up": safe_float(q.get("limit_up")),
            "limit_down": safe_float(q.get("limit_down")),
            "vol_ratio": safe_float(q.get("vol_ratio")),
            "is_stale": bool(q.get("is_stale", False)),
            "stale_reason": q.get("stale_reason", ""),
            "source": "astock.tencent",
        }

    if is_single:
        target = normalize_code(raw_list[0])
        return result.get(target, {})
    return result


def get_basic_from_astock(code: str) -> Dict[str, Any]:
    """从 a-stock-data 获取股票基础信息与估值。兼容 get_basic_from_ak 输出结构。"""
    code = normalize_code(code)
    quote = fetch_realtime_quote(code)
    if not quote:
        return {}

    # 尝试获取所属概念与行业
    industry = ""
    concepts = []
    try:
        cb = fetch_concept_blocks(code)
        industry = cb.get("industry", "")
        concepts = cb.get("concept_tags", [])
    except Exception as e:
        logger.debug("Failed to fetch concept blocks for %s: %s", code, e)

    mcap_raw = quote.get("mcap_yi")
    float_mcap_raw = quote.get("float_mcap_yi")
    price = quote.get("price")
    market_cap = (mcap_raw * 1e8) if mcap_raw is not None else None
    float_cap = (float_mcap_raw * 1e8) if float_mcap_raw is not None else None
    total_shares = (market_cap / price) if (market_cap and price and price > 0) else None
    float_shares = (float_cap / price) if (float_cap and price and price > 0) else None

    return {
        "code": code,
        "name": quote.get("name", ""),
        "industry": industry,
        "concepts": concepts,
        "market_cap": market_cap,
        "float_cap": float_cap,
        "total_shares": total_shares,
        "float_shares": float_shares,
        "pe_ttm": quote.get("pe_ttm"),
        "pb": quote.get("pb"),
        "listing_date": "",
        "source": "astock",
    }


def get_market_quote_from_astock(code: str) -> Dict[str, Any]:
    """从 a-stock-data 获取当前实时盘口快照。"""
    code = normalize_code(code)
    quote = fetch_realtime_quote(code)
    if not quote:
        return {}

    price = quote.get("price")
    amt_wan = quote.get("amount_wan") or 0.0
    turnover_yuan = amt_wan * 10000.0

    return {
        "latest_price": price,
        "latest_date": datetime.now().strftime("%Y-%m-%d"),
        "price_change_pct": quote.get("change_pct"),
        "volume": (turnover_yuan / price) if (price and price > 0) else None,
        "turnover": turnover_yuan,
        "turnover_rate": quote.get("turnover_pct"),
        "open": quote.get("open"),
        "high": quote.get("high"),
        "low": quote.get("low"),
        "last_close": quote.get("last_close"),
        "limit_up": quote.get("limit_up"),
        "limit_down": quote.get("limit_down"),
        "is_stale": quote.get("is_stale", False),
        "source": "astock.tencent",
    }


def fetch_concept_blocks(code: str) -> Dict[str, Any]:
    """获取股票所属概念标签与细分板块。"""
    code = normalize_code(code)
    res = call_astock("eastmoney_concept_blocks", code)
    if not isinstance(res, dict):
        return {}
    # 提取主要行业
    industry = ""
    tags = res.get("concept_tags", [])
    if tags:
        industry = tags[0]
    return {
        "code": code,
        "industry": industry,
        "concept_tags": tags,
        "concepts": res.get("concepts", []),
        "source": "astock.eastmoney",
    }


def fetch_holder_num_change(code: str, page_size: int = 10) -> List[Dict[str, Any]]:
    """获取股东户数变动历史（筹码集中度关键指标）。"""
    code = normalize_code(code)
    try:
        return call_astock("holder_num_change", code, page_size=page_size)
    except Exception as e:
        logger.warning("fetch_holder_num_change failed for %s: %s", code, e)
        return []


def fetch_dividend_history(code: str, page_size: int = 20) -> List[Dict[str, Any]]:
    """获取分红送转历史。"""
    code = normalize_code(code)
    try:
        return call_astock("dividend_history", code, page_size=page_size)
    except Exception as e:
        logger.warning("fetch_dividend_history failed for %s: %s", code, e)
        return []


def fetch_margin_trading(code: str, page_size: int = 30) -> List[Dict[str, Any]]:
    """获取融资融券历史交易明细。"""
    code = normalize_code(code)
    try:
        return call_astock("margin_trading", code, page_size=page_size)
    except Exception as e:
        logger.warning("fetch_margin_trading failed for %s: %s", code, e)
        return []


def fetch_dragon_tiger_board(
    code: str, trade_date: Optional[str] = None, look_back: int = 30
) -> Dict[str, Any]:
    """获取股票龙虎榜上榜明细。"""
    code = normalize_code(code)
    date_str = trade_date or datetime.now().strftime("%Y-%m-%d")
    try:
        return call_astock("dragon_tiger_board", code, date_str, look_back=look_back)
    except Exception as e:
        logger.warning("fetch_dragon_tiger_board failed for %s: %s", code, e)
        return {"records": []}


def fetch_chip_distribution(
    df: pd.DataFrame, grid_size: int = 300, decay: float = 1.0
) -> Dict[str, Any]:
    """计算筹码分布指标。

    df 需包含：date, high, low, close, turn（turn 为换手率百分数，如 1.5 表示 1.5%）。
    """
    chip_fn = get_astock_function("chip_distribution")
    if chip_fn is None:
        raise RuntimeError("chip_distribution function not found in a-stock-data")

    res = chip_fn(df, grid_size=grid_size, decay=decay)

    # 提取标量结果与分布点
    dist_points = res.get("distribution", [])
    # 限制分布采样点为 50 个点以避免 JSON 过大
    if len(dist_points) > 50:
        step = len(dist_points) // 50
        dist_sample = dist_points[::step]
    else:
        dist_sample = dist_points

    return {
        "price": res.get("price"),
        "profit_ratio": res.get("profit_ratio"),
        "avg_cost": res.get("avg_cost"),
        "cost_90": res.get("cost_90"),
        "cost_70": res.get("cost_70"),
        "concentration_90": res.get("concentration_90"),
        "concentration_70": res.get("concentration_70"),
        "peak_price": res.get("peak_price"),
        "distribution_sample": dist_sample,
        "source": "astock.chip_distribution",
    }


def fetch_kline(
    code: str, count: int = 240, period: str = "day"
) -> Optional[pd.DataFrame]:
    """从 mootdx 获取 K 线，带网络超时与降级保护。

    若通达信服务器连接失败或不可达，安全返回 None，不阻塞上层流程。
    """
    code = normalize_code(code)
    tdx_client_fn = get_astock_function("tdx_client")
    if tdx_client_fn is None:
        return None

    try:
        # frequency: 9=日K, 0=5分, 8=1分
        freq = 9 if period == "day" else (0 if period == "5m" else 9)
        client = tdx_client_fn(market="std")
        df = client.bars(symbol=code, frequency=freq, offset=count)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning("fetch_kline via mootdx failed for %s: %s", code, e)

    return None


def fetch_financial_reports(code: str, years: int = 3) -> Dict[str, Any]:
    """从 a-stock-data 新浪财报接口快速抓取三表（单次请求，毫秒级响应）。"""
    code = normalize_code(code)
    num = min(years * 4, 12)
    reports = {
        "balance_sheet": [],
        "income_statement": [],
        "cash_flow": [],
        "source": "astock.sina",
    }
    type_map = {
        "balance_sheet": "fzb",
        "income_statement": "lrb",
        "cash_flow": "llb",
    }
    for field, rtype in type_map.items():
        try:
            data = call_astock("sina_financial_report", code, rtype, num)
            if isinstance(data, list):
                reports[field] = data
        except Exception as e:
            logger.warning("fetch_financial_reports (%s) failed for %s: %s", rtype, code, e)

    return reports
