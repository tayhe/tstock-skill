"""AkShare 数据源：基础信息、行情、估值、财务报表。"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict

import akshare as ak
import pandas as pd

from tstock_lib.utils import safe_float, with_exchange_prefix
from tstock_lib.constants import RETRY_BACKOFF

logger = logging.getLogger(__name__)


def get_basic_from_ak(code: str) -> Dict[str, Any]:
    df = ak.stock_individual_info_em(symbol=code)
    if df is None or df.empty:
        return {}
    m = {row["item"]: row["value"] for _, row in df.iterrows()}
    return {
        "code": code,
        "name": m.get("股票简称", ""),
        "industry": m.get("行业", ""),
        "market_cap": safe_float(m.get("总市值")),
        "float_cap": safe_float(m.get("流通市值")),
        "total_shares": safe_float(m.get("总股本")),
        "float_shares": safe_float(m.get("流通股")),
        "pe_ttm": safe_float(m.get("市盈率(动态)")),
        "pb": safe_float(m.get("市净率")),
        "listing_date": m.get("上市时间", ""),
        "source": "akshare"
    }


def get_market_from_ak(code: str, days: int = 150) -> Dict[str, Any]:
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    if df is None or df.empty:
        return {}
    latest = df.iloc[-1]
    return {
        "latest_price": safe_float(latest.get("收盘")),
        "latest_date": str(latest.get("日期")),
        "price_change_pct": safe_float(latest.get("涨跌幅")),
        "volume": safe_float(latest.get("成交量")),
        "turnover": safe_float(latest.get("成交额")),
        "high_60d": safe_float(df["最高"].max()),
        "low_60d": safe_float(df["最低"].min()),
        "avg_volume_20d": safe_float(df.tail(20)["成交量"].mean()),
        "price_data": df.tail(120).to_dict(orient="records"),
        "source": "akshare"
    }


def get_valuation(code: str) -> Dict[str, Any]:
    out = {}
    try:
        df = ak.stock_a_ttm_lyr()
        if df is None or df.empty:
            return out
        target = df[df["code"].astype(str).str.endswith(code)]
        if target.empty:
            return out
        latest = target.iloc[-1].to_dict()
        out["latest"] = latest
        for col in ["pe_ttm", "pb"]:
            v = latest.get(col)
            if v is not None:
                s = target[col].dropna()
                if len(s) > 0:
                    out[f"{col}_percentile"] = float((s < v).mean() * 100)
        out["source"] = "akshare"
    except Exception as e:
        out["error"] = str(e)
    return out


def get_financial(code: str, years: int = 3) -> Dict[str, Any]:
    max_records = min(years * 4, 12)
    symbol = with_exchange_prefix(code)
    result = {
        "balance_sheet": [],
        "income_statement": [],
        "cash_flow": [],
        "financial_indicators": []
    }

    fetchers = [
        ("balance_sheet", ak.stock_balance_sheet_by_report_em),
        ("income_statement", ak.stock_profit_sheet_by_report_em),
        ("cash_flow", ak.stock_cash_flow_sheet_by_report_em),
    ]
    for k, fn in fetchers:
        try:
            df = fn(symbol=symbol)
            if df is not None and not df.empty:
                result[k] = df.head(max_records).to_dict(orient="records")
        except Exception as e:
            result[f"{k}_error"] = str(e)

    try:
        dfi = ak.stock_financial_abstract(symbol=code)
        if dfi is not None and not dfi.empty:
            result["financial_indicators"] = dfi.head(20).to_dict(orient="records")
    except Exception as e:
        result["financial_indicators_error"] = str(e)

    return result


def fetch_spot_with_retry(retries: int = 2):
    last = None
    for i in range(retries + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            last = e
            logger.warning("AkShare spot retry %d/%d: %s", i + 1, retries, e)
            time.sleep(RETRY_BACKOFF)
    return None


def winsorized_median(series, low_q=0.05, high_q=0.95):
    s = pd.to_numeric(series, errors='coerce').dropna()
    if s.empty:
        return None
    ql = s.quantile(low_q)
    qh = s.quantile(high_q)
    s = s[(s >= ql) & (s <= qh)]
    if s.empty:
        return None
    return float(s.median())
