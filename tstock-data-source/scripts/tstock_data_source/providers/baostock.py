"""Baostock 数据源：基础信息、行情、财务数据。"""

from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd

from tstock_lib.utils import safe_float, to_bs_code

try:
    import baostock as bs
    BS_AVAILABLE = True
except Exception:
    BS_AVAILABLE = False


def get_basic_from_bs(code: str) -> Dict[str, Any]:
    if not BS_AVAILABLE:
        return {}
    lg = bs.login()
    if lg.error_code != '0':
        return {}
    try:
        rs = bs.query_stock_basic(code=to_bs_code(code))
        row = None
        while rs.next():
            row = rs.get_row_data()
            break
        if not row:
            return {}
        return {
            "code": code,
            "name": row[1],
            "industry": "",
            "market_cap": None,
            "float_cap": None,
            "total_shares": None,
            "float_shares": None,
            "pe_ttm": None,
            "pb": None,
            "listing_date": row[2],
            "source": "baostock"
        }
    finally:
        bs.logout()


def get_market_from_bs(code: str, days: int = 60) -> Dict[str, Any]:
    if not BS_AVAILABLE:
        return {}
    lg = bs.login()
    if lg.error_code != '0':
        return {}
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days + 30)).strftime('%Y-%m-%d')
        rs = bs.query_history_k_data_plus(
            to_bs_code(code),
            "date,code,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency='d'
        )
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return {}
        df = pd.DataFrame(rows, columns=["日期", "代码", "开盘", "最高", "最低", "收盘", "成交量", "成交额"])
        for c in ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        change = ((latest["收盘"] - prev["收盘"]) / prev["收盘"] * 100) if prev["收盘"] else None
        return {
            "latest_price": safe_float(latest["收盘"]),
            "latest_date": str(latest["日期"]),
            "price_change_pct": safe_float(change),
            "volume": safe_float(latest["成交量"]),
            "turnover": safe_float(latest["成交额"]),
            "high_60d": safe_float(df["最高"].max()),
            "low_60d": safe_float(df["最低"].min()),
            "avg_volume_20d": safe_float(df.tail(20)["成交量"].mean()),
            "price_data": df.tail(30).to_dict(orient="records"),
            "source": "baostock"
        }
    finally:
        bs.logout()


def get_baostock_financial(code: str) -> Dict[str, Any]:
    if not BS_AVAILABLE:
        return {}
    lg = bs.login()
    if lg.error_code != '0':
        return {"error": "baostock login failed"}
    try:
        full = to_bs_code(code)
        yq = [(2026, 1), (2025, 4), (2025, 3), (2025, 2), (2025, 1), (2024, 4)]
        picked = None
        for y, q in yq:
            rs = bs.query_profit_data(code=full, year=y, quarter=q)
            rows = []
            if rs.error_code == '0':
                while rs.next():
                    rows.append(rs.get_row_data())
            if rows:
                picked = (y, q)
                break
        if not picked:
            return {"error": "no available quarter"}

        y, q = picked
        out = {"data_year": y, "data_quarter": q}

        for key, query in [
            ("profit", bs.query_profit_data),
            ("balance", bs.query_balance_data),
            ("cash_flow", bs.query_cash_flow_data),
            ("dupont", bs.query_dupont_data),
        ]:
            rs = query(code=full, year=y, quarter=q)
            rows = []
            if rs.error_code == '0':
                while rs.next():
                    rows.append(rs.get_row_data())
            out[key] = dict(zip(rs.fields, rows[0])) if rows else None

        return out
    finally:
        bs.logout()
