"""统一稳定估值口径：双主源 + 权威校验。"""

import logging
from datetime import datetime
from typing import Any, Dict

import akshare as ak

from tstock_lib.utils import safe_float
from tstock_lib.constants import PREMIUM_HIGH_PCT, PREMIUM_LOW_PCT, PEG_HIGH, PEG_LOW, INDUSTRY_MIN_SAMPLE

from tstock_data_source.providers.akshare import fetch_spot_with_retry, winsorized_median
from tstock_data_source.providers.dfcf import get_valuation_from_dfcf
from tstock_data_source.providers.tencent import get_valuation_from_tencent

logger = logging.getLogger(__name__)


def _supplement_industry_from_spot(out: Dict, code: str, industry_name: str):
    """当主数据源未返回行业均值时，从 AkShare 实时行情补充。"""
    try:
        spot = fetch_spot_with_retry()
        if spot is None or spot.empty:
            return
        row = spot[spot["代码"].astype(str) == str(code)]
        if row.empty:
            return
        r = row.iloc[0]
        if not industry_name:
            industry_name = str(r.get("行业", "") or "")
        if industry_name and "行业" in spot.columns:
            g = spot[spot["行业"].astype(str) == industry_name].copy()
            out["sample_size"] = int(len(g))
            if len(g) >= INDUSTRY_MIN_SAMPLE:
                if out["industry_avg"].get("pe") is None:
                    out["industry_avg"]["pe"] = winsorized_median(g.get("市盈率-动态"))
                if out["industry_avg"].get("pb") is None:
                    out["industry_avg"]["pb"] = winsorized_median(g.get("市净率"))
                if out["industry_avg"].get("pr") is None:
                    out["industry_avg"]["pr"] = winsorized_median(g.get("市销率"))
            if industry_name and not out.get("industry_name"):
                out["industry_name"] = industry_name
            out["meta"]["source_used"].append("akshare.spot")
    except Exception as e:
        logger.debug("AkShare spot supplement failed for %s: %s", code, e)


def get_valuation_stable(code: str, industry_name: str = "") -> Dict[str, Any]:
    """双主源 + 权威校验：稳定估值口径（由数据源独家输出）。"""
    out = {
        "pe_ttm": None,
        "pb": None,
        "pr": None,
        "peg": None,
        "growth_yoy_pct": None,
        "industry_avg": {"pe": None, "pb": None, "pr": None, "pe_median": None, "pb_median": None},
        "industry_name": "",
        "premium_pct": {"pe": None, "pb": None, "pr": None},
        "assessment": {},
        "sample_size": 0,
        "meta": {
            "valuation_basis": "PE(TTM)/PB(当前)/PR(TTM)/PEG=PE÷净利润同比增速%",
            "as_of": datetime.now().strftime("%Y-%m-%d"),
            "source_used": []
        }
    }

    # 优先使用东方财富数据
    dfcf = get_valuation_from_dfcf(code)
    dfcf_has_data = (dfcf and not dfcf.get("error")
                     and (dfcf.get("pe_ttm") is not None or dfcf.get("pb") is not None))
    if dfcf_has_data:
        out["pe_ttm"] = dfcf.get("pe_ttm")
        out["pb"] = dfcf.get("pb")
        out["pr"] = dfcf.get("pr")
        out["peg"] = dfcf.get("peg")
        out["growth_yoy_pct"] = dfcf.get("growth_yoy_pct")

        if dfcf.get("industry_avg"):
            out["industry_avg"] = dfcf["industry_avg"]
        if dfcf.get("industry_name"):
            out["industry_name"] = dfcf["industry_name"]

        out["premium_pct"] = dfcf.get("premium_pct", {"pe": None, "pb": None, "pr": None})
        out["meta"]["source_used"].append("dfcf.skill")

        if out["industry_avg"].get("pe") is None and out["industry_avg"].get("pe_median") is None:
            _supplement_industry_from_spot(out, code, industry_name)
    else:
        # 备源1：Akshare 实时行情
        spot = fetch_spot_with_retry()
        if spot is not None and not spot.empty:
            try:
                row = spot[spot["代码"].astype(str) == str(code)]
                if not row.empty:
                    r = row.iloc[0]
                    out["pe_ttm"] = out["pe_ttm"] if out["pe_ttm"] is not None else safe_float(r.get("市盈率-动态"))
                    out["pb"] = out["pb"] if out["pb"] is not None else safe_float(r.get("市净率"))
                    out["pr"] = out["pr"] if out["pr"] is not None else safe_float(r.get("市销率"))
                    if not industry_name:
                        industry_name = str(r.get("行业", "") or "")

                if industry_name and "行业" in spot.columns:
                    g = spot[spot["行业"].astype(str) == industry_name].copy()
                    out["sample_size"] = int(len(g))
                    if len(g) >= INDUSTRY_MIN_SAMPLE:
                        out["industry_avg"] = {
                            "pe": out["industry_avg"].get("pe") or winsorized_median(g.get("市盈率-动态")),
                            "pb": out["industry_avg"].get("pb") or winsorized_median(g.get("市净率")),
                            "pr": out["industry_avg"].get("pr") or winsorized_median(g.get("市销率")),
                        }
                out["meta"]["source_used"].append("akshare.spot")
            except Exception as e:
                logger.debug("AkShare spot fallback failed for %s: %s", code, e)

        # 备源2：腾讯补 PE/PB
        if out["pe_ttm"] is None or out["pb"] is None:
            try:
                tencent = get_valuation_from_tencent(code)
                if out["pe_ttm"] is None:
                    out["pe_ttm"] = tencent.get("pe_ttm")
                if out["pb"] is None:
                    out["pb"] = tencent.get("pb")
                out["meta"]["source_used"].append("tencent.qt")
            except Exception as e:
                logger.debug("Tencent fallback failed for %s: %s", code, e)

        # 权威校验：从财务摘要提净利润同比，计算 PEG
        if out["growth_yoy_pct"] is None:
            try:
                fa = ak.stock_financial_abstract(symbol=str(code))
                if fa is not None and not fa.empty:
                    yoy_row = fa[fa["指标"] == "归母净利润同比增长率"]
                    if not yoy_row.empty:
                        cols = [c for c in yoy_row.columns if str(c).isdigit()]
                        if cols:
                            growth = safe_float(yoy_row.iloc[0][cols[-1]])
                            out["growth_yoy_pct"] = growth
                            out["meta"]["source_used"].append("akshare.financial_abstract")
            except Exception as e:
                logger.debug("Financial abstract growth lookup failed for %s: %s", code, e)

    # 计算PEG（若东方财富未提供）
    if out["peg"] is None:
        gy = out.get("growth_yoy_pct")
        if out.get("pe_ttm") is not None and gy is not None and gy > 0:
            out["peg"] = round(out["pe_ttm"] / gy, 3)
        elif gy is not None and gy <= 0:
            out["assessment"]["peg"] = "增速<=0，PEG不适用"

    # 计算溢价率与判断（若东方财富未提供）
    if out["premium_pct"].get("pe") is None:
        pe基准 = out["industry_avg"].get("pe_median") or out["industry_avg"].get("pe")
        if out["pe_ttm"] is not None and pe基准 is not None and pe基准 != 0:
            prem = (out["pe_ttm"] - pe基准) / pe基准 * 100
            out["premium_pct"]["pe"] = round(prem, 2)

    if out["premium_pct"].get("pb") is None:
        pb基准 = out["industry_avg"].get("pb_median") or out["industry_avg"].get("pb")
        if out["pb"] is not None and pb基准 is not None and pb基准 != 0:
            prem = (out["pb"] - pb基准) / pb基准 * 100
            out["premium_pct"]["pb"] = round(prem, 2)

    # 估值判断
    for k, kk in [("pe_ttm", "pe"), ("pb", "pb")]:
        v = out.get(k)
        ind = out["industry_avg"].get(kk) or out["industry_avg"].get(f"{kk}_median")
        prem = out["premium_pct"].get(kk)

        if prem is not None:
            if prem > PREMIUM_HIGH_PCT:
                out["assessment"][kk] = "偏高估"
            elif prem < PREMIUM_LOW_PCT:
                out["assessment"][kk] = "偏低估"
            else:
                out["assessment"][kk] = "估值合理"
        elif v is not None and ind is not None and ind != 0:
            prem_calc = (v - ind) / ind * 100
            if prem_calc > PREMIUM_HIGH_PCT:
                out["assessment"][kk] = "偏高估"
            elif prem_calc < PREMIUM_LOW_PCT:
                out["assessment"][kk] = "偏低估"
            else:
                out["assessment"][kk] = "估值合理"

    if out["peg"] is not None:
        if out["peg"] > PEG_HIGH:
            out["assessment"]["peg"] = "成长定价偏贵"
        elif out["peg"] < PEG_LOW:
            out["assessment"]["peg"] = "成长定价偏低"
        else:
            out["assessment"]["peg"] = "成长定价合理"

    out["meta"]["source_used"] = sorted(set(out["meta"]["source_used"]))
    return out
