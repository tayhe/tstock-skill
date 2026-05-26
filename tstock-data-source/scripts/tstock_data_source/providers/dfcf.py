"""东方财富数据源：PE/PB/PEG/行业估值。"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent.parent))
import config

from tstock_lib.utils import safe_float
from tstock_lib.constants import HTTP_TIMEOUT_DFCF

logger = logging.getLogger(__name__)


def _extract_first_numeric_from_table_obj(obj: Dict[str, Any]) -> Optional[float]:
    if not isinstance(obj, dict):
        return None
    for k, v in obj.items():
        if k in ("headName", "headNameSub", "headDate"):
            continue
        if isinstance(v, list) and v:
            n = safe_float(v[0])
            if n is not None:
                return n
        elif isinstance(v, (int, float, str)):
            n = safe_float(v)
            if n is not None:
                return n
    return None


def get_valuation_from_dfcf(code: str) -> Dict[str, Any]:
    """东方财富官方 Skill 适配层。"""
    out = {
        "pe_ttm": None,
        "pb": None,
        "pr": None,
        "peg": None,
        "growth_yoy_pct": None,
        "industry_avg": {"pe": None, "pb": None, "pr": None, "pe_median": None, "pb_median": None},
        "industry_name": "",
        "premium_pct": {"pe": None, "pb": None, "pr": None},
        "sample_size": 0,
        "source": "dfcf.skill"
    }

    api_key = config.EASTMONEY_APIKEY.strip()

    try:
        url = "https://mkapi2.dfcfs.com/finskillshub/api/claw/query"
        headers = {"Content-Type": "application/json", "apikey": api_key}

        market_suffix = "SH" if str(code).startswith("6") else "SZ"

        # 1. 获取个股估值指标
        queries = [
            f"{code}.{market_suffix} 市盈率(TTM) 市净率(PB) 归母净利润同比增长率",
            f"{code}.{market_suffix} 市盈率PE 市净率PB 净利润同比增速",
            f"{code}.{market_suffix} 市盈率PE(TTM)",
        ]

        for q in queries:
            try:
                r = requests.post(url, headers=headers, json={"toolQuery": q}, timeout=HTTP_TIMEOUT_DFCF)
                if r.status_code != 200:
                    continue
                obj = r.json()
                if obj and not obj.get("success") and obj.get("code"):
                    return {"error": f"dfcf.api_limit: {obj.get('message', '')}", "source": "dfcf.skill"}
                root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                dt_list = root.get("dataTableDTOList") or []
                target_code = f"{code}.{market_suffix}"

                for it in dt_list:
                    c = str(it.get("code") or "")
                    if c != target_code:
                        continue
                    table = (it.get("table") or {})
                    field = (it.get("field") or {})
                    name = str(field.get("returnName") or "")

                    v = _extract_first_numeric_from_table_obj(table)
                    if v is None:
                        continue

                    low_name = name.lower()
                    if ("市盈率" in name and "TTM" in name) or ("petrm" in low_name) or ("pe(ttm)" in low_name):
                        if out["pe_ttm"] is None:
                            out["pe_ttm"] = v
                    elif ("市净率" in name and "PB" in name) or ("pb" in low_name):
                        if out["pb"] is None:
                            out["pb"] = v
                    elif ("净利润同比" in name) or ("归母净利润同比" in name) or ("sjltz" in low_name):
                        if out["growth_yoy_pct"] is None:
                            out["growth_yoy_pct"] = v

                if out["pe_ttm"] is not None:
                    break
            except Exception:
                continue

        # 2. 获取行业分类
        industry_queries = [
            f"{code}.{market_suffix} 所属行业 申万行业 中信行业 东财行业",
        ]

        industry_code = None
        industry_names = []

        for iq in industry_queries:
            try:
                r = requests.post(url, headers=headers, json={"toolQuery": iq}, timeout=15)
                if r.status_code != 200:
                    continue
                obj = r.json()
                if obj and not obj.get("success") and obj.get("code"):
                    break
                root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                dt_list = root.get("dataTableDTOList") or []

                for it in dt_list:
                    table = it.get("table") or {}
                    for k, v in table.items():
                        if k == "headName" or k == "headNameSub" or k == "headDate":
                            continue
                        if isinstance(v, list) and v:
                            for item in v:
                                if isinstance(item, list) and item:
                                    name_str = str(item[0])
                                elif isinstance(item, str):
                                    name_str = item
                                else:
                                    continue
                                if len(name_str) > 2 and name_str not in industry_names:
                                    industry_names.append(name_str)

                    field = it.get("field") or {}
                    name = field.get("returnName", "")
                    if "申万" in str(name) or "中信" in str(name):
                        industry_code = it.get("code")

                if industry_names:
                    break
            except Exception:
                continue

        if industry_names:
            detailed = [x for x in industry_names if "-" in x or "—" in x]
            if detailed:
                industry_names = [detailed[-1]]
            else:
                industry_names = [industry_names[-1]]

        # 3. 获取行业估值
        industry_val_queries = []
        for ind_name in industry_names:
            main_ind = ind_name.split('-')[-1] if '-' in ind_name else ind_name
            industry_val_queries.append((f"{main_ind} PE", main_ind))
            industry_val_queries.append((f"{ind_name} PE", main_ind))

        for iq, ind_name in industry_val_queries:
            try:
                r = requests.post(url, headers=headers, json={"toolQuery": iq}, timeout=15)
                if r.status_code != 200:
                    continue
                obj = r.json()
                if obj and not obj.get("success") and obj.get("code"):
                    break
                root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                dt_list = root.get("dataTableDTOList") or []

                for it in dt_list:
                    table = it.get("table") or {}
                    field = it.get("field") or {}
                    name = str(field.get("returnName") or "")

                    v = _extract_first_numeric_from_table_obj(table)
                    if v is None:
                        continue

                    if "市盈率PE(TTM)" in name or "市盈率PE" in name or "整体法" in name:
                        if out["industry_avg"]["pe"] is None:
                            out["industry_avg"]["pe"] = v
                    if "市净率PB" in name:
                        if out["industry_avg"]["pb"] is None:
                            out["industry_avg"]["pb"] = v
                    if "中值" in name or "中位数" in name:
                        if "市盈率" in name:
                            if out["industry_avg"]["pe_median"] is None:
                                out["industry_avg"]["pe_median"] = v
                        if "市净率" in name:
                            if out["industry_avg"]["pb_median"] is None:
                                out["industry_avg"]["pb_median"] = v

                if out["industry_avg"]["pe"] is not None or out["industry_avg"]["pe_median"] is not None:
                    if industry_names:
                        out["industry_name"] = industry_names[0]
                    break
            except Exception:
                continue

        # 灵活匹配重试
        if not out["industry_avg"]["pe"] and not out["industry_avg"]["pe_median"]:
            for iq, ind_name in industry_val_queries:
                try:
                    r = requests.post(url, headers=headers, json={"toolQuery": iq}, timeout=15)
                    if r.status_code != 200:
                        continue
                    obj = r.json()
                    if obj and not obj.get("success") and obj.get("code"):
                        break
                    root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                    dt_list = root.get("dataTableDTOList") or []

                    for it in dt_list:
                        table = it.get("table") or {}
                        field = it.get("field") or {}
                        field_name = str(field.get("returnName", ""))

                        v = _extract_first_numeric_from_table_obj(table)
                        if v is None:
                            continue

                        field_lower = field_name.lower()
                        if "市盈率" in field_name or "pe" in field_lower:
                            if "中值" in field_name or "中位数" in field_name:
                                if out["industry_avg"]["pe_median"] is None:
                                    out["industry_avg"]["pe_median"] = v
                            elif "整体法" in field_name or "ttm" in field_lower:
                                if out["industry_avg"]["pe"] is None:
                                    out["industry_avg"]["pe"] = v
                            elif out["industry_avg"]["pe"] is None:
                                out["industry_avg"]["pe"] = v
                        elif "市净率" in field_name or "pb" in field_lower:
                            if "中值" in field_name or "中位数" in field_name:
                                if out["industry_avg"]["pb_median"] is None:
                                    out["industry_avg"]["pb_median"] = v
                            elif "整体法" in field_name:
                                if out["industry_avg"]["pb"] is None:
                                    out["industry_avg"]["pb"] = v
                            elif out["industry_avg"]["pb"] is None:
                                out["industry_avg"]["pb"] = v

                    if out["industry_avg"]["pe"] or out["industry_avg"]["pe_median"]:
                        if industry_names:
                            out["industry_name"] = industry_names[0]
                        break
                except Exception:
                    continue

        # 4. 计算PEG
        if out["pe_ttm"] is not None and out["growth_yoy_pct"] is not None:
            if out["growth_yoy_pct"] > 0:
                out["peg"] = round(out["pe_ttm"] / out["growth_yoy_pct"], 3)
            else:
                out["peg"] = None

        # 5. 计算溢价率
        pe基准 = out["industry_avg"].get("pe_median") or out["industry_avg"].get("pe")
        pb基准 = out["industry_avg"].get("pb_median") or out["industry_avg"].get("pb")

        if out["pe_ttm"] is not None and pe基准 is not None and pe基准 != 0:
            out["premium_pct"]["pe"] = round((out["pe_ttm"] - pe基准) / pe基准 * 100, 2)

        if out["pb"] is not None and pb基准 is not None and pb基准 != 0:
            out["premium_pct"]["pb"] = round((out["pb"] - pb基准) / pb基准 * 100, 2)

        return out

    except Exception as e:
        return {"error": str(e), "source": "dfcf.skill"}


def get_industry_valuation(industry_keyword: str) -> Dict[str, Any]:
    """获取行业估值数据（东方财富API）。"""
    out = {
        "industry_name": industry_keyword,
        "pe_ttm": None,
        "pb": None,
        "pe_median": None,
        "pb_median": None,
        "source": "dfcf.skill"
    }

    api_key = config.EASTMONEY_APIKEY
    if not api_key:
        return out

    try:
        url = "https://mkapi2.dfcfs.com/finskillshub/api/claw/query"
        headers = {"Content-Type": "application/json", "apikey": api_key}

        q = f"{industry_keyword} 市盈率PE(TTM) 市净率PB 市盈率中值 市净率中值"
        r = requests.post(url, headers=headers, json={"toolQuery": q}, timeout=20)

        if r.status_code != 200:
            return out

        obj = r.json()
        if obj and not obj.get("success") and obj.get("code"):
            return out
        root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
        dt_list = root.get("dataTableDTOList") or []

        for it in dt_list:
            table = it.get("table") or {}
            field = it.get("field") or {}
            name = str(field.get("returnName") or "")

            v = _extract_first_numeric_from_table_obj(table)
            if v is None:
                continue

            if "市盈率PE(TTM)" in name or "市盈率(TTM)" in name:
                if out["pe_ttm"] is None:
                    out["pe_ttm"] = v
            elif "市净率PB" in name or "市净率(MRQ)" in name:
                if out["pb"] is None:
                    out["pb"] = v
            elif "中值" in name or "中位数" in name:
                if "市盈率" in name:
                    if out["pe_median"] is None:
                        out["pe_median"] = v
                elif "市净率" in name:
                    if out["pb_median"] is None:
                        out["pb_median"] = v

        return out

    except Exception:
        return out
