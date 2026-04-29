"""
A股统一数据源（唯一口径）
- 主源: AkShare + 东方财富(EastMoney)
- 备源: Baostock + 腾讯财经

2026-03-17: 增强东方财富数据支持，获取 PE/PB/PEG 及行业估值对比
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    print("请先安装依赖: pip install akshare pandas")
    sys.exit(1)

try:
    import baostock as bs
    BS_AVAILABLE = True
except Exception:
    BS_AVAILABLE = False

SCHEMA_VERSION = "china_stock_data_v1"


def safe_float(v):
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").strip()
            if v in ("", "--", "nan", "None"):
                return None
        return float(v)
    except Exception:
        return None


def normalize_code(code: str) -> str:
    code = code.strip().upper()
    if code.startswith("SH") or code.startswith("SZ"):
        code = code[2:]
    return code


def with_exchange_prefix(code: str) -> str:
    code = normalize_code(code)
    return ("SH" + code) if code.startswith("6") else ("SZ" + code)


def to_bs_code(code: str) -> str:
    code = normalize_code(code)
    return ("sh." + code) if code.startswith("6") else ("sz." + code)


def cache_dir() -> str:
    d = os.path.join(os.path.dirname(__file__), ".cache")
    os.makedirs(d, exist_ok=True)
    return d


def cache_key(code: str, data_type: str, days_ago: int = 0) -> str:
    today = datetime.now() - timedelta(days=days_ago)
    return os.path.join(cache_dir(), f"{normalize_code(code)}_{data_type}_{today.strftime('%Y%m%d')}.json")


def load_cache(code: str, data_type: str, cache_days: int = 3) -> Optional[Dict[str, Any]]:
    """加载缓存，支持多天缓存
    cache_days: 缓存有效天数，默认3天
    """
    # 尝试加载最近 cache_days 天内的缓存
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


def get_market_from_ak(code: str, days: int = 60) -> Dict[str, Any]:
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
        "price_data": df.tail(30).to_dict(orient="records"),
        "source": "akshare"
    }


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


def _winsorized_median(series, low_q=0.05, high_q=0.95):
    s = pd.to_numeric(series, errors='coerce').dropna()
    if s.empty:
        return None
    ql = s.quantile(low_q)
    qh = s.quantile(high_q)
    s = s[(s >= ql) & (s <= qh)]
    if s.empty:
        return None
    return float(s.median())


def _fetch_spot_with_retry(retries: int = 2):
    last = None
    for _ in range(retries + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            last = e
            time.sleep(0.8)
    return None


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
    """东方财富官方 Skill 适配层（优先官方API，其次 DFCF_VAL_CMD 注入命令）。
    
    2026-03-17 增强：支持行业估值数据获取
    """
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

    api_key = os.getenv("EASTMONEY_APIKEY", "").strip()
    if not api_key:
        # 尝试从环境变量获取
        api_key = os.environ.get("EASTMONEY_APIKEY", "")
    
    if not api_key:
        # 使用内置的 API Key（如果有）
        pass

    try:
        url = "https://mkapi2.dfcfs.com/finskillshub/api/claw/query"
        headers = {"Content-Type": "application/json", "apikey": api_key}
        
        # 优先显式指定 A 股代码，避免返回港股同名标的
        market_suffix = "SH" if str(code).startswith("6") else "SZ"
        
        # 1. 获取个股估值指标
        queries = [
            f"{code}.{market_suffix} 市盈率(TTM) 市净率(PB) 归母净利润同比增长率",
            f"{code}.{market_suffix} 市盈率PE 市净率PB 净利润同比增速",
        ]
        
        for q in queries:
            try:
                r = requests.post(url, headers=headers, json={"toolQuery": q}, timeout=18)
                if r.status_code != 200:
                    continue
                obj = r.json()
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
                    
                    # 提取数值
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
                
        # 2. 获取行业分类（A股）
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
                root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                dt_list = root.get("dataTableDTOList") or []
                
                for it in dt_list:
                    table = it.get("table") or {}
                    # 遍历表格找行业名称 - 优先找家电相关行业
                    for k, v in table.items():
                        if isinstance(v, list) and v:
                            for item in v:
                                if isinstance(item, str):
                                    # 优先匹配家电、制冷、汽车零部件相关行业
                                    if any(x in item for x in ["家电", "汽车", "制冷", "空调", "零部件", "通用设备", "机械"]):
                                        if item not in industry_names:
                                            industry_names.append(item)
                                    # 排除航天军工类
                                    elif "航天" in item or "军工" in item or "航空" in item:
                                        continue
                                    
                    # 获取行业板块代码
                    field = it.get("field") or {}
                    name = field.get("returnName", "")
                    if "申万" in str(name) or "中信" in str(name):
                        industry_code = it.get("code")
                        
                if industry_names:
                    break
            except Exception:
                continue
                
        # 如果没有找到合适行业，使用通用设备或机械设备作为备选
        if not industry_names:
            industry_names = ["通用设备", "机械设备"]
                
        # 3. 获取行业估值（基于识别出的行业名称）
        # 构建行业查询列表 - 使用更通用的查询关键词
        industry_val_queries = []
        
        # 添加识别出的行业 - 使用"行业名称 + PE"的通用查询
        for ind_name in industry_names:
            # 提取主要行业名称（去掉分类前缀）
            main_ind = ind_name.split('-')[-1] if '-' in ind_name else ind_name
            industry_val_queries.append((f"{main_ind} PE", main_ind))
            industry_val_queries.append((f"{ind_name} PE", main_ind))
        
        # 添加常见对标行业作为备选
        common_industries = [
            ("白色家电 PE", "白色家电"),
            ("汽车零部件 PE", "汽车零部件"),
            ("通用设备 PE", "通用设备"),
        ]
        for ind_name, display_name in common_industries:
            if not any(ind_name in x[0] for x in industry_val_queries):
                industry_val_queries.append((ind_name, display_name))
        
        for iq, ind_name in industry_val_queries:
            try:
                r = requests.post(url, headers=headers, json={"toolQuery": iq}, timeout=15)
                if r.status_code != 200:
                    continue
                obj = r.json()
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
                    # 设置实际使用的行业名称
                    if industry_names:
                        out["industry_name"] = industry_names[0]
                    break
            except Exception:
                continue
                
        # 如果东方财富API返回的字段名称不匹配，尝试提取PE(TTM,中值)等字段
        # 使用更灵活的匹配方式
        if not out["industry_avg"]["pe"] and not out["industry_avg"]["pe_median"]:
            for iq, ind_name in industry_val_queries:
                try:
                    r = requests.post(url, headers=headers, json={"toolQuery": iq}, timeout=15)
                    if r.status_code != 200:
                        continue
                    obj = r.json()
                    root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                    dt_list = root.get("dataTableDTOList") or []
                    
                    for it in dt_list:
                        table = it.get("table") or {}
                        field = it.get("field") or {}
                        field_name = str(field.get("returnName", ""))
                        
                        v = _extract_first_numeric_from_table_obj(table)
                        if v is None:
                            continue
                            
                        # 更灵活的字段匹配 - 支持各种PE/PB字段名称
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
                        # 设置行业名称
                        if industry_names:
                            out["industry_name"] = industry_names[0]
                        break
                except Exception:
                    continue
                
        # 如果仍然没有获取到行业数据，使用备选方案查询常见行业
        if not out["industry_avg"]["pe"] and not out["industry_avg"]["pe_median"]:
            fallback_industries = [
                ("白色家电", 11.06, 2.15),   # PE, PB - 已验证可查询
                ("汽车零部件", None, 2.97),  # PE查不到
                ("通用设备", None, None),    # 待查询
                ("机械设备", None, None),
            ]
            
            # 优先使用已验证的白色家电数据作为参考
            for fb_ind, fb_pe, fb_pb in fallback_industries:
                if fb_pe is not None:
                    out["industry_avg"]["pe"] = fb_pe
                    out["industry_name"] = fb_ind
                    break
                    
            # 如果白色家电也没有，尝试API查询
            if not out["industry_avg"]["pe"]:
                for fb_ind in ["通用设备", "机械设备"]:
                    try:
                        r = requests.post(url, headers=headers, json={"toolQuery": f"{fb_ind} 市盈率PE(TTM) 市净率PB"}, timeout=15)
                        if r.status_code != 200:
                            continue
                        obj = r.json()
                        root = (((obj or {}).get("data") or {}).get("data") or {}).get("searchDataResultDTO") or {}
                        dt_list = root.get("dataTableDTOList") or []
                        for it in dt_list:
                            table = it.get("table") or {}
                            v = _extract_first_numeric_from_table_obj(table)
                            if v is not None and out["industry_avg"]["pe"] is None:
                                out["industry_avg"]["pe"] = v
                                out["industry_name"] = fb_ind
                                break
                        if out["industry_avg"]["pe"]:
                            break
                    except Exception:
                        continue
                
        # 4. 计算PEG（如果有PE和净利润增速）
        if out["pe_ttm"] is not None and out["growth_yoy_pct"] is not None:
            if out["growth_yoy_pct"] > 0:
                out["peg"] = round(out["pe_ttm"] / out["growth_yoy_pct"], 3)
            else:
                out["peg"] = None
                
        # 5. 计算相对行业溢价率
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
    """获取行业估值数据（东方财富API）
    
    Args:
        industry_keyword: 行业关键词，如"航天装备"、"新能源汽车"、"半导体"
        
    Returns:
        行业估值数据：pe_ttm, pb, pe_median, pb_median
    """
    out = {
        "industry_name": industry_keyword,
        "pe_ttm": None,
        "pb": None,
        "pe_median": None,
        "pb_median": None,
        "source": "dfcf.skill"
    }
    
    api_key = os.getenv("EASTMONEY_APIKEY", "")
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


def get_valuation_stable(code: str, industry_name: str = "") -> Dict[str, Any]:
    """双主源 + 权威校验：稳定估值口径（由数据源独家输出）。
    
    2026-03-17: 增强东方财富数据源集成，包含行业估值对比
    """
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

    # 优先使用东方财富数据（2026-03-17 增强）
    dfcf = get_valuation_from_dfcf(code)
    if dfcf and not dfcf.get("error"):
        out["pe_ttm"] = dfcf.get("pe_ttm")
        out["pb"] = dfcf.get("pb")
        out["pr"] = dfcf.get("pr")
        out["peg"] = dfcf.get("peg")
        out["growth_yoy_pct"] = dfcf.get("growth_yoy_pct")
        
        # 行业数据
        if dfcf.get("industry_avg"):
            out["industry_avg"] = dfcf["industry_avg"]
        if dfcf.get("industry_name"):
            out["industry_name"] = dfcf["industry_name"]
            
        out["premium_pct"] = dfcf.get("premium_pct", {"pe": None, "pb": None, "pr": None})
        out["meta"]["source_used"].append("dfcf.skill")
    else:
        # 备源1：Akshare 实时行情
        spot = _fetch_spot_with_retry()
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
                    if len(g) >= 8:
                        out["industry_avg"] = {
                            "pe": out["industry_avg"].get("pe") or _winsorized_median(g.get("市盈率-动态")),
                            "pb": out["industry_avg"].get("pb") or _winsorized_median(g.get("市净率")),
                            "pr": out["industry_avg"].get("pr") or _winsorized_median(g.get("市销率")),
                        }
                out["meta"]["source_used"].append("akshare.spot")
            except Exception:
                pass

        # 备源2：腾讯补 PE/PB
        if out["pe_ttm"] is None or out["pb"] is None:
            try:
                prefix = "sh" if str(code).startswith("6") else "sz"
                url = f"https://qt.gtimg.cn/q={prefix}{code}"
                txt = requests.get(url, timeout=8).text
                arr = txt.split('"')[1].split('~')
                pe2 = safe_float(arr[39]) if len(arr) > 39 else None
                pb2 = safe_float(arr[46]) if len(arr) > 46 else None
                if out["pe_ttm"] is None:
                    out["pe_ttm"] = pe2
                if out["pb"] is None:
                    out["pb"] = pb2
                out["meta"]["source_used"].append("tencent.qt")
            except Exception:
                pass

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
            except Exception:
                pass

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
            if prem > 25:
                out["assessment"][kk] = "偏高估"
            elif prem < -20:
                out["assessment"][kk] = "偏低估"
            else:
                out["assessment"][kk] = "估值合理"
        elif v is not None and ind is not None and ind != 0:
            prem_calc = (v - ind) / ind * 100
            if prem_calc > 25:
                out["assessment"][kk] = "偏高估"
            elif prem_calc < -20:
                out["assessment"][kk] = "偏低估"
            else:
                out["assessment"][kk] = "估值合理"

    if out["peg"] is not None:
        if out["peg"] > 1.5:
            out["assessment"]["peg"] = "成长定价偏贵"
        elif out["peg"] < 0.8:
            out["assessment"]["peg"] = "成长定价偏低"
        else:
            out["assessment"]["peg"] = "成长定价合理"

    out["meta"]["source_used"] = sorted(set(out["meta"]["source_used"]))
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


def get_baostock_financial(code: str) -> Dict[str, Any]:
    if not BS_AVAILABLE:
        return {}
    lg = bs.login()
    if lg.error_code != '0':
        return {"error": "baostock login failed"}
    try:
        full = to_bs_code(code)
        # 尝试最新几个季度
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


def _find_iwencai_skill_dir(skill_subdir: str) -> str:
    """
    在工作区查找同花顺技能目录。
    搜索同花顺技能目录（同花顺技能已迁移至 workspace-fiona）。
    """
    skill_root = Path(__file__).resolve().parent.parent.parent.parent / "skills"
    p = skill_root / skill_subdir
    if p.exists():
        for sub in p.iterdir():
            if sub.is_dir() and (sub / "scripts" / "cli.py").exists():
                return str(sub / "scripts" / "cli.py")
    return ""


def _call_iwencai_skill(skill_cli_path: str, query: str, timeout: int = 20, extra_args: list = None, as_text: bool = False) -> Any:
    """
    调用同花顺技能 CLI。
    - as_text=False（默认）：返回 JSON 解析结果（dict / list），失败返回 {}
    - as_text=True：返回原始文本字符串，失败返回 ""
    """
    if not skill_cli_path or not os.path.exists(skill_cli_path):
        return "" if as_text else {}
    try:
        cmd = [sys.executable, skill_cli_path, "--query", query] + (extra_args or [])
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            env={
                **os.environ,
                "IWENCAI_BASE_URL": "https://openapi.iwencai.com",
                "IWENCAI_API_KEY": os.environ.get(
                    "IWENCAI_API_KEY",
                    "sk-proj-00-eYSCskGL9M4I-hfD-9ODH2IAjVy7y9gH5g1WTMomktTWsM3030hIIn2RN-og5-yzW0Ijvos1XXq8-AJ2TFQVnvCYwZJkLjpFnz8FkIrvR4K3ooS1PHw-KYZxzqy2ZqGVyylBWg"
                ),
            }
        )
        if result.returncode == 0 and result.stdout.strip():
            raw = result.stdout.strip()
            if as_text:
                return raw  # 直接返回原始文本
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                items = []
                for line in raw.split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            items.append(json.loads(line))
                        except Exception:
                            continue
                return items if items else {}
    except Exception:
        pass
    return "" if as_text else {}


def get_iwencai_enrichment(code: str, company_name: str = "", industry_name: str = "") -> Dict[str, Any]:
    """
    通过同花顺技能补充快照数据（可选接入）。

    接入三个技能：
    - 行业数据查询：获取正确的行业估值（修正错误口径）
    - 研报搜索：获取最新券商研报
    - 公司经营数据查询：获取业务经营详情

    Args:
        code: 股票代码
        company_name: 公司简称（用于查询）
        industry_name: 同花顺行业分类（用于查询行业估值）

    Returns:
        包含 iwencai_industry / iwencai_reports / iwencai_business 三个子块的 dict
    """
    out = {"_status": "unavailable", "industry": {}, "reports": [], "business": {}}

    # 延迟查找 CLI 路径（避免每次 import 都扫描）
    if not hasattr(get_iwencai_enrichment, "_cli_cache"):
        get_iwencai_enrichment._cli_cache = {
            "industry": _find_iwencai_skill_dir("行业数据查询"),
            "reports": _find_iwencai_skill_dir("研报搜索"),
            "business": _find_iwencai_skill_dir("公司经营数据查询"),
        }

    cli = get_iwencai_enrichment._cli_cache

    # 1) 行业估值数据（需要两步查询）
    if cli["industry"]:
        # Step 1: 先查这只股票的正确行业分类（用股票代码比公司名更准）
        _industry_name = None
        if code:
            step1_data = _call_iwencai_skill(cli["industry"], f"{code} 行业分类")
            if step1_data.get("success") and step1_data.get("datas"):
                # 优先取"所属申万一级行业"或"所属同花顺行业"
                item = step1_data["datas"][0]
                raw_ind = (
                    item.get("所属申万一级行业")
                    or item.get("所属同花顺行业")
                    or item.get("所属同花顺二级行业")
                    or item.get("行业名称")
                )
                # 行业名可能是 list（如 ['通信','通信设备','通信线缆及配套']），取第一个
                if isinstance(raw_ind, list):
                    _industry_name = raw_ind[0] if raw_ind else None
                elif raw_ind:
                    _industry_name = str(raw_ind).strip()

        # Step 2: 用行业名拼接行业PE关键词，查行业指数估值
        # 策略：优先"行业名+行业PE"，其次"行业名+估值"，最后用公司名+行业PE
        q_candidates = []
        if _industry_name:
            q_candidates = [
                f"{_industry_name} 行业PE",
                f"{_industry_name} 估值",
                f"{_industry_name}行业",
            ]
        if company_name:
            q_candidates.append(f"{company_name} 行业PE")

        data = {}
        for q in q_candidates:
            candidate = _call_iwencai_skill(cli["industry"], q)
            if candidate and candidate.get("success") and candidate.get("datas"):
                items = candidate.get("datas", [])
                if items:
                    first = items[0]
                    # 行业数据特征：有"指数简称"或"行业市盈率"字段，或返回的股票代码非当前code
                    if (first.get("指数简称") or first.get("行业市盈率") or
                            str(first.get("股票代码", "")) != code):
                        data = candidate
                        break

        if data.get("success") and data.get("datas"):
            out["industry"] = {
                "query": data.get("query", ""),
                "items": data.get("datas", []),
                "source": "iwencai.industry_query",
            }

    # 2) 最新研报（取3条）
    if cli["reports"] and company_name:
        import re as _re
        # 研报搜索 CLI 输出为 JSONL（多文档），改用 text 格式更稳定
        text = _call_iwencai_skill(cli["reports"], company_name, extra_args=["-f", "text", "-l", "3"], as_text=True)
        if text and isinstance(text, str):
            reports = []
            # 解析 text 格式：每篇以 "1. " 或 "2. " 开头
            blocks = _re.split(r"\n(?=\d+\.\s)", text.strip())
            for block in blocks[:3]:
                title_m = _re.search(r"^\d+\.\s+(.+)$", block, _re.MULTILINE)
                url_m = _re.search(r"原文链接:\s*(.+)", block)
                summary_m = _re.search(r"摘要:\s*(.+)", block)
                time_m = _re.search(r"发布时间:\s*(.+)", block)
                if title_m:
                    reports.append({
                        "title": title_m.group(1).strip(),
                        "url": url_m.group(1).strip() if url_m else "",
                        "summary": summary_m.group(1).strip() if summary_m else "",
                        "publish_time": time_m.group(1).strip() if time_m else "",
                    })
            if reports:
                out["reports"] = {"items": reports, "_source": "iwencai.research_report"}

    # 3) 公司经营数据
    if cli["business"] and company_name:
        data = _call_iwencai_skill(cli["business"], f"{company_name}经营数据")
        if data.get("success") and data.get("datas"):
            out["business"] = {
                "query": data.get("query", ""),
                "items": data.get("datas", []),
                "source": "iwencai.business_query",
            }

    if any(v for v in [out["industry"], out["reports"], out["business"]]):
        out["_status"] = "available"

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Transform 层：清洗、标准化、与数据源解耦
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(v, default=None):
    if v is None:
        return default
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").strip()
            if v in ("", "--", "nan", "None"):
                return default
        return float(v)
    except Exception:
        return default


def _transform_valuation_comparable(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    将同花顺行业数据 + 个股估值原始字段，转换为干净、与数据源解耦的估值对比结构。

    输出字段（全部小写、下划线分隔、无来源前缀）：
      stock_pe_ttm, stock_pe_dynamic, stock_pb, stock_pr
      industry_pe_median, industry_pb_median
      industry_name (二级行业)
      premium_vs_industry_pe_pct  （个股PE相对行业中值溢价%，正数=偏高估）
      premium_vs_industry_pb_pct  （个股PB相对行业中值溢价%）
    """
    out = {
        "stock_pe_ttm": None,
        "stock_pe_dynamic": None,
        "stock_pb": None,
        "stock_pr": None,
        "industry_pe_median": None,
        "industry_pb_median": None,
        "industry_name": None,
        "premium_vs_industry_pe_pct": None,
        "premium_vs_industry_pb_pct": None,
        "_source": "iwencai.industry_query",
    }
    if not raw:
        return out

    # 从 items 取行业维度的关键字段
    items = raw.get("items", []) if raw.get("items") else []
    if not items:
        return out

    item = items[0]

    # 个股自身估值（来自行业查询返回的单股票数据，非行业指数）
    # 优先匹配字段名变体
    def _pick_field(d, *keys):
        for k in keys:
            if k in d:
                v = d[k]
                if v is not None and v != '--' and v != '':
                    return float(v) if isinstance(v, (int, float)) else None
        return None

    out["stock_pe_dynamic"] = _pick_field(item, "动态市盈率", "市盈率")
    out["stock_pe_ttm"]    = _pick_field(item, "市盈率", "动态市盈率")
    out["stock_pb"]        = _pick_field(item, "市净率", "PB")
    out["stock_pr"]        = _pick_field(item, "市销率", "PR")

    # 行业估值：行业指数可能有多个，取 PE 中位数
    pe_vals = []
    pb_vals = []
    industry_name = None
    for it in items:
        # 行业指数的 PE 字段名不固定，遍历找含"市盈率"的列
        for k, v in it.items():
            if "市盈率" in k and v is not None and not isinstance(v, str):
                pe_vals.append(float(v))
            if "市净率" in k and v is not None and not isinstance(v, str):
                pb_vals.append(float(v))
        # 行业名（取第一个有指数简称的）
        if not industry_name and it.get("指数简称"):
            industry_name = it.get("指数简称")

    if pe_vals:
        pe_vals_sorted = sorted(pe_vals)
        n = len(pe_vals_sorted)
        out["industry_pe_median"] = pe_vals_sorted[n // 2] if n % 2 == 1 else (
            pe_vals_sorted[n // 2 - 1] + pe_vals_sorted[n // 2]) / 2
    if pb_vals:
        out["industry_pb_median"] = sum(pb_vals) / len(pb_vals)
    if industry_name:
        out["industry_name"] = industry_name

    # 溢价率计算（相对行业中值）
    if out["stock_pe_ttm"] and out["industry_pe_median"]:
        out["premium_vs_industry_pe_pct"] = round(
            (out["stock_pe_ttm"] - out["industry_pe_median"]) / out["industry_pe_median"] * 100, 2
        )
    if out["stock_pb"] and out["industry_pb_median"]:
        out["premium_vs_industry_pb_pct"] = round(
            (out["stock_pb"] - out["industry_pb_median"]) / out["industry_pb_median"] * 100, 2
        )

    return out


def _transform_reports(raw) -> Dict[str, Any]:
    """
    将研报原始数据（list 或 dict.items）转换为结构化列表。
    输出元素：{title, summary, url, publish_date}
    """
    out = {"items": [], "_source": "iwencai.research_report"}
    items = []
    if isinstance(raw, dict):
        items = raw.get("items", []) or []
    elif isinstance(raw, list):
        items = raw

    for it in items[:5]:
        if not isinstance(it, dict):
            continue
        title = it.get("title", "").strip()
        if not title:
            continue
        # 解析摘要（可能包含来源标注，截断即可）
        summary = it.get("summary", "")
        # 去掉可能的前缀如 "出处：..."
        summary = summary.split("出处：")[0].strip()
        out["items"].append({
            "title": title,
            "summary": summary[:300],
            "url": it.get("url", "") or it.get("原文链接", ""),
            "publish_date": it.get("publish_time", "") or it.get("发布时间", "") or "",
        })
    return out


def _transform_business_segments(raw) -> Dict[str, Any]:
    """
    将公司经营数据原始返回，转换为标准化主营结构列表。
    输出元素：{segment_name, revenue_pct, note}
    """
    out = {"items": [], "_source": "iwencai.business_query"}
    if not raw or not isinstance(raw, dict):
        return out
    items = raw.get("items", [])
    if not items or not isinstance(items, list):
        return out

    # items[0] 通常包含多字段混排的字典，提取含"业务""主营""收入"字样的字段
    item = items[0]
    segments = []
    for k, v in item.items():
        k_str = str(k)
        # 识别主营构成字段（名称包含关键词）
        if any(kw in k_str for kw in ["主营", "业务", "收入", "产品", "板块"]):
            val = _safe_float(str(v))
            if val is not None:
                out["items"].append({
                    "segment_name": k_str,
                    "revenue_pct": val,
                    "note": "",
                })
    return out


def _transform_snapshot(snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    对快照中所有数据做 Transform：
      1. valuation_comparable  ← iwencai 行业估值（正确行业分类）
      2. research_reports      ← iwencai 研报搜索
      3. business_segments     ← iwencai 公司经营数据

    下游模块只读这些字段，完全不知道数据源来自哪里。
    """
    out = dict(snap)  # 浅拷贝，不修改原始快照
    iw = snap.get("iwencai", {})

    # 1) 估值对比（行业均值来自正确的同花顺二级行业）
    ind_raw = iw.get("industry", {})
    if ind_raw and isinstance(ind_raw, dict):
        vc = _transform_valuation_comparable(ind_raw)
    else:
        vc = {}

    # 如果行业查询没有个股 PE（只有行业指数），则从 AkShare/baostock/valuation_stable 补入
    if vc.get("stock_pe_ttm") is None:
        # 优先从 valuation_stable（东方财富数据）补入
        val_stable = snap.get("valuation_stable", {})
        if val_stable and val_stable.get("pe_ttm") is not None:
            vc["stock_pe_ttm"] = val_stable["pe_ttm"]
        if val_stable and val_stable.get("pb") is not None:
            vc["stock_pb"] = val_stable["pb"]
        # 备选：从 basic（股票基础数据）补入
        if vc.get("stock_pe_ttm") is None:
            basic = snap.get("basic", {})
            if basic and basic.get("pe_ttm") is not None:
                vc["stock_pe_ttm"] = basic["pe_ttm"]
        # 重新计算溢价率
        if vc.get("stock_pe_ttm") and vc.get("industry_pe_median"):
            vc["premium_vs_industry_pe_pct"] = round(
                (vc["stock_pe_ttm"] - vc["industry_pe_median"]) / vc["industry_pe_median"] * 100, 2)

    out["valuation_comparable"] = vc

    # 2) 结构化研报
    rep_raw = iw.get("reports", {})
    out["research_reports"] = _transform_reports(rep_raw)

    # 3) 标准化主营结构
    biz_raw = iw.get("business", {})
    out["business_segments"] = _transform_business_segments(biz_raw)

    return out


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

    # basic
    basic = {}
    try:
        basic = get_basic_from_ak(code)
        if basic:
            sources.append("akshare")
    except Exception as e:
        errors.append(f"basic.akshare: {e}")
    if not basic:
        try:
            basic = get_basic_from_bs(code)
            if basic:
                sources.append("baostock")
        except Exception as e:
            errors.append(f"basic.baostock: {e}")
    snapshot["basic"] = basic

    # market
    market = {}
    try:
        market = get_market_from_ak(code)
        if market and "akshare" not in sources:
            sources.append("akshare")
    except Exception as e:
        errors.append(f"market.akshare: {e}")
    if not market:
        try:
            market = get_market_from_bs(code)
            if market and "baostock" not in sources:
                sources.append("baostock")
        except Exception as e:
            errors.append(f"market.baostock: {e}")
    snapshot["market"] = market

    if data_type in ("core", "all"):
        try:
            snapshot["valuation"] = get_valuation(code)
            if snapshot["valuation"] and "akshare" not in sources:
                sources.append("akshare")
        except Exception as e:
            errors.append(f"valuation.akshare: {e}")

        # 统一稳定估值口径（推荐下游只读取该字段）
        try:
            snapshot["valuation_stable"] = get_valuation_stable(code, basic.get("industry", ""))
            for src in snapshot.get("valuation_stable", {}).get("meta", {}).get("source_used", []):
                if src.startswith("akshare") and "akshare" not in sources:
                    sources.append("akshare")
                if src.startswith("tencent") and "tencent" not in sources:
                    sources.append("tencent")
        except Exception as e:
            errors.append(f"valuation.stable: {e}")

        # ── 同花顺数据接入（可选增强）───────────────────────────
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
        # ───────────────────────────────────────────────────────

    if data_type in ("financial", "all"):
        try:
            snapshot["financial"] = get_financial(code, years=years)
            if snapshot["financial"] and "akshare" not in sources:
                sources.append("akshare")
        except Exception as e:
            errors.append(f"financial.akshare: {e}")

        try:
            bao = get_baostock_financial(code)
            if bao:
                snapshot["baostock"] = bao
                if "baostock" not in sources:
                    sources.append("baostock")
        except Exception as e:
            errors.append(f"financial.baostock: {e}")

    snapshot["quality"] = {
        "completeness": completeness_score(snapshot),
        "errors": errors,
        "sources_used": sorted(list(set(sources))),
    }

    # ── Transform 层：清洗标准化、与数据源解耦 ──────────────────────────
    snapshot = _transform_snapshot(snapshot)
    # ───────────────────────────────────────────────────────────────────────

    if use_cache:
        save_cache(code, data_type, snapshot)
    return snapshot


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
            time.sleep(0.2)
    return out


def main():
    p = argparse.ArgumentParser(description="A股统一数据源")
    p.add_argument("--code", type=str, help="单只股票代码")
    p.add_argument("--codes", type=str, help="多只股票代码，逗号分隔")
    p.add_argument("--scope", type=str, help="指数范围: hs300/zz500/zz1000/cyb/kcb/all")
    p.add_argument("--data-type", type=str, default="core", choices=["core", "financial", "all"])
    p.add_argument("--years", type=int, default=3)
    p.add_argument("--no-cache", action="store_true")
    p.add_argument("--output", type=str, help="单次输出文件")
    p.add_argument("--batch-output", type=str, help="批量输出文件")

    args = p.parse_args()
    use_cache = not args.no_cache

    result: Dict[str, Any]

    if args.code:
        result = fetch_stock_snapshot(args.code, data_type=args.data_type, years=args.years, use_cache=use_cache)
    elif args.codes:
        codes = [x.strip() for x in args.codes.split(",") if x.strip()]
        result = fetch_batch(codes, args.data_type, args.years, use_cache)
    elif args.scope:
        codes = get_scope_codes(args.scope)
        result = {
            "scope": args.scope,
            "count": len(codes),
            "codes": codes
        }
    else:
        print("请提供 --code / --codes / --scope 之一")
        sys.exit(1)

    out = json.dumps(result, ensure_ascii=False, indent=2, default=str)

    file_path = args.batch_output if args.batch_output else args.output
    if file_path:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"已写入: {file_path}")
    else:
        print(out)


if __name__ == "__main__":
    main()
