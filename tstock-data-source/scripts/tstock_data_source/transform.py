"""Transform 层：清洗、标准化、与数据源解耦。"""

from typing import Any, Dict

from tstock_lib.utils import safe_float


def _transform_valuation_comparable(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    将同花顺行业数据 + 个股估值原始字段，转换为干净、与数据源解耦的估值对比结构。
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

    items = raw.get("items", []) if raw.get("items") else []
    if not items:
        return out

    item = items[0]

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

    pe_vals = []
    pb_vals = []
    industry_name = None
    for it in items:
        for k, v in it.items():
            if "市盈率" in k and v is not None and not isinstance(v, str):
                pe_vals.append(float(v))
            if "市净率" in k and v is not None and not isinstance(v, str):
                pb_vals.append(float(v))
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
    """将研报原始数据转换为结构化列表。"""
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
        summary = it.get("summary", "")
        summary = summary.split("出处：")[0].strip()
        out["items"].append({
            "title": title,
            "summary": summary[:300],
            "url": it.get("url", "") or it.get("原文链接", ""),
            "publish_date": it.get("publish_time", "") or it.get("发布时间", "") or "",
        })
    return out


def _transform_business_segments(raw) -> Dict[str, Any]:
    """将公司经营数据原始返回，转换为标准化主营结构列表。"""
    out = {"items": [], "_source": "iwencai.business_query"}
    if not raw or not isinstance(raw, dict):
        return out
    items = raw.get("items", [])
    if not items or not isinstance(items, list):
        return out

    item = items[0]
    for k, v in item.items():
        k_str = str(k)
        if any(kw in k_str for kw in ["主营", "业务", "收入", "产品", "板块"]):
            val = safe_float(str(v))
            if val is not None:
                out["items"].append({
                    "segment_name": k_str,
                    "revenue_pct": val,
                    "note": "",
                })
    return out


def transform_snapshot(snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    对快照中所有数据做 Transform：
      1. valuation_comparable  ← iwencai 行业估值
      2. research_reports      ← iwencai 研报搜索
      3. business_segments     ← iwencai 公司经营数据
    """
    out = dict(snap)
    iw = snap.get("iwencai", {})

    # 1) 估值对比
    ind_raw = iw.get("industry", {})
    if ind_raw and isinstance(ind_raw, dict):
        vc = _transform_valuation_comparable(ind_raw)
    else:
        vc = {}

    if vc.get("stock_pe_ttm") is None:
        val_stable = snap.get("valuation_stable", {})
        if val_stable and val_stable.get("pe_ttm") is not None:
            vc["stock_pe_ttm"] = val_stable["pe_ttm"]
        if val_stable and val_stable.get("pb") is not None:
            vc["stock_pb"] = val_stable["pb"]
        if vc.get("stock_pe_ttm") is None:
            basic = snap.get("basic", {})
            if basic and basic.get("pe_ttm") is not None:
                vc["stock_pe_ttm"] = basic["pe_ttm"]
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
