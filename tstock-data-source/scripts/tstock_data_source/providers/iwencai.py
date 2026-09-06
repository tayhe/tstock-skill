"""同花顺（iwencai）数据源：行业数据、研报、公司经营数据。"""

import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent.parent))
import config

from tstock_lib.constants import HTTP_TIMEOUT_IWENCAI

logger = logging.getLogger(__name__)


def _find_iwencai_skill_dir(skill_subdir: str) -> str:
    p = config.IWENCAI_SKILLS_ROOT / skill_subdir
    if not p.exists():
        return ""
    for name in ("cli.py", "report_search.py"):
        cli = p / "scripts" / name
        if cli.exists():
            return str(cli)
    for sub in p.iterdir():
        if sub.is_dir():
            for name in ("cli.py", "report_search.py"):
                candidate = sub / "scripts" / name
                if candidate.exists():
                    return str(candidate)
    return ""


def _call_iwencai_skill(skill_cli_path: str, query: str, timeout: int = HTTP_TIMEOUT_IWENCAI,
                         extra_args: list = None, as_text: bool = False) -> Any:
    if not skill_cli_path or not os.path.exists(skill_cli_path):
        return "" if as_text else {}
    try:
        cmd = [sys.executable, skill_cli_path, "--query", query] + (extra_args or [])
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            env={
                **os.environ,
                "IWENCAI_BASE_URL": config.IWENCAI_BASE_URL,
                "IWENCAI_API_KEY": config.IWENCAI_API_KEY,
            }
        )
        if result.returncode == 0 and result.stdout.strip():
            raw = result.stdout.strip()
            if as_text:
                return raw
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
    """
    out = {"_status": "unavailable", "industry": {}, "reports": [], "business": {}}

    if not hasattr(get_iwencai_enrichment, "_cli_cache"):
        get_iwencai_enrichment._cli_cache = {
            "industry": _find_iwencai_skill_dir("hithink-industry-query"),
            "reports": _find_iwencai_skill_dir("report-search"),
            "business": _find_iwencai_skill_dir("hithink-business-query"),
        }

    cli = get_iwencai_enrichment._cli_cache

    # 1) 行业估值数据
    if cli["industry"]:
        _industry_name = None
        if code:
            step1_data = _call_iwencai_skill(cli["industry"], f"{code} 行业分类")
            if step1_data.get("success") and step1_data.get("datas"):
                item = step1_data["datas"][0]
                raw_ind = (
                    item.get("所属申万一级行业")
                    or item.get("所属同花顺行业")
                    or item.get("所属同花顺二级行业")
                    or item.get("行业名称")
                )
                if isinstance(raw_ind, list):
                    _industry_name = raw_ind[0] if raw_ind else None
                elif raw_ind:
                    _industry_name = str(raw_ind).strip()

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

        # 补充查询行业 PB（行业 PE 查询通常不返回 PB）
        if _industry_name:
            pb_data = _call_iwencai_skill(cli["industry"], f"{_industry_name} PB")
            if pb_data.get("success") and pb_data.get("datas"):
                out["industry_pb"] = {
                    "query": pb_data.get("query", ""),
                    "items": pb_data.get("datas", []),
                    "source": "iwencai.industry_query",
                }

        # 补充查询个股自身 PE/PB（行业查询返回的是行业样本，不含目标个股）
        if code:
            stock_data = _call_iwencai_skill(cli["industry"], f"{code} 市盈率TTM 市净率PB")
            if stock_data.get("success") and stock_data.get("datas"):
                out["stock_valuation"] = stock_data["datas"][0] if stock_data["datas"] else {}

    # 2) 最新研报
    if cli["reports"] and company_name:
        resp = _call_iwencai_skill(cli["reports"], company_name, extra_args=["--limit", "3"])
        reports = []
        if isinstance(resp, dict):
            articles = resp.get("data", [])
            for item in articles[:3]:
                if isinstance(item, dict):
                    reports.append({
                        "title": item.get("title", "").strip(),
                        "url": item.get("url", "").strip(),
                        "summary": item.get("summary", "").strip(),
                        "publish_time": (item.get("publish_date") or item.get("publish_time") or "").strip(),
                    })
        elif isinstance(resp, str) and resp.strip():
            # 兼容旧版纯文本输出
            blocks = re.split(r"\n(?=\d+\.\s)", resp.strip())
            for block in blocks[:3]:
                title_m = re.search(r"^\d+\.\s+(.+)$", block, re.MULTILINE)
                url_m = re.search(r"原文链接:\s*(.+)", block)
                summary_m = re.search(r"摘要:\s*(.+)", block)
                time_m = re.search(r"发布时间:\s*(.+)", block)
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
