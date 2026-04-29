#!/usr/bin/env python3
"""
China Stock Alpha Orchestrator
==============================
兼容入口：统一编排各独立 skill，生成整合分析结果。

流程：
1) 统一数据源快照 (tstock-data-source)
2) 基本面分析 (tstock-fundamental_analyzer)
3) 技术面分析 (tstock-technical_analyzer)
4) 风险评估 (tstock-risk_analyzer)
5) 策略规划 (tstock-portfolio/strategy_planner)
"""

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

# 动态推导 workspace 根目录（相对于脚本位置）
# 脚本位于 skills/tstock-workflow/scripts/，向上四级到 workspace 根
WORKSPACE = Path(__file__).resolve().parent.parent.parent.parent

PATHS = {
    "data_source": WORKSPACE / "skills/tstock-data-source/scripts/data_source.py",
    "fundamental": WORKSPACE / "skills/tstock-fundamental_analyzer/scripts/fundamental_analyzer.py",
    "technical": WORKSPACE / "skills/tstock-technical_analyzer/scripts/technical_analyzer.py",
    "risk": WORKSPACE / "skills/tstock-risk_analyzer/scripts/risk_evaluator.py",
    "strategy": WORKSPACE / "skills/tstock-portfolio/scripts/strategy_planner.py",
}


def _run(cmd: list[str]):
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"命令失败: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return proc.stdout.strip()


def _load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _ensure_scripts_exist():
    missing = [k for k, p in PATHS.items() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"缺少脚本: {missing}")


def _fmt_pct(v, decimals=2):
    """格式化百分比，小数位控制。"""
    if v is None:
        return "N/A"
    return f"{round(v * 100, decimals)}%"


def _fmt_float(v, decimals=2):
    """格式化浮点数，小数位控制。"""
    if v is None:
        return "N/A"
    return f"{round(v, decimals)}"


def _bullet_list(items, indent=2):
    """把列表渲染为带缩进的 markdown 无序列表。"""
    if not items:
        return ""
    sep = " " * indent
    return "\n".join(f"{sep}- {item}" for item in items)


def run_analysis(code: str, keep_tmp: bool = False, refresh_data: bool = False) -> dict:
    _ensure_scripts_exist()

    tmp_dir = Path("/tmp/china_stock_alpha_orchestrator")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    snapshot = tmp_dir / f"{code}_snapshot.json"
    fundamental = tmp_dir / f"{code}_fundamental.json"
    technical = tmp_dir / f"{code}_technical.json"
    risk = tmp_dir / f"{code}_risk.json"
    strategy = tmp_dir / f"{code}_strategy.json"

    # 1) 统一数据源
    data_cmd = [
        "python3", str(PATHS["data_source"]),
        "--code", code,
        "--data-type", "all",
        "--output", str(snapshot)
    ]
    if refresh_data:
        data_cmd.append("--no-cache")
    _run(data_cmd)

    # 2) 基本面
    _run([
        "python3", str(PATHS["fundamental"]),
        "--snapshot", str(snapshot),
        "--output", str(fundamental)
    ])

    # 3) 技术面
    _run([
        "python3", str(PATHS["technical"]),
        "--snapshot", str(snapshot),
        "--output", str(technical)
    ])

    # 4) 风险
    _run([
        "python3", str(PATHS["risk"]),
        "--snapshot", str(snapshot),
        "--fundamental-json", str(fundamental),
        "--output", str(risk)
    ])

    # 5) 策略
    _run([
        "python3", str(PATHS["strategy"]),
        "--code", code,
        "--fundamental", str(fundamental),
        "--technical", str(technical),
        "--risk", str(risk),
        "--output", str(strategy)
    ])

    data = _load_json(snapshot)
    fund = _load_json(fundamental)
    tech = _load_json(technical)
    rsk = _load_json(risk)
    stg = _load_json(strategy)

    report = {
        "code": code,
        "name": data.get("basic", {}).get("name", ""),
        "analysis_time": datetime.now().isoformat(),
        "pipeline": [
            "data_source",
            "fundamental_analyzer",
            "technical_analyzer",
            "tstock-risk_analyzer"
            "strategy_planner",
        ],
        "snapshot_quality": data.get("quality", {}),
        "summary": {
            "fundamental_score": fund.get("score"),
            "technical_trend": tech.get("trend"),
            "risk": {
                "overall": rsk.get("overall_risk"),
                "score": rsk.get("risk_score"),
            },
            "strategy": {
                "action": stg.get("action"),
                "score": stg.get("score"),
                "position": stg.get("position_recommendation"),
                "stop_ref": stg.get("stop_ref"),
            },
        },
        "outputs": {
            "snapshot": str(snapshot),
            "fundamental": str(fundamental),
            "technical": str(technical),
            "risk": str(risk),
            "strategy": str(strategy),
        },
        "details": {
            "fundamental": fund,
            "technical": tech,
            "risk": rsk,
            "strategy": stg,
        }
    }

    return report


# ─────────────────────────────────────────────────────────────────────────────
# 报告渲染
# ─────────────────────────────────────────────────────────────────────────────

def _build_markdown_report(report: dict) -> str:
    d = report.get("details", {})
    fund = d.get("fundamental", {})
    tech = d.get("technical", {})
    rsk = d.get("risk", {})
    stg = d.get("strategy", {})

    lines = []

    # ═══════════════════════════════════════════════════════════════════════════
    # 头部
    # ═══════════════════════════════════════════════════════════════════════════
    lines.append(f"# {report.get('name', '')}（{report.get('code')}）完整分析报告")
    lines.append("")
    lines.append(f"**分析时间**：{report.get('analysis_time', '')}")
    q = report.get("snapshot_quality", {})
    completeness = q.get("completeness", "N/A")
    errors = q.get("errors", [])
    sources = ", ".join(q.get("sources_used", []) or ["未知"])
    lines.append(f"**数据完整性**：{completeness}（来源：{sources}）")
    if errors:
        lines.append(f"**数据警告**：{errors}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ═══════════════════════════════════════════════════════════════════════════
    # 模块一：基本面分析（tstock-fundamental_analyzer）
    # ═══════════════════════════════════════════════════════════════════════════
    lines.append("## 📊 模块一：基本面分析（tstock-fundamental_analyzer）")
    lines.append("")

    # 1.1 投资认知总览（四维打分卡）
    qual = fund.get("qualitative", {})
    sc = fund.get("scorecard", {})
    lines.append("### 1.1 投资认知总览（四维打分）")
    lines.append("")

    def _score_bar(v, max_=10):
        filled = "★" * v + "☆" * (max_ - v)
        return f"[{filled}] {v}/{max_}"

    lines.append(f"| 维度 | 评分 | 说明 |")
    lines.append(f"|------|------|------|")

    macro_v = sc.get("policy", 0)
    macro_view = qual.get("macro_policy", {}).get("view", "未识别")
    lines.append(f"| 宏观政策环境 | {_score_bar(macro_v)} | {macro_view} |")

    ind_v = sc.get("industry", 0)
    moat = qual.get("industry_competition", {}).get("moat_level", "未识别")
    lines.append(f"| 行业竞争格局 | {_score_bar(ind_v)} | 壁垒：{moat} |")

    moat_v = sc.get("moat", 0)
    cp_summary = qual.get("company_profile", {}).get("summary", "未形成稳定画像")
    lines.append(f"| 公司业务壁垒 | {_score_bar(moat_v)} | {cp_summary[:40]}... |")

    growth_v = sc.get("growth", 0)
    growth_hl = (qual.get("growth_map", {}).get("highlights") or [[]])[0]
    top_growth = str(growth_hl[0])[:50] if growth_hl else "暂无"
    lines.append(f"| 未来增长空间 | {_score_bar(growth_v)} | {top_growth}... |")

    total = sc.get("total", 0)
    max_sc = sc.get("max", 40)
    lines.append(f"| **{'综合':^{12}}** | **[{'★' * round(total/4)}{'☆' * (10 - round(total/4))}] {total}/{max_sc}（≈{round(total/max_sc*100)}%）** | 总分 |")
    lines.append("")

    # 1.2 定量财务数据
    prof = fund.get("profitability", {})
    health = fund.get("financial_health", {})
    val = fund.get("valuation", {})
    lines.append("### 1.2 定量财务数据")
    lines.append("")

    lines.append("**盈利能力**")
    roe = prof.get("roe")
    nm = prof.get("net_margin")
    gm = prof.get("gross_margin")
    lines.append(f"| 指标 | 数值 | 参考 |")
    lines.append(f"|------|------|------|")
    lines.append(f"| ROE（净资产收益率） | {_fmt_pct(roe)} | 优秀 >15%，良好 10-15% |")
    lines.append(f"| 净利率 | {_fmt_pct(nm)} | 优秀 >15%，良好 8-15% |")
    lines.append(f"| 毛利率 | {_fmt_pct(gm)} | 优秀 >30%，良好 20-30% |")
    lines.append("")

    lines.append("**财务健康**")
    debt = health.get("debt_ratio")
    curr = health.get("current_ratio")
    lines.append(f"| 指标 | 数值 | 参考 |")
    lines.append(f"|------|------|------|")
    lines.append(f"| 资产负债率 | {_fmt_pct(debt)} | 健康 <50% |")
    lines.append(f"| 流动比率 | {_fmt_float(curr)} | 充足 >1.5，优秀 >2.0 |")
    lines.append("")

    # 1.3 估值分析
    val_meta = val.get("meta", {})
    lines.append("### 1.3 估值分析")
    lines.append("")
    lines.append(f"*（数据截止：{val_meta.get('as_of', 'N/A')}，基准：{val_meta.get('valuation_basis', 'PE/PB/PR/PEG')}）*")
    lines.append("")

    ia = val.get("industry_avg") or {}
    pp = val.get("premium_pct") or {}
    ass = val.get("assessment") or {}

    def _val_row(name, my_val, ind_avg, premium, assess, fmt="float"):
        def _f(v, is_pct=False):
            if v is None:
                return "N/A"
            if is_pct:
                return f"{round(v, 2)}%"
            return f"{round(v, 2)}"

        my_s = _f(my_val, fmt == "pct")
        ia_s = _f(ind_avg, fmt == "pct")
        pp_s = f"+{round(premium, 1)}%" if premium is not None else "N/A"
        return f"| {name} | {my_s} | {ia_s} | {pp_s} | {assess} |"

    lines.append("| 指标 | 我的数值 | 行业均值 | 溢价率 | 判断 |")
    lines.append("|------|---------|---------|--------|------|")
    lines.append(_val_row("PE（TTM）", val.get("pe_ttm"), ia.get("pe"), pp.get("pe"), ass.get("pe", "N/A")))
    lines.append(_val_row("PB", val.get("pb"), ia.get("pb"), pp.get("pb"), ass.get("pb", "N/A")))
    lines.append(_val_row("PR", val.get("pr"), ia.get("pr"), pp.get("pr"), ass.get("pr", "N/A")))
    peg = val.get("peg")
    peg_assess = ass.get("peg", "N/A")
    lines.append(f"| PEG | {_fmt_float(peg)} | — | — | {peg_assess} |")
    lines.append("")

    # 1.4 定性分析（宏观/业务/行业/增长）
    lines.append("### 1.4 定性分析")
    lines.append("")

    def _render_qual_section(title, data):
        view = data.get("view", "")
        highlights = data.get("highlights") or []
        refs = data.get("references") or []
        lines_out = []
        lines_out.append(f"**{title}**（{view}）")
        if highlights:
            for h in highlights:
                h = h.strip()
                lines_out.append(f"- {h}")
        else:
            lines_out.append("-（暂无有效信息）")
        if refs:
            lines_out.append("")
            lines_out.append("  **参考来源**：")
            for r in refs[:3]:
                lines_out.append(f"  - {r}")
        lines_out.append("")
        return lines_out

    for title, key in [
        ("📌 宏观政策要点", "macro_policy"),
        ("📌 主营与利润分布", "business_profile"),
        ("📌 行业地位与技术壁垒", "industry_competition"),
        ("📌 未来增长点", "growth_map"),
    ]:
        for line in _render_qual_section(title, qual.get(key, {})):
            lines.append(line)

    # 1.5 投资认知评分结论
    lines.append("### 1.5 投资认知评分结论")
    lines.append("")
    reasons = fund.get("reasons") or []
    view = fund.get("view", "")
    score = fund.get("score", "N/A")
    lines.append(f"**综合观点**：{view}（基本面评分：{score}）")
    if reasons:
        lines.append("")
        lines.append("**支撑依据**：")
        for r in reasons:
            lines.append(f"- {r}")
    lines.append("")

    # ═══════════════════════════════════════════════════════════════════════════
    # 模块二：技术面分析（tstock-technical_analyzer）
    # ═══════════════════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## 📈 模块二：技术面分析（tstock-technical_analyzer）")
    lines.append("")

    close = tech.get("close", "N/A")
    trend = tech.get("trend", "N/A")
    signals = tech.get("signals") or []
    lines.append(f"**当前收盘价**：{close} 元")
    lines.append(f"**趋势判断**：{trend}")
    if signals:
        sig_tags = " ".join(f"`{s}`" for s in signals)
        lines.append(f"**信号列表**：{sig_tags}")
    lines.append("")

    # 指标详情
    lines.append("**主要指标**")
    lines.append("")

    rsi = tech.get("rsi14")
    if rsi is not None:
        if rsi <= 25:
            rsi_note = "⚠️ 超卖严重，短线存在技术修复可能"
        elif rsi <= 30:
            rsi_note = "🔔 超卖区，关注反弹信号"
        elif rsi >= 75:
            rsi_note = "⚠️ 超买严重，警惕回调"
        elif rsi >= 70:
            rsi_note = "🔔 超买区，注意震荡/回落风险"
        else:
            rsi_note = "中性区间"
        lines.append(f"- **RSI（14日）**：{round(rsi, 2)} → {rsi_note}")

    macd_hist = tech.get("macd_hist")
    if macd_hist is not None:
        if macd_hist > 0:
            macd_note = "✅ 多头动能占优"
        elif macd_hist < 0:
            macd_note = "⚠️ 空头动能占优"
        else:
            macd_note = "— 多空动能均衡"
        lines.append(f"- **MACD 柱状图**：{round(macd_hist, 4)} → {macd_note}")

    kdj = tech.get("kdj") or {}
    k, d, jv = kdj.get("k"), kdj.get("d"), kdj.get("j")
    if k is not None:
        cross = "✅ 金叉，短线偏强" if k > d else "⚠️ 死叉，短线偏弱"
        j_note = ""
        if jv is not None:
            if jv > 100:
                j_note = "；J值高位，警惕回落"
            elif jv < 0:
                j_note = "；J值低位，留意修复"
        lines.append(f"- **KDJ**：K={round(k, 2)} D={round(d, 2)} J={round(jv, 2) if jv else 'N/A'} → {cross}{j_note}")

    boll = tech.get("boll") or {}
    up, mid, dn = boll.get("up"), boll.get("mid"), boll.get("dn")
    if up is not None:
        if close > up:
            boll_note = "突破上轨，短线偏强但有过热风险"
        elif close < dn:
            boll_note = "跌破下轨，短线偏弱或超卖"
        else:
            boll_note = "位于轨道内，震荡运行"
        lines.append(f"- **BOLL 布林带**：上轨={round(up, 2)} / 中轨={round(mid, 2)} / 下轨={round(dn, 2)} → {boll_note}")

    lines.append("")
    lines.append("**技术位参考**")
    lines.append("")
    sup = tech.get("support_20d", "N/A")
    res = tech.get("resistance_20d", "N/A")
    atr = tech.get("atr14")
    stop = tech.get("stop_ref", "N/A")
    lines.append(f"| 类型 | 价格（元） |")
    lines.append(f"|------|---------|")
    lines.append(f"| 支撑位 | {sup} |")
    lines.append(f"| 压力位 | {res} |")
    lines.append(f"| 止损参考 | {stop} |")
    if atr is not None and close:
        atr_pct = round(atr / close * 100, 2)
        lines.append(f"| ATR（14日） | {round(atr, 3)}（约现价 {atr_pct}%） |")
    lines.append("")

    # ═══════════════════════════════════════════════════════════════════════════
    # 模块三：风险评估（tstock-risk_analyzer）
    # ═══════════════════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## 🛡️ 模块三：风险评估（tstock-risk_analyzer）")
    lines.append("")

    overall = rsk.get("overall_risk", "N/A")
    risk_score = rsk.get("risk_score", "N/A")
    factors = rsk.get("factors") or []
    macro_sig = rsk.get("macro_signals") or []
    recs = rsk.get("recommendations") or []

    lines.append(f"**综合风险等级**：{overall}（评分 {risk_score}/100，分越低越安全）")
    lines.append("")

    if factors:
        lines.append("**分项风险拆解**")
        lines.append("")
        lines.append("| 风险类型 | 评分 | 风险程度 |")
        lines.append("|---------|------|---------|")
        for f in factors:
            name = f.get("name", "")
            score_f = f.get("score", 0)
            if score_f < 25:
                level = "🟢 低"
            elif score_f < 50:
                level = "🟡 中低"
            elif score_f < 75:
                level = "🟠 中高"
            else:
                level = "🔴 高"
            lines.append(f"| {name} | {score_f} | {level} |")
        lines.append("")

    if macro_sig:
        lines.append("**宏观/行业信号**：")
        for s in macro_sig:
            lines.append(f"- {s}")
        lines.append("")

    if recs:
        lines.append("**风控建议**：")
        for r in recs:
            lines.append(f"- {r}")
        lines.append("")

    # ═══════════════════════════════════════════════════════════════════════════
    # 模块四：策略建议（tstock-portfolio/strategy_planner）
    # ═══════════════════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## 🎯 模块四：策略建议（tstock-portfolio）")
    lines.append("")

    action = stg.get("action", "N/A")
    confidence = stg.get("confidence", "N/A")
    strat_score = stg.get("score", "N/A")
    pos = stg.get("position_recommendation", "N/A")
    strat_stop = stg.get("stop_ref", "N/A")
    strat_sup = stg.get("support_20d", "N/A")
    strat_res = stg.get("resistance_20d", "N/A")
    strat_reasons = stg.get("reasons") or []

    lines.append(f"**操作动作**：{action}")
    lines.append(f"**置信度**：{confidence}")
    lines.append(f"**策略评分**：{strat_score}/100")
    lines.append("")
    lines.append("**仓位建议**：")
    lines.append(f"- {pos}")
    lines.append("")
    lines.append("**技术位**：")
    lines.append(f"| 类型 | 价格（元） |")
    lines.append(f"|------|---------|")
    lines.append(f"| 支撑位 | {strat_sup} |")
    lines.append(f"| 压力位 | {strat_res} |")
    lines.append(f"| 止损位 | {strat_stop} |")
    lines.append("")

    if strat_reasons:
        lines.append("**策略依据**：")
        for r in strat_reasons:
            lines.append(f"- {r}")
        lines.append("")

    # ═══════════════════════════════════════════════════════════════════════════
    # 尾部
    # ═══════════════════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## 📋 各模块原始输出文件")
    lines.append("")
    outputs = report.get("outputs", {})
    for module, path in outputs.items():
        lines.append(f"- **{module}**：{path}")
    lines.append("")
    lines.append("⚠️ *本报告仅供个人投资研究参考，不构成投资建议。股市有风险，入市需谨慎。*")

    return "\n".join(lines)


def _save_knowledge_report(report: dict) -> str:
    """保存完整明细到 memory/股票分析/{股票名}-{日期}.md"""
    date_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    stock_name = report.get("name") or report.get("code")
    safe_name = str(stock_name).replace("/", "-").replace("\\", "-")
    out_dir = WORKSPACE / "memory/股票分析"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{safe_name}-{date_str}.md"

    md = _build_markdown_report(report)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(md)

    return str(out_file)


def main():
    parser = argparse.ArgumentParser(description="TStock Alpha Orchestrator")
    parser.add_argument("code", help="股票代码，如 300308")
    parser.add_argument("--output", "-o", help="输出 JSON 文件路径")
    parser.add_argument("--pretty", action="store_true", help="输出完整 markdown 报告")
    parser.add_argument("--refresh-data", action="store_true", help="强制刷新数据源缓存（no-cache）")
    args = parser.parse_args()

    result = run_analysis(args.code, refresh_data=args.refresh_data)

    # 自动保存完整明细到知识库目录
    knowledge_path = _save_knowledge_report(result)
    result["knowledge_report_path"] = knowledge_path

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {args.output}")

    if args.pretty:
        md = _build_markdown_report(result)
        print(md)


if __name__ == "__main__":
    main()
