#!/usr/bin/env python3
"""Markdown 报告渲染器：将分析 JSON 转为可读报告。"""


def _fmt_pct(v, decimals=2):
    if v is None:
        return "N/A"
    return f"{round(v * 100, decimals)}%"


def _fmt_float(v, decimals=2):
    if v is None:
        return "N/A"
    return f"{round(v, decimals)}"


def _score_bar(v, max_=10):
    filled = "★" * v + "☆" * (max_ - v)
    return f"[{filled}] {v}/{max_}"


def _render_header(report: dict) -> list[str]:
    lines = []
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
    return lines


def _render_fundamental(fund: dict) -> list[str]:
    lines = []
    qual = fund.get("qualitative", {})
    sc = fund.get("scorecard", {})

    # 1.1 四维打分卡
    lines.append("## 📊 模块一：基本面分析（tstock-fundamental_analyzer）")
    lines.append("")
    lines.append("### 1.1 投资认知总览（四维打分）")
    lines.append("")
    lines.append("| 维度 | 评分 | 说明 |")
    lines.append("|------|------|------|")

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
    lines.append("| 指标 | 数值 | 参考 |")
    lines.append("|------|------|------|")
    lines.append(f"| ROE（净资产收益率） | {_fmt_pct(prof.get('roe'))} | 优秀 >15%，良好 10-15% |")
    lines.append(f"| 净利率 | {_fmt_pct(prof.get('net_margin'))} | 优秀 >15%，良好 8-15% |")
    lines.append(f"| 毛利率 | {_fmt_pct(prof.get('gross_margin'))} | 优秀 >30%，良好 20-30% |")
    lines.append("")

    lines.append("**财务健康**")
    lines.append("| 指标 | 数值 | 参考 |")
    lines.append("|------|------|------|")
    lines.append(f"| 资产负债率 | {_fmt_pct(health.get('debt_ratio'))} | 健康 <50% |")
    lines.append(f"| 流动比率 | {_fmt_float(health.get('current_ratio'))} | 充足 >1.5，优秀 >2.0 |")
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
    lines.append(f"| PEG | {_fmt_float(peg)} | — | — | {ass.get('peg', 'N/A')} |")
    lines.append("")

    # 1.4 定性分析
    lines.append("### 1.4 定性分析")
    lines.append("")

    def _render_qual_section(title, data):
        view = data.get("view", "")
        highlights = data.get("highlights") or []
        refs = data.get("references") or []
        out = [f"**{title}**（{view}）"]
        if highlights:
            for h in highlights:
                out.append(f"- {h.strip()}")
        else:
            out.append("-（暂无有效信息）")
        if refs:
            out.append("")
            out.append("  **参考来源**：")
            for r in refs[:3]:
                out.append(f"  - {r}")
        out.append("")
        return out

    for title, key in [
        ("📌 宏观政策要点", "macro_policy"),
        ("📌 主营与利润分布", "business_profile"),
        ("📌 行业地位与技术壁垒", "industry_competition"),
        ("📌 未来增长点", "growth_map"),
    ]:
        lines.extend(_render_qual_section(title, qual.get(key, {})))

    # 1.5 结论
    lines.append("### 1.5 投资认知评分结论")
    lines.append("")
    reasons = fund.get("reasons") or []
    lines.append(f"**综合观点**：{fund.get('view', '')}（基本面评分：{fund.get('score', 'N/A')}）")
    if reasons:
        lines.append("")
        lines.append("**支撑依据**：")
        for r in reasons:
            lines.append(f"- {r}")
    lines.append("")
    return lines


def _render_technical(tech: dict) -> list[str]:
    lines = []
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
        lines.append(f"**信号列表**：{' '.join(f'`{s}`' for s in signals)}")
    lines.append("")

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
    lines.append("| 类型 | 价格（元） |")
    lines.append("|------|---------|")
    lines.append(f"| 支撑位 | {tech.get('support_20d', 'N/A')} |")
    lines.append(f"| 压力位 | {tech.get('resistance_20d', 'N/A')} |")
    lines.append(f"| 止损参考 | {tech.get('stop_ref', 'N/A')} |")
    atr = tech.get("atr14")
    if atr is not None and close:
        atr_pct = round(atr / close * 100, 2)
        lines.append(f"| ATR（14日） | {round(atr, 3)}（约现价 {atr_pct}%） |")
    lines.append("")

    chips = tech.get("chip_distribution")
    if chips:
        lines.append("**筹码分布（CYQ）**")
        lines.append("")
        lines.append("| 指标 | 数值 | 说明 |")
        lines.append("|------|------|------|")
        pr = chips.get("profit_ratio")
        pr_str = f"{round(pr * 100, 1)}%" if pr is not None else "N/A"
        lines.append(f"| 获利盘比例 | {pr_str} | >85% 预警高位抛压，<10% 评估超跌反弹 |")
        lines.append(f"| 平均持仓成本 | {chips.get('avg_cost', 'N/A')} 元 | 全市场加权持仓均价 |")
        c90 = chips.get("cost_90") or []
        lines.append(f"| 90% 筹码区间 | [{c90[0] if len(c90)>0 else 'N/A'}, {c90[1] if len(c90)>1 else 'N/A'}] | 主力筹码聚集区间 |")
        conc90 = chips.get("concentration_90")
        conc_str = f"{round(conc90, 4)}" if conc90 is not None else "N/A"
        lines.append(f"| 筹码集中度 (90%) | {conc_str} | <0.08 为单峰高度密集 |")
        lines.append(f"| 筹码主峰价格 | {chips.get('peak_price', 'N/A')} 元 | 密集峰值最高价位 |")
        lines.append("")

    return lines


def _render_risk(rsk: dict) -> list[str]:
    lines = []
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
            score_f = f.get("score", 0)
            if score_f < 25:
                level = "🟢 低"
            elif score_f < 50:
                level = "🟡 中低"
            elif score_f < 75:
                level = "🟠 中高"
            else:
                level = "🔴 高"
            lines.append(f"| {f.get('name', '')} | {score_f} | {level} |")
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
    return lines


def _render_strategy(stg: dict) -> list[str]:
    lines = []
    lines.append("---")
    lines.append("")
    lines.append("## 🎯 模块四：策略建议（tstock-portfolio）")
    lines.append("")
    lines.append(f"**操作动作**：{stg.get('action', 'N/A')}")
    lines.append(f"**置信度**：{stg.get('confidence', 'N/A')}")
    lines.append(f"**策略评分**：{stg.get('score', 'N/A')}/100")
    lines.append("")
    lines.append("**仓位建议**：")
    lines.append(f"- {stg.get('position_recommendation', 'N/A')}")
    lines.append("")
    lines.append("**技术位**：")
    lines.append("| 类型 | 价格（元） |")
    lines.append("|------|---------|")
    lines.append(f"| 支撑位 | {stg.get('support_20d', 'N/A')} |")
    lines.append(f"| 压力位 | {stg.get('resistance_20d', 'N/A')} |")
    lines.append(f"| 止损位 | {stg.get('stop_ref', 'N/A')} |")
    lines.append("")

    strat_reasons = stg.get("reasons") or []
    if strat_reasons:
        lines.append("**策略依据**：")
        for r in strat_reasons:
            lines.append(f"- {r}")
        lines.append("")
    return lines


def _render_footer(report: dict) -> list[str]:
    lines = []
    lines.append("---")
    lines.append("")
    lines.append("## 📋 各模块原始输出文件")
    lines.append("")
    for module, path in report.get("outputs", {}).items():
        lines.append(f"- **{module}**：{path}")
    lines.append("")
    lines.append("⚠️ *本报告仅供个人投资研究参考，不构成投资建议。股市有风险，入市需谨慎。*")
    return lines


def build_markdown_report(report: dict) -> str:
    """将分析 JSON 转为完整 markdown 报告。"""
    d = report.get("details", {})
    lines = []
    lines.extend(_render_header(report))
    lines.extend(_render_fundamental(d.get("fundamental", {})))
    lines.extend(_render_technical(d.get("technical", {})))
    lines.extend(_render_risk(d.get("risk", {})))
    lines.extend(_render_strategy(d.get("strategy", {})))
    lines.extend(_render_footer(report))
    return "\n".join(lines)
