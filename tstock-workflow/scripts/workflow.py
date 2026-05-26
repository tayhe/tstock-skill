#!/usr/bin/env python3
"""
China Stock Alpha Orchestrator
==============================
统一编排各独立 skill，生成整合分析结果。

流程：
1) 统一数据源快照 (tstock-data-source)
2) 基本面分析 (tstock-fundamental_analyzer)
3) 技术面分析 (tstock-technical_analyzer)
4) 风险评估 (tstock-risk_analyzer)
5) 策略规划 (tstock-portfolio/strategy_planner)
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tstock_lib.logging_config import setup_logging
from tstock_lib.paths import (
    PROJECT_ROOT,
    DATA_SOURCE_SCRIPT,
    FUNDAMENTAL_SCRIPT,
    TECHNICAL_SCRIPT,
    RISK_SCRIPT,
    STRATEGY_SCRIPT,
)
from report import build_markdown_report

PATHS = {
    "data_source": DATA_SOURCE_SCRIPT,
    "fundamental": FUNDAMENTAL_SCRIPT,
    "technical": TECHNICAL_SCRIPT,
    "risk": RISK_SCRIPT,
    "strategy": STRATEGY_SCRIPT,
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


def run_analysis(code: str, refresh_data: bool = False) -> dict:
    """执行完整分析 pipeline，返回报告 dict。"""
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
        sys.executable, str(PATHS["data_source"]),
        "--code", code,
        "--data-type", "all",
        "--output", str(snapshot)
    ]
    if refresh_data:
        data_cmd.append("--no-cache")
    _run(data_cmd)

    # 2) 基本面
    _run([
        sys.executable, str(PATHS["fundamental"]),
        "--snapshot", str(snapshot),
        "--output", str(fundamental)
    ])

    # 3) 技术面
    _run([
        sys.executable, str(PATHS["technical"]),
        "--snapshot", str(snapshot),
        "--output", str(technical)
    ])

    # 4) 风险
    _run([
        sys.executable, str(PATHS["risk"]),
        "--snapshot", str(snapshot),
        "--fundamental-json", str(fundamental),
        "--output", str(risk)
    ])

    # 5) 策略
    _run([
        sys.executable, str(PATHS["strategy"]),
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

    return {
        "code": code,
        "name": data.get("basic", {}).get("name", ""),
        "analysis_time": datetime.now().isoformat(),
        "pipeline": [
            "data_source",
            "fundamental_analyzer",
            "technical_analyzer",
            "tstock-risk_analyzer",
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


def _save_knowledge_report(report: dict) -> str:
    """保存完整明细到 memory/股票分析/{股票名}-{日期}.md"""
    date_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    stock_name = report.get("name") or report.get("code")
    safe_name = str(stock_name).replace("/", "-").replace("\\", "-")
    out_dir = PROJECT_ROOT / "memory/股票分析"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{safe_name}-{date_str}.md"

    md = build_markdown_report(report)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(md)

    return str(out_file)


def main():
    parser = argparse.ArgumentParser(description="TStock Alpha Orchestrator")
    parser.add_argument("code", help="股票代码，如 300308")
    parser.add_argument("--output", "-o", help="输出 JSON 文件路径")
    parser.add_argument("--pretty", action="store_true", help="输出完整 markdown 报告")
    parser.add_argument("--refresh-data", action="store_true", help="强制刷新数据源缓存（no-cache）")
    parser.add_argument("--verbose", action="store_true", help="显示详细日志")
    parser.add_argument("--debug", action="store_true", help="显示调试日志")
    args = parser.parse_args()
    setup_logging("DEBUG" if args.debug else ("INFO" if args.verbose else "WARNING"))

    result = run_analysis(args.code, refresh_data=args.refresh_data)

    knowledge_path = _save_knowledge_report(result)
    result["knowledge_report_path"] = knowledge_path

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {args.output}")

    if args.pretty:
        md = build_markdown_report(result)
        print(md)


if __name__ == "__main__":
    main()
