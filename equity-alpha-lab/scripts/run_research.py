#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from datetime import datetime


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main():
    p = argparse.ArgumentParser(description="Equity Alpha Lab V1 - Research pipeline (scaffold)")
    p.add_argument("--config", required=True, help="Path to research config JSON")
    p.add_argument("--output-dir", default="outputs", help="Output directory")
    args = p.parse_args()

    cfg = load_json(Path(args.config))
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # V1 scaffold output (replace with real pipeline integration)
    result = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "market": cfg.get("market", "cn_a"),
            "universe": cfg.get("universe", "csi800"),
            "start_date": cfg.get("start_date"),
            "end_date": cfg.get("end_date"),
            "rebalance": cfg.get("rebalance", "weekly")
        },
        "factor_definition": {
            "name": cfg.get("factor_name", "demo_factor"),
            "economic_rationale": cfg.get("economic_rationale", "TBD"),
            "formula": cfg.get("formula", "TBD"),
            "direction": cfg.get("direction", "positive"),
            "pit_risk": "medium"
        },
        "single_factor": {
            "coverage": None,
            "rank_ic_mean": None,
            "ic_ir": None,
            "ic_positive_ratio": None,
            "decay": {},
            "portfolio": {
                "long_short_ann_return": None,
                "long_short_sharpe": None,
                "max_drawdown": None,
                "turnover": None,
                "cost_adjusted_return": None
            },
            "stability": {
                "rolling_12m_rank_ic": [],
                "recent_24m_status": "pending"
            }
        },
        "decision": {
            "pass": False,
            "confidence": "low",
            "reasons": ["V1 scaffold only: connect data + evaluation modules"]
        }
    }

    out_file = out_dir / "factor_report.json"
    out_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {out_file}")


if __name__ == "__main__":
    main()
