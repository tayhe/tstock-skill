#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from datetime import datetime


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main():
    p = argparse.ArgumentParser(description="Equity Alpha Lab V1 - Selection pipeline (scaffold)")
    p.add_argument("--config", required=True, help="Path to selection config JSON")
    p.add_argument("--output-dir", default="outputs", help="Output directory")
    args = p.parse_args()

    cfg = load_json(Path(args.config))
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "rebalance_date": cfg.get("rebalance_date")
        },
        "model": {
            "factors": cfg.get("factors", []),
            "weights": cfg.get("weights", {})
        },
        "risk_controls": cfg.get("risk_controls", {}),
        "top_n": [],
        "notes": [
            "V1 scaffold only: plug in factor values, risk constraints, tradability filters"
        ]
    }

    out_file = out_dir / "selection_report.json"
    out_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {out_file}")


if __name__ == "__main__":
    main()
