# Output Schema (V1)

## factor_report.json

```json
{
  "meta": {
    "market": "cn_a",
    "universe": "csi800",
    "start_date": "2019-01-01",
    "end_date": "2025-12-31",
    "rebalance": "weekly"
  },
  "factor_definition": {
    "name": "quality_roe_stability",
    "economic_rationale": "...",
    "formula": "...",
    "direction": "positive",
    "pit_risk": "low|medium|high"
  },
  "single_factor": {
    "coverage": 0.92,
    "rank_ic_mean": 0.028,
    "ic_ir": 0.45,
    "ic_positive_ratio": 0.57,
    "decay": {"1": 0.028, "5": 0.019, "10": 0.014, "20": 0.01},
    "portfolio": {
      "long_short_ann_return": 0.14,
      "long_short_sharpe": 1.02,
      "max_drawdown": -0.11,
      "turnover": 0.42,
      "cost_adjusted_return": 0.10
    },
    "stability": {
      "rolling_12m_rank_ic": [],
      "recent_24m_status": "pass|warn|fail"
    }
  },
  "decision": {
    "pass": true,
    "confidence": "high|medium|low",
    "reasons": ["..."]
  }
}
```

## selection_report.json

```json
{
  "meta": {"rebalance_date": "2026-03-13"},
  "model": {
    "factors": ["value", "quality", "momentum"],
    "weights": {"value": 0.3, "quality": 0.4, "momentum": 0.3}
  },
  "risk_controls": {
    "industry_cap": 0.3,
    "single_name_cap": 0.05
  },
  "top_n": [
    {
      "symbol": "600xxx.SH",
      "score": 1.82,
      "factor_scores": {"value": 0.7, "quality": 0.8, "momentum": 0.32},
      "risk_flags": ["none"]
    }
  ]
}
```
