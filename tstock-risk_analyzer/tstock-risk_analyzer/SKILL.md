---
name: tstock-risk_analyzer
description: A股风险评估 Skill，输出统一风险评分与分项风险（估值、财务、波动、流动性、行业）。用于：(1) 单标的风险体检，(2) 交易前风控闸门，(3) 组合风险分层，(4) 与 strategy_planner 协同生成仓位和止损建议。
---

# Risk Evaluator

```bash
python scripts/risk_evaluator.py --code 300308 --output /tmp/300308_risk.json
```

输出：
- overall_risk（低/中/高）
- risk_score（0-100，越高风险越高）
- factors（分项评分）
- recommendations（风险管理建议）
