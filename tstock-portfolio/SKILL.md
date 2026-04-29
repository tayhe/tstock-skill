---
name: tstock-portfolio
description: 投资组合管理 Skill，包含 watchlist_manager 与 strategy_planner 两个基础能力。用于：(1) 自选池维护，(2) 组合仓位建议，(3) 基于基本面/技术面/风险评分生成行动建议，(4) 输出可执行的调仓清单。
---

# Portfolio

## 1) 自选池管理

```bash
python scripts/watchlist_manager.py add --code 300308 --name 中际旭创 --group AI算力
python scripts/watchlist_manager.py list
```

## 2) 策略建议

```bash
python scripts/strategy_planner.py --code 300308 \
  --fundamental /tmp/300308_fundamental.json \
  --technical /tmp/300308_tech.json \
  --risk /tmp/300308_risk.json
```
