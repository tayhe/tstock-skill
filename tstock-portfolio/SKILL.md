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
  --risk /tmp/300308_risk.json \
  --output /tmp/300308_strategy.json
```

### strategy_planner 参数

| 参数 | 说明 |
|------|------|
| `--code` | 股票代码（必填） |
| `--fundamental` | 基本面分析 JSON 路径（必填） |
| `--technical` | 技术面分析 JSON 路径（必填） |
| `--risk` | 风险评估 JSON 路径（必填） |
| `--output` | 输出文件路径 |
| `--verbose` | 显示详细日志 |
| `--debug` | 显示调试日志 |
