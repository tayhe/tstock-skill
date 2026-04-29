---
name: tstock-technical_analyzer
description: A股技术面分析 Skill，基于日线数据计算趋势/动量/波动指标（MA、MACD、RSI、布林带、ATR）。用于：(1) 判断趋势方向，(2) 给出短中期技术信号，(3) 提供技术位（支撑/压力/止损参考），(4) 为交易与风控提供统一技术口径。
---

# Technical Analyzer

```bash
python scripts/technical_analyzer.py --code 300308 --output /tmp/300308_tech.json
```

输出：
- 趋势状态（多头/震荡/空头）
- 指标信号（MACD、RSI、布林）
- 支撑位/压力位
- 建议止损区间（ATR）
