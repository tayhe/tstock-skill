---
name: tstock-fundamental_analyzer
description: A股基本面分析 Skill，聚焦定量+定性基本面结论输出。用于：(1) 单只股票基本面体检，(2) 财务质量/成长性评估，(3) 相对估值与行业对比，(4) 生成可复用的基本面 JSON 结果供风控与组合策略使用。
---

# Fundamental Analyzer

使用统一数据源 `tstock-data-source` 的快照做输入，避免口径漂移。

## 命令

```bash
python scripts/fundamental_analyzer.py --code 300308 --output /tmp/300308_fundamental.json
```

可选：

```bash
python scripts/fundamental_analyzer.py --snapshot /tmp/300308_snapshot.json
```

## 输出

- 盈利能力（ROE/净利率/毛利率）
- 成长性（营收与净利趋势）
- 财务健康（资产负债率/流动性）
- 估值结论（PE/PB 分位 + 合理/偏高/偏低）
- 综合评分与结论
- 定性信息（宏观政策、行业格局、竞争壁垒、增长点）
- 参考来源链接

## 搜索策略

定性分析（宏观/行业/公司）采用**级联搜索**：

```
minimax-web-search（首选）
  → 中文搜索效果好，无需 API Key
  → 直接返回中文内容
  ↓ 失败或结果不足时
tavily-search（备选）
  → 通用 AI 搜索
  → 提供英文研报中文化内容
```

> **注意**：`eastmoney-financial-search` 定向查询公告/研报/政策，不纳入级联搜索。有精准金融查询需求时单独调用。
