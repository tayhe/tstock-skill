# TStock Alpha（Orchestrator）

该 Skill 已升级为**兼容入口编排器**，不再承载具体分析逻辑。

## 新定位

`tstock-workflow` 现在只负责流程编排，调用下列独立 Skill：

1. `tstock-data-source`：统一数据源快照（唯一口径）
2. `tstock-fundamental_analyzer`：基本面分析
3. `tstock-technical_analyzer`：技术面分析
4. `tstock-risk_analyzer`：风险评估
5. `tstock-portfolio`（strategy_planner）：策略建议与仓位

## 使用方式

```bash
python scripts/workflow.py <股票代码> [--output result.json] [--pretty]
```

示例：

```bash
python scripts/workflow.py 300308 --pretty
python scripts/workflow.py 300308 --output /tmp/300308_full_report.json
```

## 输出内容

- 流程执行信息（pipeline）
- 统一数据源质量信息（snapshot_quality）
- 摘要结论（基本面评分、技术趋势、风险等级、策略建议）
- 各模块详细结果（details）
- 中间文件路径（outputs）

## 说明

- 旧版 `tstock-workflow` 中的单体分析逻辑已拆分到独立 skill。
- 建议后续新增能力均在对应独立 skill 中迭代，本 orchestrator 只做组合调用与报告聚合。
