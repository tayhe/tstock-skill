---
name: tstock-data-source
description: A股统一数据源 Skill，提供标准化、可复现、可缓存的股票数据获取接口（单股/批量/指数成分/全市场），同时整合 AkShare + Baostock + 东方财富 的主备数据链路与质量标记。用于任何需要"唯一数据源"的场景：(1) 股票分析前的数据准备，(2) 量化选股/因子研究的数据拉取，(3) 回测前数据快照固化，(4) 多模块共享同一份原始数据，避免口径不一致。

**2026-03-17 更新**：已集成东方财富（EastMoney）API，支持获取 PE/PB/PEG 等估值指标及行业估值对比数据。
---

# China Stock Data Source

统一数据源目标：**一次拉取，处处复用，口径一致**。

## 1) 先决条件

安装依赖：

```bash
pip install akshare pandas requests
# 可选（推荐，作为财务备份源）
pip install baostock
```

**东方财富数据（可选，需配置API Key）**：
```bash
# 设置环境变量（可选，已内置东方财富API适配）
export EASTMONEY_APIKEY="your_api_key"  # 从 https://marketing.dfcfs.com/ 获取
```

## 2) 核心脚本

- 主脚本：`scripts/data_source.py`
- 输出模式：标准 JSON（含 schema_version、snapshot_id、as_of、quality）

## 3) 常用命令

### 单股全量快照

```bash
python scripts/data_source.py --code 600118 --data-type all --years 3 --output /tmp/600118.json
```

### 多股批量（适合量化预处理）

```bash
python scripts/data_source.py --codes 600118,002050,300308 --data-type core --batch-output /tmp/batch.json
```

### 指数成分股列表

```bash
python scripts/data_source.py --scope hs300
```

支持范围：`hs300 | zz500 | zz1000 | cyb | kcb | all`

## 4) 数据类型（--data-type）

- `core`: 基础信息 + 行情 + 估值（推荐默认）
- `financial`: 财报/财务指标（含 Baostock 补充）
- `all`: 全量

## 5) 数据源说明

| 数据类型 | 主源 | 备源 | 说明 |
|---------|------|------|------|
| 基础信息 | AkShare | Baostock | 代码、名称、行业、总市值等 |
| 实时行情 | AkShare | 腾讯财经 | 实时涨跌幅、成交量等 |
| 估值指标 | **东方财富** | AkShare | **PE/PB/PEG（含行业对比）** |
| 财务数据 | AkShare | Baostock | 财报、资产负债表等 |
| 行业估值 | **东方财富** | - | **行业PE/PB中位数** |

### 东方财富数据（2026-03-17 新增）

脚本已内置东方财富 API 适配层，支持：

- **个股估值**：PE(TTM)、PB、PEG、净利润同比增速
- **行业估值**：行业PE/PB中位数、行业PE/PB整体法
- **技术位**：支撑位、压力位

查询示例：
```python
# 估值数据（已内置于 core 类型）
# 输出字段：pe_ttm, pb, peg, growth_yoy_pct, industry_avg{pe,pb,pr}, premium_pct{pe,pb,pr}
```

## 6) 缓存与复现

- 默认开启日级缓存（`scripts/.cache/`）
- 可以 `--no-cache` 强制刷新
- `snapshot_id` 用于回测和报告追溯

## 7) 输出字段说明

### valuation_stable（推荐使用）

统一估值口径，包含：

| 字段 | 类型 | 说明 |
|------|------|------|
| `pe_ttm` | float | 市盈率（TTM） |
| `pb` | float | 市净率 |
| `pr` | float | 市销率 |
| `peg` | float | PEG = PE / 净利润增速 |
| `growth_yoy_pct` | float | 归母净利润同比增速(%) |
| `industry_avg.pe` | float | 行业PE中位数 |
| `industry_avg.pb` | float | 行业PB中位数 |
| `premium_pct.pe` | float | PE相对行业溢价(%) |
| `premium_pct.pb` | float | PB相对行业溢价(%) |
| `assessment.pe` | string | PE估值判断（偏高估/偏低估/估值合理） |
| `assessment.pb` | string | PB估值判断 |
| `assessment.peg` | string | PEG判断 |

### meta 信息

```json
{
  "valuation_basis": "PE(TTM)/PB(当前)/PR(TTM)/PEG=PE÷净利润同比增速%",
  "as_of": "2026-03-17",
  "source_used": ["akshare.spot", "dfcf.skill", "tencent.qt"]
}
```

## 8) 与 tstock-workflow 的衔接建议

在分析工作流中，将"数据收集步骤"替换为：

```python
from data_source import fetch_stock_snapshot
raw = fetch_stock_snapshot("600118", data_type="all", years=3)
```

并将 `raw` 作为后续财务分析/估值/风控的唯一输入。

## 9) 数据口径说明

详细字段见：`references/schema.md`
