# Schema（v1）

## 顶层字段

- `schema_version`: 固定 `china_stock_data_v1`
- `snapshot_id`: `<code>-<YYYYMMDD>-<HHMMSS>`
- `as_of`: ISO 时间
- `code`: 6位股票代码
- `data_type`: `core|financial|all`
- `quality`: 数据质量信息
- `basic`: 股票基础信息
- `market`: 行情与近60日统计
- `valuation`: 估值字段（PE/PB/估值分位）
- `financial`: 财报三表简表 + 财务指标
- `baostock`: Baostock 补充数据（可选）

## quality

- `completeness`: 0~1
- `errors`: 错误列表
- `sources_used`: 实际用到的数据源（如 `akshare`,`baostock`）

## 设计原则

1. 所有数值字段尽可能转为数值型（无法转换为 `null`）
2. 同一字段名在所有策略中保持一致
3. 每次拉取保留 `snapshot_id`，便于回测复现
4. 数据源失败时降级，但必须记录在 `quality.errors`
