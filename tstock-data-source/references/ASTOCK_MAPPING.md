# a-stock-data 吸收与端点映射规范 (ASTOCK_MAPPING.md)

## 一、概述

- **上游项目**：`~/Projects/upstream/a-stock-data`（只读协议，保持纯净上游，支持随时 `git pull` 升级）
- **加载机制**：动态 AST 代码载入器 [`loader.py`](file:///home/tayhe/Projects/mine/tstock-skills/tstock-data-source/scripts/tstock_data_source/providers/astock_adapter/loader.py)
  - 通过 Python 抽象语法树（AST）直接从上游 `SKILL.md` 中提取纯净的函数定义、类定义与配置常量。
  - 剔除上游文档中用于演示的顶层示例调用与测试代码，避免运行期网络阻塞或崩溃。
  - 线程安全单例缓存，加载开销 < 100ms。

---

## 二、端点映射表 (Endpoint Mapping)

| 业务维度 | 上游函数 (`SKILL.md`) | tstock 封装函数 (`astock.py`) | 协议与特性 | 吸收状态 |
| :--- | :--- | :--- | :--- | :--- |
| **实时行情快照** | `tencent_quote(codes)` | `fetch_realtime_quote(codes)` / `get_basic_from_astock` | HTTP GET (`qt.gtimg.cn`)，GBK 编码，~ 分隔 88 字段，不封 IP，毫秒级响应 | **已接入首选源** |
| **代码清洗与市场前缀** | `norm_ticker(code)`, `get_prefix(code)` | `call_astock("norm_ticker", code)` | 纯本地正则，自动识别 000xxx 指数/个股歧义及北交所 920 号段 | **已接入** |
| **筹码分布推演 (CYQ)** | `chip_distribution(df, grid_size, decay)` | `fetch_chip_distribution(df)` | 本地时序三角网格衰减计算，输出获利盘比例、90%/70%成本集中度、筹码峰 | **已接入** |
| **概念板块与所属行业** | `eastmoney_concept_blocks(code)` | `fetch_concept_blocks(code)` | 东财 HTTP 直连，带 `em_get` 节流保护 | **已接入** |
| **股东户数与筹码集中度** | `holder_num_change(code, page_size)` | `fetch_holder_num_change(code)` | 东财数据中心直连，提取环比变化与户均持股 | **已接入** |
| **分红送转历史** | `dividend_history(code, page_size)` | `fetch_dividend_history(code)` | 东财数据中心直连，送转派息方案与股权登记日 | **已接入** |
| **融资融券明细** | `margin_trading(code, page_size)` | `fetch_margin_trading(code)` | 沪深两融明细，融资买入与融券余量 | **已接入** |
| **龙虎榜上榜明细** | `dragon_tiger_board(code, date)` | `fetch_dragon_tiger_board(code)` | 营业部与机构席位净买入明细 | **已接入** |
| **通达信直连 K 线** | `tdx_client()`, `client.bars()` | `fetch_kline(code, count, period)` | TCP 7709 端口直连，内置服务器探测与超时平滑降级保护 | **已接入备选** |
| **新浪前/后复权因子** | `sina_adjust_factor(code, kind)`, `apply_adjust(bars, factors)` | - | HTTP 零鉴权，除权除息价格跳空修正 | *待扩展接入* |
| **打板与连板情绪** | `ths_limit_up_pool(date)`, `limit_up_sentiment(date)` | - | 炸板率、连板梯队、涨跌停统计 | *待扩展接入* |
| **巨潮互动易投资者问答** | `cninfo_irm(code, page_size)` | - | 董秘官方一手答复，定性催化剂提取 | *待扩展接入* |
| **宏观社融与PMI** | `pboc_social_financing()`, `nbs_pmi()` | - | 人民银行与国家统计局官方公开数据 | *待扩展接入* |

---

## 三、关键防坑经验与规则汇总

1. **东财防封节流机制 (`em_get`)**：
   - 东财 HTTP 接口（`datacenter`, `push2`, `search`）有严格风控规则（单 IP 并发 ≥10 或 1秒 >5 次会触发临时封禁）。
   - `a-stock-data` 采用统一的 `em_get()` 入口：串行请求 + `EM_MIN_INTERVAL=1.0s` 间隔 + 随机抖动 + Session Keep-Alive。
   - `tstock` 必须保证所有东财调用复用该配置，严禁并发泛洪请求。

2. **腾讯行情字段索引偏移注意**：
   - 字段索引 `vals[44]` 为流通市值（亿元），`vals[45]` 为总市值（亿元）。
   - 当遇到长期停牌股或北交所老代码时，腾讯会返回 `amount_wan == 0 and price == last_close` 的“僵尸报价”，`tencent_quote` 已内建 `is_stale` 标识。

3. **通达信 TCP 7709 端口不可达时的降级对策**：
   - 在部分云服务器或受限网络下，通达信 TCP 7709 端口可能全部不可达或被 reset。
   - `tstock` 在设计上严格执行**“腾讯 HTTP 直连为第一首选、通达信/AkShare/BaoStock 为梯度备选”**策略，绝不在单一数据源阻塞主分析流。

4. **筹码分布必须时序递增**：
   - `chip_distribution` 采用换手衰减递推算法，入参 DataFrame 必须包含 `date, high, low, close, turn` 列，且必须按日期升序排列；若倒序传入会导致最老的收盘价被当作现价。

---

## 四、上游升级追踪机制

运行根目录审计脚本可自动对比本地与上游的提交状态：
```bash
uv run python scripts/audit_upstream_astock.py
```
- 监控 `a-stock-data` 的版本发布（Release / Tags）。
- 解析其 `CHANGELOG.md` 中带有 `🔴 修复`、`★ 新增` 的端点，作为 `tstock` 持续增强的灵感源泉。
