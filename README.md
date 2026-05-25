# A股智能投资分析系统

基于 OpenClaw Skill 架构的 A股股票分析工具链，提供从数据获取 → 基本面 → 技术面 → 风险评估 → 组合管理 的完整工作流。

---

## 🗂️ 技能一览

| 技能 | 说明 |
|------|------|
| **tstock-workflow** | 编排器入口，统一调度全流程 |
| **tstock-data-source** | 统一数据源（AkShare + 东方财富 + Baostock） |
| **tstock-fundamental_analyzer** | 基本面分析（财务质量、成长性、估值） |
| **tstock-technical_analyzer** | 技术面分析（MA、MACD、RSI、布林带、ATR） |
| **tstock-risk_analyzer** | 风险评估（估值/财务/波动/流动性/行业风险） |
| **tstock-portfolio** | 组合管理（自选池、仓位建议、调仓清单） |

---

## 🚀 快速开始

### 依赖安装

```bash
uv sync                    # 安装核心依赖
uv sync --extra baostock   # 可选：安装 baostock 财务备份数据源
```

运行脚本时使用 `uv run` 自动激活虚拟环境：

```bash
uv run python tstock-workflow/scripts/workflow.py 300308
```

### 配置（config.py）

外部技能路径和 API Key 统一在项目根目录 `config.py` 中管理，优先级：**环境变量 > config.py 默认值**。

#### API Keys

| 变量 | 用途 | 获取方式 |
|------|------|----------|
| `EASTMONEY_APIKEY` | 东方财富 PE/PB/PEG 及行业估值（免费版每日 150 次） | [marketing.dfcfs.com](https://marketing.dfcfs.com/) |
| `IWENCAI_BASE_URL` | 同花顺 API 地址 | 默认 `https://openapi.iwencai.com` |
| `IWENCAI_API_KEY` | 同花顺行业数据、研报、经营数据 | [iwencai.com](https://www.iwencai.com/) |

不设置时相关功能自动降级跳过，不影响其他模块运行。东方财富 API 限流时也会自动降级到 AkShare/腾讯。

#### 外部技能路径

| 变量 | 用途 | 默认值 |
|------|------|--------|
| `SEARCH_SKILLS_ROOT` | 搜索系列（minimax-web-search、tavily-search） | `~/.openclaw/skills` |
| `IWENCAI_SKILLS_ROOT` | 同花顺系列（行业数据、研报、经营数据） | `~/Projects/iwencai-skills` |
| `EASTMONEY_SKILLS_ROOT` | 东方财富系列（financial-data、financial-search、select-stock） | `~/Projects/eastmoney-skills` |

各系列可指向不同路径，互不影响。

### 基本用法

```bash
# 完整分析流程（由 tstock-workflow 编排）
uv run python tstock-workflow/scripts/workflow.py 300308

# 单独使用各技能
uv run python tstock-data-source/scripts/data_source.py --code 600118 --data-type all --output /tmp/600118.json
uv run python tstock-fundamental_analyzer/scripts/fundamental_analyzer.py --code 300308 --output /tmp/fundamental.json
uv run python tstock-technical_analyzer/scripts/technical_analyzer.py --code 300308 --output /tmp/tech.json
uv run python tstock-risk_analyzer/scripts/risk_evaluator.py --code 300308 --output /tmp/risk.json

# 自选池管理
uv run python tstock-portfolio/scripts/watchlist_manager.py add --code 300308 --name 中际旭创 --group AI算力
uv run python tstock-portfolio/scripts/watchlist_manager.py list
```

---

## 📂 目录结构

```
tstock-skills/
├── README.md
├── LICENSE
├── .gitignore
├── config.py                 ← 外部技能路径配置
│
├── tstock-workflow/          ← 编排器（统一入口）
│   └── SKILL.md
│
├── tstock-data-source/       ← 数据源
│   └── SKILL.md
│
├── tstock-fundamental_analyzer/  ← 基本面分析
│   └── SKILL.md
│
├── tstock-technical_analyzer/    ← 技术面分析
│   └── SKILL.md
│
├── tstock-risk_analyzer/     ← 风险评估
│   └── SKILL.md
│
└── tstock-portfolio/         ← 组合管理
    └── SKILL.md
```

---

## 📊 工作流说明

```
股票代码
   ↓
[数据收集] tstock-data-source
   ↓ (快照 JSON)
[基本面] tstock-fundamental_analyzer  ←→  [技术面] tstock-technical_analyzer
   ↓                               ↓
         [风险评估] tstock-risk_analyzer
                    ↓
         [组合策略] tstock-portfolio
                    ↓
              分析报告
```

每一步都使用统一数据源，确保口径一致，避免数据漂移。

## 🔍 搜索策略

`tstock-fundamental_analyzer` 的定性分析（宏观政策、行业格局、竞争壁垒、增长点）采用**级联搜索**：

| 优先级 | 工具 | 适用场景 | 优点 |
|--------|------|---------|------|
| **首选** | `minimax-web-search` | 通用中文搜索 | 中文支持好，无需 API Key，直接返回中文内容 |
| **备选** | `tavily-search` | 英文研报/国际信息 | 通用 AI 搜索，提供英文研报中文化 |

> `eastmoney-financial-search` 定向查询公告/研报/政策，不纳入级联搜索。有精准金融查询需求时**单独调用**。

---

## ⚠️ 免责声明

本工具仅供个人投资研究使用，不构成任何投资建议。股票投资有风险，入市需谨慎。数据分析结果可能与实际情况存在偏差，请自行判断。

---

## 📄 License

MIT License - 详见 LICENSE 文件
