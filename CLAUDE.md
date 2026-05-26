# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A-share (China stock market) analysis toolkit built as a collection of independent OpenClaw skills. Each skill is a standalone Python CLI that communicates via JSON files. The orchestrator (`tstock-workflow`) chains them into a full analysis pipeline.

## Commands

```bash
# Full analysis pipeline (orchestrates all skills)
uv run python tstock-workflow/scripts/workflow.py 300308 --pretty
uv run python tstock-workflow/scripts/workflow.py 300308 --output /tmp/report.json --refresh-data

# Individual skills
uv run python tstock-data-source/scripts/data_source.py --code 600118 --data-type all --output /tmp/600118.json
uv run python tstock-fundamental_analyzer/scripts/fundamental_analyzer.py --code 300308 --output /tmp/fund.json
uv run python tstock-technical_analyzer/scripts/technical_analyzer.py --code 300308 --output /tmp/tech.json
uv run python tstock-risk_analyzer/scripts/risk_evaluator.py --code 300308 --output /tmp/risk.json
uv run python tstock-portfolio/scripts/strategy_planner.py --code 300308 --fundamental /tmp/fund.json --technical /tmp/tech.json --risk /tmp/risk.json

# Watchlist management
uv run python tstock-portfolio/scripts/watchlist_manager.py add --code 300308 --name 中际旭创 --group AI算力
uv run python tstock-portfolio/scripts/watchlist_manager.py list

# Batch & index constituents
uv run python tstock-data-source/scripts/data_source.py --codes 600118,002050 --data-type core --batch-output /tmp/batch.json
uv run python tstock-data-source/scripts/data_source.py --scope hs300
```

## Architecture

```
workflow.py (orchestrator)
  │
  ├─► data_source.py ──────► tstock_data_source/    # 实际逻辑在包中
  │     ├── providers/akshare.py                     # AkShare 数据源
  │     ├── providers/baostock.py                    # Baostock 数据源
  │     ├── providers/dfcf.py                        # 东方财富 API
  │     ├── providers/iwencai.py                     # 同花顺 skills
  │     ├── providers/tencent.py                     # 腾讯 PE/PB 兜底
  │     ├── valuation.py                             # 稳定估值口径
  │     ├── transform.py                             # 数据标准化层
  │     ├── snapshot.py                              # 快照编排器
  │     ├── batch.py                                 # 批量与指数成分
  │     └── cache.py                                 # 日级文件缓存
  │
  ├─► fundamental_analyzer.py # Reads snapshot → profitability, health, valuation, qualitative (web search)
  ├─► technical_analyzer.py   # Reads snapshot → MA/MACD/RSI/BOLL/KDJ/ATR indicators
  ├─► risk_evaluator.py       # Reads snapshot + fundamental JSON → risk score (0-100)
  └─► strategy_planner.py     # Reads fundamental + technical + risk → action/position/stop

tstock_lib/ (shared library)
  ├── utils.py     # safe_float, normalize_code, with_exchange_prefix, to_bs_code
  ├── paths.py     # PROJECT_ROOT, all script path constants
  ├── constants.py # 魔法数字（评分阈值、HTTP 超时、风险门槛等）
  ├── snapshot.py  # load_snapshot(code, snapshot_path, data_type)
  └── logging_config.py # setup_logging(level)
```

**Data flow**: Each downstream skill accepts either `--code` (fetched fresh) or `--snapshot` (reuses existing JSON). The orchestrator always passes snapshot paths via subprocess to avoid re-fetching.

**Path resolution**: Every script inserts the project root into `sys.path` so `tstock_lib` and `config` are importable. `data_source.py` additionally adds `scripts/` to `sys.path` for the `tstock_data_source` package. Skills live at `{project_root}/tstock-{name}/scripts/`.

**External skill dependencies**: `fundamental_analyzer.py` calls `minimax-web-search` and `tavily-search` (for qualitative web research). `tstock_data_source/providers/iwencai.py` optionally calls 同花顺 skills. Each series has its own root variable in `config.py`, overridable via `SEARCH_SKILLS_ROOT`, `IWENCAI_SKILLS_ROOT`, and `EASTMONEY_SKILLS_ROOT` env vars.

**Data source priority**: 东方财富 (PE/PB/PEG/industry, needs `EASTMONEY_APIKEY`, free tier 150 calls/day) → AkShare (spot行情+行业均值) → Baostock (财务备份) → 腾讯 (PE/PB兜底). 同花顺为可选增强（行业分类、研报、经营数据）。东方财富限流时自动降级到同花顺/AkShare/腾讯。同花顺行业估值通过 iwencai skills 获取，需 `IWENCAI_API_KEY`。

**Qualitative search cascade** (fundamental_analyzer): `minimax-web-search` (preferred, good Chinese support) → `tavily-search` (fallback). `eastmoney-financial-search` is called separately for precise financial queries.

**Logging**: All scripts support `--verbose` (INFO) and `--debug` (DEBUG) flags. Uses standard `logging.getLogger(__name__)`. Default level is WARNING (silent).

## Key Conventions

- Shared utilities live in `tstock_lib/` package — never duplicate `safe_float`, path resolution, or constants
- Data source logic lives in `tstock_data_source/` package — `data_source.py` is a thin shim
- All inter-skill data passes through JSON files, never direct function calls
- All numeric fields use `safe_float()` helper — returns `None` for `"--"`, `"nan"`, empty strings
- Stock codes are normalized to 6-digit (no exchange prefix) internally; `with_exchange_prefix()` / `to_bs_code()` convert as needed
- Valuation data should be read from `valuation_stable` in snapshots (unified口径), not raw `valuation`
- The transform layer (`transform_snapshot`) normalizes data from different sources so downstream skills are decoupled from source specifics

## Dependencies

```bash
uv sync                    # install core dependencies
uv sync --extra baostock   # include optional baostock
uv run python ...          # run scripts within the venv
```

## Configuration (config.py)

All external paths and API keys are centralized in `config.py`. Priority: env var > config.py default.

**API Keys:**
- `EASTMONEY_APIKEY` — 东方财富 API key (enables PE/PB/PEG + industry valuation, free tier: 150 calls/day)
- `IWENCAI_API_KEY` — 同花顺 API key (enables industry data, reports, business data)
- `IWENCAI_BASE_URL` — 同花顺 API 地址 (defaults to `https://openapi.iwencai.com`)
- `MINIMAX_API_KEY` — minimax-web-search API key (enables qualitative web research)

**External skill paths:**
- `SEARCH_SKILLS_ROOT` — minimax-web-search, tavily-search (defaults to `~/.openclaw/skills`)
- `IWENCAI_SKILLS_ROOT` — 同花顺系列 (defaults to `~/Projects/iwencai-skills`)
- `EASTMONEY_SKILLS_ROOT` — 东方财富系列 (defaults to `~/Projects/eastmoney-skills`)

**Output paths:**
- `TSTOCK_REPORT_DIR` — 分析报告输出目录 (defaults to `{project_root}/memory/股票分析`)
- `OPENCLAW_WATCHLIST_DB` — watchlist JSON path (defaults to `{project_root}/memory/watchlist.json`)
