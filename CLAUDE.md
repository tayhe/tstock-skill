# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A-share (China stock market) analysis toolkit built as a collection of independent OpenClaw skills. Each skill is a standalone Python CLI that communicates via JSON files. The orchestrator (`tstock-workflow`) chains them into a full analysis pipeline.

## Commands

```bash
# Full analysis pipeline (orchestrates all skills)
python tstock-workflow/scripts/workflow.py 300308 --pretty
python tstock-workflow/scripts/workflow.py 300308 --output /tmp/report.json --refresh-data

# Individual skills
python tstock-data-source/scripts/data_source.py --code 600118 --data-type all --output /tmp/600118.json
python tstock-fundamental_analyzer/scripts/fundamental_analyzer.py --code 300308 --output /tmp/fund.json
python tstock-technical_analyzer/scripts/technical_analyzer.py --code 300308 --output /tmp/tech.json
python tstock-risk_analyzer/scripts/risk_evaluator.py --code 300308 --output /tmp/risk.json
python tstock-portfolio/scripts/strategy_planner.py --code 300308 --fundamental /tmp/fund.json --technical /tmp/tech.json --risk /tmp/risk.json

# Watchlist management
python tstock-portfolio/scripts/watchlist_manager.py add --code 300308 --name 中际旭创 --group AI算力
python tstock-portfolio/scripts/watchlist_manager.py list

# Batch & index constituents
python tstock-data-source/scripts/data_source.py --codes 600118,002050 --data-type core --batch-output /tmp/batch.json
python tstock-data-source/scripts/data_source.py --scope hs300
```

## Architecture

```
workflow.py (orchestrator)
  │
  ├─► data_source.py          # Single source of truth for all stock data
  │     Outputs: snapshot JSON (schema_version, snapshot_id, quality, basic, market, valuation, financial)
  │     Cache: scripts/.cache/{code}_{type}_{date}.json (day-level, --no-cache to bypass)
  │
  ├─► fundamental_analyzer.py # Reads snapshot → profitability, health, valuation, qualitative (web search)
  ├─► technical_analyzer.py   # Reads snapshot → MA/MACD/RSI/BOLL/KDJ/ATR indicators
  ├─► risk_evaluator.py       # Reads snapshot + fundamental JSON → risk score (0-100)
  └─► strategy_planner.py     # Reads fundamental + technical + risk → action/position/stop
```

**Data flow**: Each downstream skill accepts either `--code` (fetched fresh) or `--snapshot` (reuses existing JSON). The orchestrator always passes snapshot paths via subprocess to avoid re-fetching.

**Path resolution**: Every script resolves the project root as `Path(__file__).resolve().parent.parent.parent`. Skills live at `{project_root}/tstock-{name}/scripts/`.

**External skill dependencies**: `fundamental_analyzer.py` calls `minimax-web-search` and `tavily-search` (for qualitative web research). `data_source.py` optionally calls 同花顺 (iwencai) skills. 东方财富 skills are available but currently unused (data comes via direct HTTP API). Each series has its own root variable in `config.py`, overridable via `SEARCH_SKILLS_ROOT`, `IWENCAI_SKILLS_ROOT`, and `EASTMONEY_SKILLS_ROOT` env vars.

**Data source priority**: AkShare (primary) → Baostock (backup) → 东方财富 (PE/PB/PEG, needs `EASTMONEY_APIKEY`) → 腾讯 (PE/PB fallback) → 同花顺 (optional enrichment via external skills).

**Qualitative search cascade** (fundamental_analyzer): `minimax-web-search` (preferred, good Chinese support) → `tavily-search` (fallback). `eastmoney-financial-search` is called separately for precise financial queries.

## Key Conventions

- All scripts are standalone CLI with `argparse`; no shared library imports between skills
- All inter-skill data passes through JSON files, never direct function calls
- All numeric fields use `safe_float()` helper — returns `None` for `"--"`, `"nan"`, empty strings
- Stock codes are normalized to 6-digit (no exchange prefix) internally; `with_exchange_prefix()` / `to_bs_code()` convert as needed
- Valuation data should be read from `valuation_stable` in snapshots (unified口径), not raw `valuation`
- The transform layer (`_transform_snapshot`) in data_source normalizes data from different sources so downstream skills are decoupled from source specifics

## Dependencies

```bash
pip install akshare pandas requests
pip install baostock  # optional, backup financial data source
```

## Configuration (config.py)

All external paths and API keys are centralized in `config.py`. Priority: env var > config.py default.

**API Keys:**
- `EASTMONEY_APIKEY` — 东方财富 API key (enables PE/PB/PEG + industry valuation)
- `IWENCAI_API_KEY` — 同花顺 API key (enables industry data, reports, business data)

**External skill paths:**
- `SEARCH_SKILLS_ROOT` — minimax-web-search, tavily-search (defaults to `~/.openclaw/skills`)
- `IWENCAI_SKILLS_ROOT` — 同花顺系列 (defaults to `~/.openclaw/workspace-fiona/skills`)
- `EASTMONEY_SKILLS_ROOT` — 东方财富系列 (defaults to `~/.openclaw/workspace-fiona/skills`)

**Other:**
- `OPENCLAW_WATCHLIST_DB` — watchlist JSON path (defaults to `{project_root}/memory/watchlist.json`)
