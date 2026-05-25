---
name: equity-alpha-lab
description: research and validate cross-sectional equity alpha factors for A-share daily data, including factor construction, point-in-time alignment, preprocessing (winsorize/standardize/neutralize), IC/RankIC and group tests, turnover/cost-aware backtests, multi-factor scoring, and Top-N stock selection. Use when the user wants to build/compare/validate factors, diagnose factor decay, or generate explainable stock candidate lists for quantitative investing.
---

# Equity Alpha Lab (V1)

Run this skill in two modes:
1. `research`: evaluate one factor or a factor family.
2. `selection`: build a multi-factor score and output Top-N stocks.

Default scope for V1:
- Market: China A-share (`cn_a`)
- Frequency: daily cross-section
- Rebalance: weekly or monthly
- Execution assumption: signal at T close, trade at T+1 open

## Required workflow

Follow this order unless user explicitly narrows scope:
1. Define universe / date range / rebalance / holding periods.
2. Check data availability and map all fields to point-in-time observable dates.
3. Build raw factor values.
4. Preprocess factor values (outlier handling, scaling, neutralization).
5. Evaluate predictive power (IC, RankIC, monotonic groups, decay).
6. Run cost-aware portfolio backtests (raw + cost-adjusted).
7. Reject unstable or redundant factors.
8. Combine surviving factors (if selection mode).
9. Output structured report + stock list with score decomposition.

## Hard rules (must enforce)

- Never use future information.
- Treat financial statement fields with publication lag.
- Always report both raw returns and cost-adjusted returns.
- Always include recent 24M stability diagnostics.
- Never accept a factor based on one metric only.
- Explain stock picks with factor exposure decomposition.
- If coverage is too low, downgrade confidence or refuse strong conclusion.
- If industry exposure dominates returns, flag pseudo-alpha risk.

## V1 default thresholds

Use these as defaults unless user overrides:
- `min_rank_ic_mean`: 0.02
- `min_ic_ir`: 0.30
- `min_coverage`: 0.80
- `max_turnover`: 0.60
- `max_factor_corr`: 0.70
- `recent_24m_required`: true

## Data and trading assumptions

- Exclude untradable names on rebalance date: ST, suspended, limit-up/down unable to trade, newly listed (<60 trading days), illiquid names below configured ADV threshold.
- Use adjusted prices with consistent convention.
- Use historical index constituents point-in-time when universe is index based.

## Resource loading guide

- Read `references/research_protocol.md` for strict process and PIT discipline.
- Read `references/metrics_definition.md` for metric formulas and pass/fail logic.
- Read `references/output_schema.md` for required JSON outputs.
- Use scripts under `scripts/` as deterministic pipeline entrypoints.

## Script entrypoints

- `python scripts/run_research.py --config examples/research_config.json`
- `python scripts/run_selection.py --config examples/selection_config.json`

Both scripts produce JSON artifacts under `./outputs/`:
- `factor_report.json`
- `selection_report.json`

## Output contract

Always produce 4 sections:
1. Factor Definition Card
2. Single-Factor Evaluation
3. Multi-Factor Combination (if applicable)
4. Stock Candidates with score decomposition and risk notes
