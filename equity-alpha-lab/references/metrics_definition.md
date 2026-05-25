# Metrics Definition (V1)

## Cross-sectional quality
- `coverage`: available factor names / universe names
- `missing_ratio`: 1 - coverage
- `cross_section_std`, `skew`, `kurtosis`

## Predictive power
- `ic_t = corr(f_t, r_{t+1})`
- `rank_ic_t = spearman(f_t, r_{t+1})`
- `ic_mean = mean(ic_t)`
- `rank_ic_mean = mean(rank_ic_t)`
- `ic_ir = mean(rank_ic_t) / std(rank_ic_t)`
- `ic_positive_ratio = mean(rank_ic_t > 0)`

## Decay
Compute IC for horizons h in {1,5,10,20}:
- `rank_ic_mean_h`
- check monotonic decay profile (not strictly required but should be interpretable)

## Portfolio layer
- `long_short_ann_return`
- `long_short_sharpe`
- `max_drawdown`
- `group_monotonicity`
- `turnover`
- `cost_adjusted_return`

## Stability
- rolling 12M RankIC
- yearly decomposition
- recent 24M pass/fail

## Default pass criteria
- `rank_ic_mean >= 0.02`
- `ic_ir >= 0.30`
- `coverage >= 0.80`
- `turnover <= 0.60` (unless strategy is explicitly high-turnover)
- recent 24M not materially degraded
