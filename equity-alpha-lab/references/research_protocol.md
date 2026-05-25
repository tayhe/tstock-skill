# Research Protocol (V1)

## 1) Request normalization

Normalize natural language into structured config:
- market, universe, dates, frequency, rebalance, hold periods
- factor family or explicit formula
- neutralization controls
- transaction cost model
- selection settings

## 2) Point-in-time discipline

- Build factor at date `t` using only information available at `t` close.
- For financial statements, align by publication date and add lag policy.
- Calculate forward returns from `t+1` onward.

## 3) Preprocessing

At each cross-section date:
1. winsorize (MAD or quantile)
2. standardize (z-score)
3. neutralize by industry + log market cap (OLS residual)

Keep both raw and processed factor versions.

## 4) Single-factor evaluation

Required:
- RankIC mean, RankIC IR, positive IC ratio
- Holding period decay (1/5/10/20 days)
- Quantile group monotonicity and spread
- Long-short stats (ann return, Sharpe, max drawdown)
- Turnover and cost-adjusted performance
- Rolling 12M IC and recent 24M summary

## 5) Multi-factor combination

- Remove highly correlated factors (>|0.7| by default)
- Weighting: equal / IC-IR / stability-penalized
- Evaluate in-sample and out-of-sample windows

## 6) Selection output

- Apply tradability filters
- Enforce concentration controls (industry and single-name cap)
- Output Top-N with score decomposition and risk flags

## 7) Confidence labels

- High: pass all hard thresholds + stable recent 24M
- Medium: pass core thresholds with minor warnings
- Low: fails one or more hard thresholds or unstable recent regime
