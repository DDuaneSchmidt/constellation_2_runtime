# RESULT_REVIEW_004

## Scope

Implementation reference: IMP_004 — Dual Momentum

Claim reference: CLAIM_001 — Trend Persistence

Evidence source: BACKTEST_004 evidence artifacts in `docs/aegis/technical_strategy_factory/evidence/backtest_004/`

This review describes implementation evidence only. It does not certify IMP_004, does not support CLAIM_001 by itself, and does not recommend capital allocation.

## Run Context

| Field | Value |
|---|---|
| Data source | Tiingo cached local CSV |
| Date range | 2000-01-03 through 2026-06-02 |
| Run quality | PRIMARY_ADJUSTED_OHLC |
| Price fields | adjOpen, adjHigh, adjLow, adjClose |
| Cost model | 10 bps round-trip |
| Execution policy | Monthly signal after signal period close, execute next market open |
| Cash return | 0% |

## Primary Metrics

| Series | Total return | CAGR | Volatility | Sharpe | Sortino | Max drawdown | Ulcer index | Time in market | Trade count |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| IMP_004 | 752.19% | 8.45% | 13.50% | 0.670 | 0.703 | -33.70% | 7.22% | 74.20% | 17 |
| SPY buy-and-hold | 732.02% | 8.35% | 19.31% | 0.513 | 0.652 | -55.20% | 16.17% | 100.00% | 1 |
| 50/50 SPY/cash monthly | 212.03% | 4.40% | 9.47% | 0.503 | 0.640 | -32.30% | 8.32% | 50.00% | 318 |

## Comparison Versus Buy-And-Hold

IMP_004 exceeded SPY buy-and-hold on full-period total return, CAGR, Sharpe, Sortino, maximum drawdown, and ulcer index.

IMP_004 did so with lower annual volatility and less time in market than buy-and-hold. The full-period evidence versus buy-and-hold is favorable.

## Comparison Versus 50/50 Baseline

IMP_004 exceeded the 50/50 SPY/cash monthly baseline on full-period total return, CAGR, Sharpe, Sortino, and ulcer index.

IMP_004 did not beat the 50/50 baseline on annual volatility or maximum drawdown. This limits the strength of the result because the 50/50 baseline remains cleaner on raw defensive exposure.

## Comparison Versus Random Timing Baseline

| Metric | Value |
|---|---:|
| Median random CAGR | 1.14% |
| 95th percentile random CAGR | 3.88% |
| Median random Sharpe | 0.152 |
| 95th percentile random Sharpe | 0.312 |
| Median random max drawdown | -62.44% |
| 95th percentile random max drawdown | -44.57% |
| IMP_004 percentile rank, CAGR | 100.00% |
| IMP_004 percentile rank, Sharpe | 100.00% |
| IMP_004 percentile rank, max drawdown | 0.00% |

IMP_004 materially exceeded the randomized timing distribution on CAGR and Sharpe. The raw random drawdown distribution is substantially worse than IMP_004, although the reported max-drawdown percentile field should be treated as a direction-convention artifact unless separately documented.

## Development-Period Behavior

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Development 2000-2012 | IMP_004 | 5.48% | 0.521 | -18.61% | 59.71% |
| Development 2000-2012 | SPY buy-and-hold | 1.68% | 0.185 | -55.20% | 100.00% |
| Development 2000-2012 | 50/50 SPY/cash | 1.16% | 0.162 | -32.30% | 50.00% |

Development-period evidence is favorable. IMP_004 materially exceeded both registered baselines on CAGR, Sharpe, maximum drawdown, and ulcer index.

## Validation-Period Behavior

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Validation 2013-2026-06-02 | IMP_004 | 11.20% | 0.779 | -33.70% | 88.23% |
| Validation 2013-2026-06-02 | SPY buy-and-hold | 15.03% | 0.918 | -33.70% | 100.00% |
| Validation 2013-2026-06-02 | 50/50 SPY/cash | 7.54% | 0.921 | -17.74% | 50.00% |

Validation-period evidence is mixed. IMP_004 remained positive and exceeded the 50/50 baseline on CAGR, but it lagged buy-and-hold on CAGR and Sharpe, lagged both baselines on Sharpe, and had worse drawdown than the 50/50 baseline.

## Subperiod Behavior

| Subperiod | IMP_004 observation |
|---|---|
| 2000-2003 | Positive CAGR and materially better drawdown than both baselines. |
| 2008 | Loss was materially smaller than both baselines, with low time in market. |
| 2020 | Positive return but much lower CAGR and Sharpe than both baselines; drawdown matched buy-and-hold and was worse than 50/50. |
| 2022 | Loss was smaller than buy-and-hold but worse than the 50/50 baseline on return, drawdown, Sharpe, and ulcer index. |

Subperiod behavior is strongest in 2000-2003 and 2008. The 2020 and 2022 subperiods are important weaknesses.

## Stress-Test Behavior

| Stress test | CAGR | Sharpe | Max drawdown | Observation |
|---|---:|---:|---:|---|
| 20 bps round-trip | 8.42% | 0.668 | -33.70% | Performance remains close to base case. |
| 50 bps round-trip | 8.31% | 0.660 | -33.70% | Higher costs degrade modestly but do not erase evidence. |
| 1-day execution delay | 8.32% | 0.661 | -33.70% | Delay remains close to base case. |
| 9-month lookback perturbation | 7.06% | 0.582 | -36.09% | Directionally similar but weaker. |
| 15-month lookback perturbation | 8.86% | 0.681 | -33.70% | Directionally similar and stronger than base case. |
| Weekly evaluation perturbation | 7.28% | 0.612 | -31.20% | Directionally similar but lower CAGR and higher turnover. |
| Quarterly evaluation perturbation | 7.29% | 0.581 | -33.70% | Directionally similar but weaker than base case. |

Stress-test behavior is favorable overall. Cost and execution-delay stress tests remain close to the base case. Perturbations remain directionally similar, though 9-month, weekly, and quarterly variants are weaker.

## Known Limitations

- BACKTEST_004 is one implementation test and cannot certify CLAIM_001.
- Evidence is limited to SPY and does not establish broader dual momentum behavior across assets.
- This pilot tests only the absolute momentum component using SPY and cash.
- Cash return is fixed at 0%.
- Validation-period performance does not dominate buy-and-hold or the 50/50 baseline.
- Random timing baseline preserves time in market but not the implementation's full holding-period structure.
- The random max-drawdown percentile field requires documented direction convention before use.
- Results are hypothetical research evidence, not achieved portfolio performance.

## Result Review Status

IMP_004_RESULT_REVIEW_STATUS: FAVORABLE
