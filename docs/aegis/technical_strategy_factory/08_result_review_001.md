# RESULT_REVIEW_001

## Scope

Implementation reference: IMP_001 — SPY 200DMA Trend Following

Claim reference: CLAIM_001 — Trend Persistence

Evidence source: BACKTEST_001 evidence artifacts in `docs/aegis/technical_strategy_factory/evidence/backtest_001/`

This review describes implementation evidence only. It does not certify IMP_001, does not support CLAIM_001 by itself, and does not recommend capital allocation.

## Run Context

| Field | Value |
|---|---|
| Data source | Tiingo cached local CSV |
| Date range | 2000-01-03 through 2026-06-02 |
| Run quality | PRIMARY_ADJUSTED_OHLC |
| Price fields | adjOpen, adjHigh, adjLow, adjClose |
| Cost model | 10 bps round-trip |
| Execution policy | Signal after close, execute next market open |
| Cash return | 0% |

## Full-Period Metrics

| Series | Total return | CAGR | Volatility | Sharpe | Max drawdown | Ulcer index | Time in market |
|---|---:|---:|---:|---:|---:|---:|---:|
| IMP_001 | 466.43% | 6.79% | 10.91% | 0.658 | -24.45% | 8.37% | 71.35% |
| SPY buy-and-hold | 732.02% | 8.35% | 19.31% | 0.513 | -55.20% | 16.17% | 100.00% |
| 50/50 SPY/cash monthly | 212.03% | 4.40% | 9.47% | 0.503 | -32.30% | 8.32% | 50.00% |

IMP_001 reduced volatility and drawdown materially versus SPY buy-and-hold and improved Sharpe. It did not match buy-and-hold total return or CAGR. Compared with the 50/50 exposure baseline, IMP_001 produced higher total return, higher CAGR, and higher Sharpe, with similar ulcer index and higher volatility.

## Random Timing Baseline

| Metric | Value |
|---|---:|
| Median random CAGR | 0.47% |
| 95th percentile random CAGR | 3.25% |
| Median random Sharpe | 0.110 |
| 95th percentile random Sharpe | 0.277 |
| Median random max drawdown | -63.54% |
| 95th percentile random max drawdown | -45.17% |
| IMP_001 percentile rank, CAGR | 100.00% |
| IMP_001 percentile rank, Sharpe | 100.00% |
| IMP_001 percentile rank, max drawdown | 0.00% |

IMP_001 materially exceeded the randomized timing distribution on CAGR and Sharpe. The raw random drawdown distribution is substantially worse than IMP_001, although the reported max-drawdown percentile field appears directionally inconsistent with that raw distribution and should be checked before relying on that percentile field.

## Development And Validation

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Development 2000-2012 | IMP_001 | 2.65% | 0.312 | -24.45% | 57.33% |
| Development 2000-2012 | SPY buy-and-hold | 1.68% | 0.185 | -55.20% | 100.00% |
| Development 2000-2012 | 50/50 SPY/cash | 1.16% | 0.162 | -32.30% | 50.00% |
| Validation 2013-2026-06-02 | IMP_001 | 10.75% | 0.935 | -19.52% | 84.94% |
| Validation 2013-2026-06-02 | SPY buy-and-hold | 15.03% | 0.918 | -33.70% | 100.00% |
| Validation 2013-2026-06-02 | 50/50 SPY/cash | 7.54% | 0.921 | -17.74% | 50.00% |

Development-period evidence is favorable versus both registered baselines. Validation-period evidence is mixed: IMP_001 retained positive CAGR, high Sharpe, and lower drawdown than buy-and-hold, but materially lagged buy-and-hold CAGR and had worse drawdown and ulcer profile than the 50/50 baseline.

## Subperiod Behavior

| Subperiod | IMP_001 observation |
|---|---|
| 2000-2003 | Positive CAGR and materially better drawdown than both baselines. |
| 2008 | Small loss and very low drawdown versus severe baseline drawdowns. |
| 2020 | Positive return, but lower return and Sharpe than both baselines. |
| 2022 | Loss smaller than buy-and-hold but worse than 50/50 SPY/cash. |

Subperiod behavior is not uniformly superior. IMP_001 was strongest in major bear-market avoidance periods and weaker during sharp recovery or lower-exposure comparison periods.

## Stress Tests

| Stress test | CAGR | Sharpe | Max drawdown | Observation |
|---|---:|---:|---:|---|
| 20 bps round-trip | 6.46% | 0.629 | -24.89% | Performance remains close to base case. |
| 50 bps round-trip | 5.47% | 0.543 | -26.23% | Performance degrades but remains positive. |
| 1-day execution delay | 7.05% | 0.674 | -24.32% | Delay did not impair this run. |
| 180DMA perturbation | 5.90% | 0.580 | -29.73% | Similar but weaker and higher drawdown. |
| 220DMA perturbation | 6.76% | 0.650 | -25.06% | Very close to 200DMA base case. |

The implementation survives the registered stress tests in the sense that evidence remains deployable and positive after higher costs, execution delay, and nearby parameter review. The 180DMA review is weaker than the base case and should remain a concern, not a parameter change.

## Known Limitations

- BACKTEST_001 is one implementation test and cannot certify CLAIM_001.
- Evidence is limited to SPY and does not establish broader trend-persistence behavior across assets.
- Cash return is fixed at 0%.
- Random timing baseline preserves time in market but not trade count or holding-period distribution.
- The reported random max-drawdown percentile field needs review because it conflicts with the raw drawdown distribution.
- Results are hypothetical research evidence, not achieved portfolio performance.

## Result Review Status

IMP_001_RESULT_REVIEW_STATUS: MIXED
