# RESULT_REVIEW_002

## Scope

Implementation reference: IMP_002 — 52-Week Breakout

Claim reference: CLAIM_001 — Trend Persistence

Evidence source: BACKTEST_002 evidence artifacts in `docs/aegis/technical_strategy_factory/evidence/backtest_002/`

This review describes implementation evidence only. It does not certify IMP_002, does not support CLAIM_001 by itself, and does not recommend capital allocation.

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

## Primary Metrics

| Series | Total return | CAGR | Volatility | Sharpe | Sortino | Max drawdown | Ulcer index | Time in market | Trade count |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| IMP_002 | 170.67% | 3.84% | 7.79% | 0.524 | 0.444 | -17.89% | 6.69% | 45.91% | 69 |
| SPY buy-and-hold | 732.02% | 8.35% | 19.31% | 0.513 | 0.652 | -55.20% | 16.17% | 100.00% | 1 |
| 50/50 SPY/cash monthly | 212.03% | 4.40% | 9.47% | 0.503 | 0.640 | -32.30% | 8.32% | 50.00% | 318 |

## Comparison Versus Buy-And-Hold

IMP_002 materially reduced volatility, maximum drawdown, and ulcer index versus SPY buy-and-hold. It had a slightly higher full-period Sharpe than buy-and-hold.

IMP_002 did not match buy-and-hold total return, CAGR, or Sortino. The evidence shows a lower-exposure, drawdown-reduction profile rather than return dominance.

## Comparison Versus 50/50 Baseline

IMP_002 had lower maximum drawdown and lower ulcer index than the 50/50 SPY/cash monthly baseline over the full period. It also had slightly higher full-period Sharpe.

IMP_002 had lower total return and lower CAGR than the 50/50 baseline. This limits the strength of the evidence because the exposure baseline captures part of the same risk-reduction behavior.

## Comparison Versus Random Timing Baseline

| Metric | Value |
|---|---:|
| Median random CAGR | -2.52% |
| 95th percentile random CAGR | 0.61% |
| Median random Sharpe | -0.131 |
| 95th percentile random Sharpe | 0.112 |
| Median random max drawdown | -67.22% |
| 95th percentile random max drawdown | -43.53% |
| IMP_002 percentile rank, CAGR | 100.00% |
| IMP_002 percentile rank, Sharpe | 100.00% |
| IMP_002 percentile rank, max drawdown | 0.00% |

IMP_002 materially exceeded the randomized timing distribution on CAGR and Sharpe. The raw random drawdown distribution is substantially worse than IMP_002, although the reported max-drawdown percentile field should be treated as a direction-convention artifact unless separately documented.

## Development-Period Behavior

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Development 2000-2012 | IMP_002 | 1.67% | 0.292 | -12.60% | 30.44% |
| Development 2000-2012 | SPY buy-and-hold | 1.68% | 0.185 | -55.20% | 100.00% |
| Development 2000-2012 | 50/50 SPY/cash | 1.16% | 0.162 | -32.30% | 50.00% |

Development-period evidence is favorable on drawdown and Sharpe. CAGR was approximately in line with buy-and-hold and above the 50/50 baseline.

## Validation-Period Behavior

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Validation 2013-2026-06-02 | IMP_002 | 6.00% | 0.696 | -17.89% | 60.91% |
| Validation 2013-2026-06-02 | SPY buy-and-hold | 15.03% | 0.918 | -33.70% | 100.00% |
| Validation 2013-2026-06-02 | 50/50 SPY/cash | 7.54% | 0.921 | -17.74% | 50.00% |

Validation-period evidence is mixed to weak. IMP_002 remained positive and reduced drawdown versus buy-and-hold, but it lagged both registered baselines on CAGR and Sharpe and did not improve drawdown versus the 50/50 baseline.

## Subperiod Behavior

| Subperiod | IMP_002 observation |
|---|---|
| 2000-2003 | Positive CAGR with materially lower drawdown than both baselines, but only 8.47% time in market. |
| 2008 | Stayed out of market, producing flat return and no drawdown while both baselines lost money. |
| 2020 | Negative return and near-zero Sharpe while both baselines were positive. |
| 2022 | Lost less than both baselines, with very low time in market. |

Subperiod behavior is strongest during major downside avoidance periods. It is weak during 2020 and does not show consistent participation in sharp upside recoveries.

## Stress-Test Behavior

| Stress test | CAGR | Sharpe | Max drawdown | Observation |
|---|---:|---:|---:|---|
| 20 bps round-trip | 3.71% | 0.507 | -18.13% | Performance remains close to base case. |
| 50 bps round-trip | 3.30% | 0.457 | -18.90% | Performance degrades but remains positive. |
| 1-day execution delay | 3.80% | 0.511 | -19.89% | Delay does not erase the evidence. |
| 200-day breakout perturbation | 4.41% | 0.578 | -17.89% | Similar and slightly stronger than base case. |
| 300-day breakout perturbation | 3.56% | 0.497 | -17.89% | Similar but weaker than base case. |
| 50DMA exit perturbation | 2.11% | 0.353 | -18.49% | Materially weaker than base case. |
| 150DMA exit perturbation | 5.59% | 0.670 | -18.02% | Stronger than base case. |

Higher-cost and execution-delay stress tests remain positive. Breakout lookback perturbations are directionally similar. Exit perturbations are less stable, with the 50DMA exit materially weaker and the 150DMA exit materially stronger.

## Known Limitations

- BACKTEST_002 is one implementation test and cannot certify CLAIM_001.
- Evidence is limited to SPY and does not establish broader trend-persistence behavior across assets.
- Cash return is fixed at 0%.
- The strategy spends less than half the full period in market, so lower exposure is a major explanatory candidate.
- Validation-period performance lags both registered baselines on CAGR and Sharpe.
- Random timing baseline preserves time in market but not the implementation's full holding-period structure.
- The random max-drawdown percentile field requires documented direction convention before use.
- Results are hypothetical research evidence, not achieved portfolio performance.

## Result Review Status

IMP_002_RESULT_REVIEW_STATUS: MIXED
