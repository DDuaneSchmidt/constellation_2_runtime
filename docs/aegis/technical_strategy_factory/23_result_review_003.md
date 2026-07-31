# RESULT_REVIEW_003

## Scope

Implementation reference: IMP_003 — Turtle Breakout

Claim reference: CLAIM_001 — Trend Persistence

Evidence source: BACKTEST_003 evidence artifacts in `docs/aegis/technical_strategy_factory/evidence/backtest_003/`

This review describes implementation evidence only. It does not certify IMP_003, does not support CLAIM_001 by itself, and does not recommend capital allocation.

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
| IMP_003 | 102.72% | 2.71% | 8.53% | 0.357 | 0.316 | -29.16% | 12.93% | 48.01% | 159 |
| SPY buy-and-hold | 732.02% | 8.35% | 19.31% | 0.513 | 0.652 | -55.20% | 16.17% | 100.00% | 1 |
| 50/50 SPY/cash monthly | 212.03% | 4.40% | 9.47% | 0.503 | 0.640 | -32.30% | 8.32% | 50.00% | 318 |

## Comparison Versus Buy-And-Hold

IMP_003 reduced volatility, maximum drawdown, and ulcer index versus SPY buy-and-hold.

IMP_003 did not match buy-and-hold total return, CAGR, Sharpe, or Sortino. The evidence shows downside reduction at the cost of materially lower return and weaker risk-adjusted performance.

## Comparison Versus 50/50 Baseline

IMP_003 had slightly lower maximum drawdown than the 50/50 SPY/cash monthly baseline over the full period.

IMP_003 lagged the 50/50 baseline on total return, CAGR, Sharpe, Sortino, and ulcer index. This is a major weakness because IMP_003 had similar full-period time in market to the exposure baseline.

## Comparison Versus Random Timing Baseline

| Metric | Value |
|---|---:|
| Median random CAGR | -2.39% |
| 95th percentile random CAGR | 0.66% |
| Median random Sharpe | -0.113 |
| 95th percentile random Sharpe | 0.116 |
| Median random max drawdown | -67.08% |
| 95th percentile random max drawdown | -44.94% |
| IMP_003 percentile rank, CAGR | 99.40% |
| IMP_003 percentile rank, Sharpe | 100.00% |
| IMP_003 percentile rank, max drawdown | 0.10% |

IMP_003 materially exceeded the randomized timing distribution on CAGR and Sharpe. The raw random drawdown distribution is substantially worse than IMP_003, although the reported max-drawdown percentile field should be treated as a direction-convention artifact unless separately documented.

## Development-Period Behavior

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Development 2000-2012 | IMP_003 | 1.17% | 0.180 | -29.16% | 38.02% |
| Development 2000-2012 | SPY buy-and-hold | 1.68% | 0.185 | -55.20% | 100.00% |
| Development 2000-2012 | 50/50 SPY/cash | 1.16% | 0.162 | -32.30% | 50.00% |

Development-period evidence is mixed. IMP_003 reduced drawdown versus buy-and-hold and was close to the 50/50 baseline on CAGR, but it did not improve Sharpe versus buy-and-hold and had worse ulcer index than the 50/50 baseline.

## Validation-Period Behavior

| Period | Series | CAGR | Sharpe | Max drawdown | Time in market |
|---|---|---:|---:|---:|---:|
| Validation 2013-2026-06-02 | IMP_003 | 4.23% | 0.524 | -19.74% | 57.68% |
| Validation 2013-2026-06-02 | SPY buy-and-hold | 15.03% | 0.918 | -33.70% | 100.00% |
| Validation 2013-2026-06-02 | 50/50 SPY/cash | 7.54% | 0.921 | -17.74% | 50.00% |

Validation-period evidence is unfavorable. IMP_003 remained positive and reduced drawdown versus buy-and-hold, but it lagged both registered baselines on CAGR, Sharpe, Sortino, and ulcer index, and it had worse drawdown than the 50/50 baseline.

## Subperiod Behavior

| Subperiod | IMP_003 observation |
|---|---|
| 2000-2003 | Negative CAGR and negative Sharpe; better drawdown than buy-and-hold but worse than the 50/50 baseline. |
| 2008 | Small loss with much lower drawdown than both baselines. |
| 2020 | Positive return but lower CAGR and Sharpe than both baselines. |
| 2022 | Smaller loss than buy-and-hold but worse loss, Sharpe, and drawdown than the 50/50 baseline. |

Subperiod behavior is not consistently favorable. The strongest evidence appears in 2008 downside avoidance, while 2000-2003, 2020, and 2022 show important weaknesses versus at least one registered baseline.

## Stress-Test Behavior

| Stress test | CAGR | Sharpe | Max drawdown | Observation |
|---|---:|---:|---:|---|
| 20 bps round-trip | 2.40% | 0.322 | -29.72% | Performance remains positive but weaker than base case. |
| 50 bps round-trip | 1.48% | 0.215 | -31.39% | Performance degrades materially. |
| 1-day execution delay | 2.27% | 0.300 | -30.52% | Delay weakens the base case. |
| 40-day breakout perturbation | 3.46% | 0.419 | -28.96% | Similar and stronger than base case. |
| 70-day breakout perturbation | 2.90% | 0.400 | -19.95% | Similar and somewhat stronger than base case. |
| 10-day low exit perturbation | 2.09% | 0.330 | -22.08% | Lower return but lower drawdown than base case. |
| 30-day low exit perturbation | 2.30% | 0.289 | -34.72% | Weaker drawdown and Sharpe than base case. |

Stress tests remain positive but generally weak. Higher costs and execution delay degrade the evidence. Parameter perturbations are directionally similar, but the base case and perturbations do not overcome the registered baseline concerns.

## Known Limitations

- BACKTEST_003 is one implementation test and cannot certify CLAIM_001.
- Evidence is limited to SPY and does not establish broader trend-persistence behavior across assets.
- Cash return is fixed at 0%.
- IMP_003 has similar time in market to the 50/50 baseline but weaker full-period and validation-period risk-adjusted performance.
- Trade count is materially higher than IMP_001 and IMP_002 evidence cycles.
- Random timing baseline preserves time in market but not the implementation's full holding-period structure.
- The random max-drawdown percentile field requires documented direction convention before use.
- Results are hypothetical research evidence, not achieved portfolio performance.

## Result Review Status

IMP_003_RESULT_REVIEW_STATUS: UNFAVORABLE
