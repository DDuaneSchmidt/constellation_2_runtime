# UltraSafe Short Hedge Study

## Research Boundary

- Purpose: determine whether a short sleeve is likely worth further research as an UltraSafe complement.
- Authority: research-only. This is not trade advice, not a capital allocation recommendation, not position sizing, and not broker or paper-trade authority.
- Runtime truth context: latest verified runtime graph read from `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-10/verified_runtime_graph.v1.json`; active mode is `HUMAN_REVIEWED_PAPER_MODE` and readiness is `BLOCKED`.
- No API calls were used. Inputs are local files only.

## Source Binding

- UltraSafe returns: `reports/portfolio_research_os/pb001_price_only_baseline/portfolio_monthly_returns.csv`, portfolio size 25, READY rows.
- Companion returns: `reports/portfolio_research_os/pb001_price_only_baseline/benchmark_monthly_returns.csv`, READY rows for `SPY`, `IWM`, and `60_40_PROXY`.
- Local candidate database: `reports/atlas_v2_research_os/final_candidate_ranking/2026-06-05/final_candidate_ranking_report.json`.
- Aligned return window: 10 monthly observations, `2022-12` through `2023-09`.
- Short sleeve modeling convention: inverse companion monthly return, before borrow cost, rebate, slippage, implementation frictions, and path-dependent short constraints.

## Baseline

| Series | CAGR | Sharpe | Max Drawdown | Annual Vol | Total Return |
|---|---:|---:|---:|---:|---:|
| 100% UltraSafe local series | +34.39% | 1.367 | -10.68% | 23.73% | +27.93% |

The local series is short and favorable for UltraSafe. The existing frontier documentation also carries an UltraSafe benchmark override of 19.00% CAGR, 1.000 Sharpe, and -36.00% max drawdown, but this study uses the local monthly series for allocation math because it is the available return history.

## Allocation Results

Primary hedge proxy: inverse SPY companion returns.

| Allocation | CAGR | Return Drag vs 100% | Sharpe | Sharpe Change | Max Drawdown | Drawdown Reduction |
|---|---:|---:|---:|---:|---:|---:|
| 100% UltraSafe | +34.39% | +0.00% | 1.367 | +0.000 | -10.68% | +0.00% |
| 95% UltraSafe + 5% short sleeve | +32.32% | -2.07% | 1.392 | +0.025 | -9.84% | +0.84% |
| 90% UltraSafe + 10% short sleeve | +30.24% | -4.15% | 1.421 | +0.053 | -9.00% | +1.68% |
| 85% UltraSafe + 15% short sleeve | +28.15% | -6.24% | 1.454 | +0.087 | -8.15% | +2.53% |

Sensitivity using inverse IWM and inverse 60/40 companion returns:

| Short Sleeve Proxy | Allocation | CAGR | Return Drag | Sharpe Change | Drawdown Reduction |
|---|---:|---:|---:|---:|---:|
| Inverse IWM | 5% | +33.13% | -1.25% | +0.067 | +0.88% |
| Inverse IWM | 10% | +31.84% | -2.55% | +0.148 | +1.77% |
| Inverse IWM | 15% | +30.50% | -3.88% | +0.245 | +2.65% |
| Inverse 60/40 proxy | 5% | +32.89% | -1.50% | +0.041 | +0.79% |
| Inverse 60/40 proxy | 10% | +31.37% | -3.02% | +0.088 | +1.57% |
| Inverse 60/40 proxy | 15% | +29.82% | -4.56% | +0.142 | +2.36% |

## Required Improvement Thresholds

For a short sleeve to be worth continued research rather than just reducing gross exposure, it should clear these hurdles in the local framework:

| Sleeve Weight | Drawdown Reduction Required | Return Drag Tolerated | Sharpe Improvement Required | Read |
|---:|---:|---:|---:|---|
| 5% | at least 0.75 percentage points | no worse than 1.5 to 2.0 percentage points CAGR | at least +0.03 | Small enough for exploration if the hedge is operationally simple. |
| 10% | at least 1.50 percentage points | no worse than 3.0 percentage points CAGR | at least +0.05 | Best research ceiling from current evidence. |
| 15% | at least 2.50 percentage points | no worse than 4.0 percentage points CAGR | at least +0.10 | Too large unless the sleeve is conditional or has materially better carry than naive inverse SPY. |

Naive inverse SPY passes the 10% drawdown and Sharpe hurdle but fails the 10% return-drag hurdle. Inverse IWM and inverse 60/40 look better in this short sample, but that is not enough evidence to authorize a larger sleeve.

## Local Candidate Database Read

The local candidate database supports research triage, not allocation:

- 600 candidates were evaluated in the final ranking summary.
- 58 candidates were classified `READY_FOR_PAPER_FORWARD_OBSERVATION`.
- The biggest remaining risk in the summary is that selected candidates still depend on SPY daily proxy evidence until candidate-specific data is supplied.
- Top local rows include `BREAKOUT`, `MEAN_REVERSION`, `EVENT_REACTION`, and related intraday mechanisms, with symbols including SPY, QQQ, IWM, TLT, USO, DBC, and large-cap equities.
- Candidate authority boundaries explicitly deny capital authorization, position sizing, trade recommendation authority, broker execution, and production promotion.

Conclusion from candidate evidence: there are plausible ingredients for a hedge research program, especially equity-index, rates, commodity, and volatility-sensitive mechanisms, but no local candidate currently proves a live short sleeve or an UltraSafe allocation overlay.

## Maximum Acceptable Short Allocation

Maximum acceptable short allocation for research design: 10%.

5% is the preferred first test size. 10% is the upper bound worth modeling further if the hedge clears the target characteristics below. 15% is not acceptable on current evidence unless future testing proves materially better carry, conditional activation, and stable crisis payoff.

## Target Hedge Characteristics

A worthwhile short sleeve should target:

- Correlation to UltraSafe monthly returns at or below -0.75 during the local stress windows.
- Positive payoff in UltraSafe weakness months, not merely lower gross exposure.
- 10% sleeve drawdown reduction of at least 1.5 percentage points versus 100% UltraSafe.
- 10% sleeve return drag no worse than about 3 percentage points annualized CAGR after realistic shorting and transaction costs.
- 10% sleeve Sharpe improvement of at least +0.05, with preference for +0.10 or better.
- Conditional activation or dynamic sizing so the sleeve is not structurally short during normal positive-equity months.
- Candidate-specific evidence, not SPY proxy evidence, before being treated as anything beyond research.

## Is Short Research Worthwhile?

Yes, but only as a constrained research track.

The local math says a short sleeve can reduce drawdown and improve Sharpe, but naive always-on inverse SPY pays too much return drag for the amount of protection delivered. The most promising path is not a permanent broad-market short. It is a conditional hedge sleeve that activates around completed-bar weakness, breadth deterioration, volatility expansion, or candidate-specific short signals, and that is capped at 5% to 10% until it proves better carry and better stress specificity.

Research is likely worthwhile if the next work focuses on short-sleeve signal quality and regime activation. It is not yet worthwhile as an allocation change.
