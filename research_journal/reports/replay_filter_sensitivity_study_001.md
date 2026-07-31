# Replay Filter Sensitivity Study 001

Date: 2026-06-05

## Scope

This is an offline, analysis-only sensitivity study for the five direct-validation candidates with trigger samples greater than zero and final replay samples equal to zero.

No production replay behavior was changed. The study reuses existing local data and existing deterministic trigger functions to estimate what would happen if diagnostic filters were relaxed.

## Method

- Baseline samples: current candidate-specific direct validation final replay samples.
- Without regime filter: count triggered samples before applying candidate regime constraints.
- Without quality filter: unchanged from baseline because direct validation does not currently implement a separate pre-replay quality filter.
- Without evidence filter: unchanged from baseline because direct validation does not currently implement a separate pre-replay evidence filter.
- Symbol-level instead of universe-level aggregation: maximum surviving sample count across individual symbols.
- Daily proxy vs required timeframe: diagnostic label comparing available daily direct data with the candidate timeframe requirement.

## Sensitivity Table

| candidate_id | baseline | without regime filter | without quality filter | without evidence filter | symbol-level instead of universe-level | daily proxy vs required timeframe |
|---|---:|---:|---:|---:|---:|---|
| `ptc_backtest_final_469607b8340421b7` | 0 | 1049 | 0 | 0 | 0 | DAILY_PROXY_AVAILABLE_BUT_REGIME_LABEL_MISMATCH |
| `ptc_backtest_final_3a4ac24107c77136` | 0 | 1049 | 0 | 0 | 0 | DAILY_PROXY_AVAILABLE_BUT_REGIME_LABEL_MISMATCH |
| `ptc_backtest_final_d5931b24bd391113` | 0 | 503 | 0 | 0 | 0 | MISMATCH_INTRADAY_REQUIRED |
| `ptc_backtest_final_7d839944a8a4070a` | 0 | 176 | 0 | 0 | 0 | DAILY_PROXY_AVAILABLE_BUT_REGIME_LABEL_MISMATCH |
| `ptc_backtest_final_b23c6756bfb3263a` | 0 | 626 | 0 | 0 | 0 | DAILY_PROXY_AVAILABLE_BUT_REGIME_LABEL_MISMATCH |

CSV companion: `research_journal/reports/replay_filter_sensitivity_table.csv`.

## Findings

Regime filtering is the only observed stage that can explain total sample loss for these candidates. Removing only the regime filter would create substantial trigger pools: 1049, 1049, 503, 176, and 626 samples respectively.

Quality and evidence filters are not currently separate sample-elimination stages in direct candidate data validation. Treating them as suspected fatal stages would invent pipeline behavior not present in the code path.

Symbol-level aggregation does not rescue any of the five candidates because every individual symbol run also has zero regime survivors.

Daily data coverage is no longer the bottleneck. The remaining mismatch is semantic and temporal: CHOP candidate labels do not survive the current daily-bar regime classifier, and the event-reaction candidate explicitly requires intraday plus daily confirmation.

## Authority Boundary

This study is diagnostic only. It does not change replay logic, relax production filters, promote candidates, override qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
