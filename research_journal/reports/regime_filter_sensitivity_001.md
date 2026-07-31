# Regime Filter Sensitivity 001

Date: 2026-06-05

## Scope

This is an offline sensitivity study for the five zero-sample direct-validation candidates. It does not change production replay logic or production regime filters.

CSV companion: `research_journal/reports/regime_filter_sensitivity_table.csv`.

## Definitions

- Baseline passing samples: current exact-regime match, where allowed regime is `CHOP`.
- Relaxed regime threshold samples: triggered samples whose daily replay regime is `RANGE_BOUND`, `LOW_VOLATILITY`, or `UNKNOWN`, used only as an analysis proxy for possible CHOP compatibility.
- Alternate regime label samples: triggered samples whose daily replay regime is `RANGE_BOUND`.
- No-regime-filter samples: all triggered samples before regime filtering.
- Matching-timeframe-only assessment: whether daily proxy replay can match the required candidate timeframe.

## Sensitivity Table

| candidate_id | classification | baseline | relaxed regime threshold | alternate regime label | no regime filter | matching-timeframe-only assessment |
|---|---|---:|---:|---:|---:|---|
| `ptc_backtest_final_469607b8340421b7` | REGIME_LABEL_MISMATCH | 0 | 208 | 146 | 1049 | DAILY_COMPONENT_AVAILABLE_OPTIONAL_INTRADAY_CONFIRMATION_UNRESOLVED |
| `ptc_backtest_final_3a4ac24107c77136` | REGIME_LABEL_MISMATCH | 0 | 208 | 146 | 1049 | DAILY_COMPONENT_AVAILABLE_OPTIONAL_INTRADAY_CONFIRMATION_UNRESOLVED |
| `ptc_backtest_final_d5931b24bd391113` | TIMEFRAME_REGIME_MISMATCH | 0 | 75 | 49 | 503 | INTRADAY_REQUIRED_DAILY_PROXY_INSUFFICIENT |
| `ptc_backtest_final_7d839944a8a4070a` | REGIME_LABEL_MISMATCH | 0 | 38 | 27 | 176 | DAILY_COMPONENT_AVAILABLE_OPTIONAL_INTRADAY_CONFIRMATION_UNRESOLVED |
| `ptc_backtest_final_b23c6756bfb3263a` | REGIME_LABEL_MISMATCH | 0 | 57 | 28 | 626 | DAILY_COMPONENT_AVAILABLE_OPTIONAL_INTRADAY_CONFIRMATION_UNRESOLVED |

## Findings

The baseline always passes zero samples because the exact `CHOP` label is not emitted by the daily replay regime classifier.

Relaxed diagnostic compatibility would recover nonzero sample pools for all five candidates, ranging from 38 to 208 samples under `RANGE_BOUND` plus `LOW_VOLATILITY` plus `UNKNOWN`. This is not a recommendation to relax production filters; it shows that the zero-sample result is sensitive to regime vocabulary translation rather than trigger absence or missing daily data.

The event-reaction candidate remains a timeframe-regime mismatch even under relaxed labels because its validation requirement is intraday plus daily confirmation.

## Authority Boundary

This study is diagnostic only. It does not change replay behavior, relax production filters, promote candidates, change qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
