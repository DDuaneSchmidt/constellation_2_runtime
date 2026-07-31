# Candidate Replay Trace 001

Date: 2026-06-05

## Scope

This diagnosis covers candidates in `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json` where candidate-specific direct validation ran, trigger samples were present, and final replay samples were zero.

Source artifacts:

- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/candidate_data_validation_plan/latest.json`
- `reports/atlas_v2_research_os/market_data_coverage_tracker/latest.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`

Runtime truth context: the Aegis runtime truth kernel reported `PARTIAL_CONTEXT` and `BLOCKED` on 2026-06-05. Trade advice, live trading, broker execution, capital authority, position sizing, candidate promotion, replay override, qualification override, and governance override remain unauthorized.

## Summary

Direct validation reviewed 8 candidates. All 8 had local direct data and all 8 direct replays ran. Five candidates had trigger samples greater than zero and final replay sample count equal to zero.

The fatal stage for all five is regime filtering. The direct validation code path does not currently expose separate quality-filter or evidence-filter stages before sample construction. For this trace, quality-filter and evidence-filter surviving counts are recorded as equal to the post-regime count, with the important caveat that these are diagnostic checkpoints, not separate production filters.

## Trace Table

| candidate_id | symbols | timeframe | raw universe samples | trigger samples | regime survivors | quality survivors | evidence survivors | final samples | first fatal stage | root cause |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| `ptc_backtest_final_469607b8340421b7` | DIA, QQQ, SPY | daily bars plus optional intraday confirmation | 8993 | 1049 | 0 | 0 | 0 | 0 | regime_filter | REGIME_FILTER_ATTRITION |
| `ptc_backtest_final_3a4ac24107c77136` | DIA, QQQ, SPY | daily bars plus optional intraday confirmation | 8993 | 1049 | 0 | 0 | 0 | 0 | regime_filter | REGIME_FILTER_ATTRITION |
| `ptc_backtest_final_d5931b24bd391113` | AMZN, BAC, META, MSFT, NFLX, TSLA | intraday plus daily confirmation | 7224 | 503 | 0 | 0 | 0 | 0 | regime_filter | TIMEFRAME_MISMATCH |
| `ptc_backtest_final_7d839944a8a4070a` | TLT, USO | daily bars plus optional intraday confirmation | 2404 | 176 | 0 | 0 | 0 | 0 | regime_filter | REGIME_FILTER_ATTRITION |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC, GOOGL, META, MSFT, NFLX, TSLA | daily bars plus optional intraday confirmation | 7212 | 626 | 0 | 0 | 0 | 0 | regime_filter | REGIME_FILTER_ATTRITION |

CSV companion: `research_journal/reports/candidate_replay_trace_table.csv`.

## Candidate Notes

### `ptc_backtest_final_469607b8340421b7`

- Mechanism/regime: BREAKOUT / CHOP.
- Direct validation selected DIA as the best direct run, but DIA, QQQ, and SPY each had zero regime survivors.
- Suspected root cause: `REGIME_FILTER_ATTRITION`.
- Minimum remediation: add stage-level telemetry and regime-label translation diagnostics. Specifically verify whether candidate `CHOP` should map to daily-bar `RANGE_BOUND`, `UNKNOWN`, or another deterministic regime label.

### `ptc_backtest_final_3a4ac24107c77136`

- Mechanism/regime: BREAKOUT / CHOP.
- Same symbol set and same attrition profile as `ptc_backtest_final_469607b8340421b7`.
- Suspected root cause: `REGIME_FILTER_ATTRITION`.
- Minimum remediation: add stage-level telemetry and regime-label translation diagnostics before changing replay behavior.

### `ptc_backtest_final_d5931b24bd391113`

- Mechanism/regime: EVENT_REACTION / CHOP.
- Required timeframe: intraday plus daily confirmation.
- Every daily symbol run produced trigger samples but zero CHOP-regime survivors.
- Suspected root cause: `TIMEFRAME_MISMATCH`, with regime attrition as the observed fatal stage.
- Minimum remediation: define event metadata and intraday replay diagnostics before interpreting daily proxy failure as candidate failure.

### `ptc_backtest_final_7d839944a8a4070a`

- Mechanism/regime: BREAKOUT / CHOP.
- TLT and USO both produced trigger samples and zero regime survivors.
- Suspected root cause: `REGIME_FILTER_ATTRITION`.
- Minimum remediation: add per-symbol stage telemetry and regime-label translation diagnostics.

### `ptc_backtest_final_b23c6756bfb3263a`

- Mechanism/regime: BREAKOUT / CHOP.
- BAC, GOOGL, META, MSFT, NFLX, and TSLA each produced trigger samples and zero regime survivors.
- Suspected root cause: `REGIME_FILTER_ATTRITION`.
- Minimum remediation: add per-symbol stage telemetry and regime-label translation diagnostics.

## Authority Boundary

This report is diagnostic only. It does not promote candidates, override replay, override qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
