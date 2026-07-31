# Post Daily Completion Validation 001

Generated: `2026-06-05T20:10:52Z`

Scope: secondary daily market-data acquisition and validation reporting for Atlas Research OS. No production, governance, authority, candidate promotion, qualification, paper placement, live trading, broker execution, position sizing, or capital allocation changes.

Runtime truth boundary: pre-change `npm run aegis:audit` exited with strict verified graph `BLOCKED` and `audit_blocker_count=12`. Runtime truth kernel reported `runtime_truth_classification=PARTIAL_CONTEXT`, `highest_readiness_layer=BLOCKED`, `trade_advice_allowed=false`, `manual_trade_capture_allowed=false`, and `autonomous_execution_allowed=false`.

## Inputs

- Requested symbols: `AAPL`, `JPM`, `TLT`, `USO`, `DBC`
- Acquisition output: `reports/atlas_v2_research_os/daily_completion_acquisition/market_data_acquisition_001.md`
- Coverage tracker output: `reports/atlas_v2_research_os/market_data_coverage_tracker/latest_summary.md`
- Direct validation refresh output: `reports/atlas_v2_research_os/direct_validation_refresh/2026-06-05-daily-completion/validation_delta_report.md`
- Direct validation report: `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`

## Acquisition

| Symbol | CSV | Rows | Validation |
| --- | --- | ---: | --- |
| `AAPL` | `data/cache/AAPL_tiingo_adjusted_daily.csv` | 1256 | PASS |
| `JPM` | `data/cache/JPM_tiingo_adjusted_daily.csv` | 1256 | PASS |
| `TLT` | `data/cache/TLT_tiingo_adjusted_daily.csv` | 1256 | PASS |
| `USO` | `data/cache/USO_tiingo_adjusted_daily.csv` | 1256 | PASS |
| `DBC` | `data/cache/DBC_tiingo_adjusted_daily.csv` | 1256 | PASS |

All requested daily adjusted OHLCV CSVs were acquired and schema-validated.

## Coverage Tracker

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Required symbols with data | 10/15 | 15/15 | +5 |
| Required symbols missing data | 5 | 0 | -5 |
| Coverage percent | 66.67% | 100.0% | +33.33 pp |
| Candidate-symbol pair coverage | 25/32 | 32/32 | +7 pairs |
| Candidate-symbol pair coverage percent | 78.12% | 100.0% | +21.88 pp |
| Full candidate coverage | 5/8 | 8/8 | +3 candidates |
| Validation block rate from coverage | 37.5% | 0.0% | -37.5 pp |

Missing symbols after the coverage tracker run: `NONE`.

## Direct Validation

| Metric | Before acquisition | After acquisition | Forced refresh after latest update |
| --- | ---: | ---: | ---: |
| `CONFIRMED` | 1 | 2 | 2 |
| `WEAKENED` | 0 | 0 | 0 |
| `INSUFFICIENT_DATA` | 7 | 6 | 6 |
| Direct data exists count | 6 | 8 | 8 |
| Direct replays run | 6 | 8 | 8 |
| Missing CSV count | >0 | 0 | 0 |

The acquisition removed daily-data coverage as a blocker for the focused validation set. The forced validation refresh made no additional classification change after `latest.json` had already been refreshed by the acquisition workflow.

## Remaining Insufficient Data Cases

| Candidate ID | Symbols | Daily data available | Direct replay ran | Remaining issue |
| --- | --- | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | `DIA`, `QQQ`, `SPY` | yes | yes | Minimum sample size not met after trigger/regime filtering; observed `0`. |
| `ptc_backtest_final_3a4ac24107c77136` | `DIA`, `QQQ`, `SPY` | yes | yes | Minimum sample size not met after trigger/regime filtering; observed `0`. |
| `ptc_backtest_final_624fdd85668e2c08` | `DBC`, `TLT`, `USO` | yes | yes | Direct replay produced `BACKTEST_WEAK` on `DBC`; not a missing-daily-data problem. |
| `ptc_backtest_final_d5931b24bd391113` | `AMZN`, `BAC`, `META`, `MSFT`, `NFLX`, `TSLA` | yes | yes | Minimum sample size not met after trigger/regime filtering; observed `0`. |
| `ptc_backtest_final_7d839944a8a4070a` | `TLT`, `USO` | yes | yes | Minimum sample size not met after trigger/regime filtering; observed `0`. |
| `ptc_backtest_final_b23c6756bfb3263a` | `BAC`, `GOOGL`, `META`, `MSFT`, `NFLX`, `TSLA` | yes | yes | Minimum sample size not met after trigger/regime filtering; observed `0`. |

Conclusion: the remaining `INSUFFICIENT_DATA` cases are no longer daily-data availability problems. They are now validation-design or evidence-strength problems: trigger/regime filtering produces insufficient qualifying samples for five candidates, and one candidate has direct evidence that is weak rather than supported.

## Authority Boundary

This was market-data acquisition and validation reporting only. No candidate promotion, qualification change, replay override, governance override, production integration, paper placement, trade recommendation, live trading, broker execution, capital allocation, or position sizing was authorized or performed.
