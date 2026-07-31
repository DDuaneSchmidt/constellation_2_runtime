# Universe Aggregation Audit 001

Date: 2026-06-05

## Scope

This audit covers the universe-level zero-sample candidates from direct candidate data validation. The question is whether individual symbols produce surviving samples and whether aggregation removes those samples.

## Current Aggregation Behavior

The direct validation implementation runs each resolved symbol independently and then selects the best direct run by support status, expectancy, and sample size. It does not currently require all-symbol confirmation. It also does not aggregate samples across symbols into one combined universe sample pool.

That means aggregation is not the immediate reason these five candidates end at zero. The zero-sample result occurs before candidate-level selection because every individual symbol has zero regime survivors.

## Per-Candidate Audit

| candidate_id | symbols audited | any individual symbol survivors? | does aggregation remove samples? | most trigger samples | least trigger samples | confirmation rule observed | current aggregation too strict? |
|---|---:|---|---|---|---|---|---|
| `ptc_backtest_final_469607b8340421b7` | 3 | no | no | SPY: 767 | DIA: 125 | best/any-symbol selection | no for sample attrition; yes for diagnostics because non-selected symbols are hidden |
| `ptc_backtest_final_3a4ac24107c77136` | 3 | no | no | SPY: 767 | DIA: 125 | best/any-symbol selection | no for sample attrition; yes for diagnostics because non-selected symbols are hidden |
| `ptc_backtest_final_d5931b24bd391113` | 6 | no | no | MSFT: 90 | META: 77 | best/any-symbol selection | no for sample attrition; yes for diagnostics because non-selected symbols are hidden |
| `ptc_backtest_final_7d839944a8a4070a` | 2 | no | no | USO: 105 | TLT: 71 | best/any-symbol selection | no for sample attrition; yes for diagnostics because non-selected symbols are hidden |
| `ptc_backtest_final_b23c6756bfb3263a` | 6 | no | no | NFLX: 119 | GOOGL: 96 | best/any-symbol selection | no for sample attrition; yes for diagnostics because non-selected symbols are hidden |

## Symbol-Level Detail

| candidate_id | symbol | data rows | raw count | trigger count | regime survivors | final samples |
|---|---|---:|---:|---:|---:|---:|
| `ptc_backtest_final_469607b8340421b7` | DIA | 1256 | 1202 | 125 | 0 | 0 |
| `ptc_backtest_final_469607b8340421b7` | QQQ | 1256 | 1202 | 157 | 0 | 0 |
| `ptc_backtest_final_469607b8340421b7` | SPY | 6643 | 6589 | 767 | 0 | 0 |
| `ptc_backtest_final_3a4ac24107c77136` | DIA | 1256 | 1202 | 125 | 0 | 0 |
| `ptc_backtest_final_3a4ac24107c77136` | QQQ | 1256 | 1202 | 157 | 0 | 0 |
| `ptc_backtest_final_3a4ac24107c77136` | SPY | 6643 | 6589 | 767 | 0 | 0 |
| `ptc_backtest_final_d5931b24bd391113` | AMZN | 1256 | 1204 | 85 | 0 | 0 |
| `ptc_backtest_final_d5931b24bd391113` | BAC | 1256 | 1204 | 80 | 0 | 0 |
| `ptc_backtest_final_d5931b24bd391113` | META | 1256 | 1204 | 77 | 0 | 0 |
| `ptc_backtest_final_d5931b24bd391113` | MSFT | 1256 | 1204 | 90 | 0 | 0 |
| `ptc_backtest_final_d5931b24bd391113` | NFLX | 1256 | 1204 | 82 | 0 | 0 |
| `ptc_backtest_final_d5931b24bd391113` | TSLA | 1256 | 1204 | 89 | 0 | 0 |
| `ptc_backtest_final_7d839944a8a4070a` | TLT | 1256 | 1202 | 71 | 0 | 0 |
| `ptc_backtest_final_7d839944a8a4070a` | USO | 1256 | 1202 | 105 | 0 | 0 |
| `ptc_backtest_final_b23c6756bfb3263a` | BAC | 1256 | 1202 | 98 | 0 | 0 |
| `ptc_backtest_final_b23c6756bfb3263a` | GOOGL | 1256 | 1202 | 96 | 0 | 0 |
| `ptc_backtest_final_b23c6756bfb3263a` | META | 1256 | 1202 | 104 | 0 | 0 |
| `ptc_backtest_final_b23c6756bfb3263a` | MSFT | 1256 | 1202 | 111 | 0 | 0 |
| `ptc_backtest_final_b23c6756bfb3263a` | NFLX | 1256 | 1202 | 119 | 0 | 0 |
| `ptc_backtest_final_b23c6756bfb3263a` | TSLA | 1256 | 1202 | 98 | 0 | 0 |

## Conclusion

Universe aggregation is not the sample-elimination bottleneck for the zero-sample candidates. The practical diagnostic issue is that the report surfaces only the selected direct run, which obscures per-symbol trigger counts and regime attrition.

Minimum remediation is symbol-level evidence summaries and stage-level telemetry, not a replay-behavior change.

## Authority Boundary

This audit is diagnostic only. It does not revise aggregation rules, promote candidates, override replay, override qualification, change governance, recommend trades, allocate capital, authorize broker execution, size positions, or place paper trades.
