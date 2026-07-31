# Efficient Frontier Analysis

## Answer

- UltraSafe dominated by any local candidate? No.
- Which candidates dominate UltraSafe?
- None.
- Which candidates are dominated by UltraSafe?
- None.
- Where does UltraSafe sit relative to the local frontier? UltraSafe is non-dominated on the strict CAGR/Sharpe/drawdown benchmark frontier. This does not mean Aegis discovered UltraSafe; it is injected only as `benchmark_override`.
- Complete best challenger: n/a
- Best challenger gap: Best complete local challenger is n/a. Gap vs UltraSafe: CAGR n/a, Sharpe n/a, drawdown n/a.
- Incomplete candidates excluded from comparison: 50
- Runtime truth context: `BLOCKED` in `HUMAN_REVIEWED_PAPER_MODE`. This report is read-only research analysis, not trade advice.

## Inputs

- Requested local candidate count: 50
- Local candidate rows available for score/drawdown comparison: 50
- Local candidate rows with true expectancy: 20
- Complete local benchmark candidates with CAGR, Sharpe, and max_drawdown: 0
- Incomplete local benchmark candidates excluded from benchmark comparison: 50
- UltraSafe benchmark: CAGR 19.00%, Sharpe 1.000000, Max DD -36.00%, source `benchmark_override`
- Source report: `reports/atlas_v2_research_os/final_candidate_ranking/2026-06-05/final_candidate_ranking_report.json`
- Verified runtime graph: `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-10/verified_runtime_graph.v1.json`
- Data caveat: The top-50 local comparison uses `reports/atlas_v2_research_os/final_candidate_ranking/2026-06-05/final_candidate_ranking_report.json` without changing candidate results. Only 20 of the top 50 rows expose true expectancy, and 0 expose complete benchmark metrics. UltraSafe is an explicit benchmark_override.

## Benchmark Frontier

| Point | Source | CAGR | Sharpe | Max DD | Benchmark frontier |
|---|---|---:|---:|---:|---|
| `UltraSafe` | benchmark_override | 19.00% | 1.000000 | -36.00% | yes |

## Incomplete Frontier Candidates

- `ptc_backtest_final_469607b8340421b7` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_3a4ac24107c77136` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_7d839944a8a4070a` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_b23c6756bfb3263a` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_f74e86444d2d96de` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_624fdd85668e2c08` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_b373e39b4e8f7af1` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_854ad10b904e1ae9` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_0ca1c062615b416a` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_7e197a281aada275` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_d68a56480f7d5eac` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_ad17bd73fcdf2a7d` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_7567e1ec1fce9888` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_32638f4eabddae6b` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_9e67522067aeb917` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_e6db1c5826c38663` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_18287c1f075e9230` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_34a365f30c889811` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_b0b94052c4989a3f` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_758c67480dc36952` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_c3b841c7293b26cc` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_d5931b24bd391113` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_05ce7ac3641b38b6` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_ce33abb5d7de02cb` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_1da3d53fcc9ad34b` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.54%
- `ptc_backtest_final_b40c8aa04b4b60a9` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_5e01b95ae069b764` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_990d036cd869f982` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_4df2e8e80685a054` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_ff7345652b802294` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_e962558456a60109` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_9c1d0b63444423ff` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-12.28%
- `ptc_backtest_final_c185b04196a63848` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_651cd169dd508c4e` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_8529139d60dcbbf6` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%
- `ptc_backtest_final_a48c07e0392e11dc` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_b14a4da3d5f9849e` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%
- `ptc_backtest_final_67346da117cdf47e` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_8edabf7988a79611` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_a377609849aa09e6` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%
- `ptc_backtest_final_bd2edd5aa289c971` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_7dd42dae2e8077ec` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%
- `ptc_backtest_final_12e0e8c0c73cff12` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_eb02cf745abcee8a` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-16.01%
- `ptc_backtest_final_b3e1a31555e0118e` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_71f8f33d0d8867e9` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%
- `ptc_backtest_final_ec85ecb1a09724ff` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_b6f0b8673f4168f2` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%
- `ptc_backtest_final_4951b825751299d1` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-13.61%
- `ptc_backtest_final_292baf4e63bd7910` source=local_candidate_result missing=cagr,sharpe cagr=n/a sharpe=n/a max_dd=-11.58%

## Risk vs Return Summary

- Score frontier points: 3 of 50 local top candidates.
- Expectancy frontier points: 18 of 20 local candidates with true expectancy.
- Score/expectancy frontier excludes UltraSafe because UltraSafe is not a discovered local candidate result.
- Strict benchmark dominance uses only complete candidates: CAGR higher is better, Sharpe higher is better, and max drawdown less negative is better.

## Score Frontier Points

| Rank | Candidate | Source | Mechanism | Class | Risk max DD | Return expectancy | Return score | N | Frontier |
|---:|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `ptc_backtest_final_469607b8340421b7` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720111 | 102 | yes |
| 2 | `ptc_backtest_final_624fdd85668e2c08` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718770 | 170 | yes |
| 3 | `ptc_backtest_final_8529139d60dcbbf6` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.710825 | 88 | yes |

## Expectancy Frontier Points

| Rank | Candidate | Source | Mechanism | Class | Risk max DD | Return expectancy | Return score | N | Frontier |
|---:|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `ptc_backtest_final_469607b8340421b7` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720111 | 102 | yes |
| 2 | `ptc_backtest_final_3a4ac24107c77136` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720108 | 102 | yes |
| 3 | `ptc_backtest_final_7d839944a8a4070a` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720011 | 102 | yes |
| 4 | `ptc_backtest_final_b23c6756bfb3263a` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.719438 | 102 | yes |
| 5 | `ptc_backtest_final_f74e86444d2d96de` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.719073 | 102 | yes |
| 6 | `ptc_backtest_final_624fdd85668e2c08` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718770 | 170 | yes |
| 7 | `ptc_backtest_final_b373e39b4e8f7af1` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.718422 | 102 | yes |
| 8 | `ptc_backtest_final_854ad10b904e1ae9` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718305 | 170 | yes |
| 9 | `ptc_backtest_final_0ca1c062615b416a` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.718285 | 102 | yes |
| 10 | `ptc_backtest_final_7e197a281aada275` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718041 | 170 | yes |
| 11 | `ptc_backtest_final_d68a56480f7d5eac` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.717513 | 170 | yes |
| 12 | `ptc_backtest_final_ad17bd73fcdf2a7d` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.717458 | 102 | yes |
| 13 | `ptc_backtest_final_7567e1ec1fce9888` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.716509 | 102 | yes |
| 14 | `ptc_backtest_final_32638f4eabddae6b` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.716440 | 102 | yes |
| 15 | `ptc_backtest_final_9e67522067aeb917` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.715961 | 170 | yes |
| 16 | `ptc_backtest_final_e6db1c5826c38663` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.715905 | 170 | yes |
| 17 | `ptc_backtest_final_18287c1f075e9230` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.715786 | 102 | yes |
| 18 | `ptc_backtest_final_1da3d53fcc9ad34b` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.713774 | 102 | yes |

## Top 50 Local Risk vs Return

| Rank | Candidate | Source | Mechanism | Class | Risk max DD | Return expectancy | Return score | N | Frontier |
|---:|---|---|---|---|---:|---:|---:|---:|---|
| 1 | `ptc_backtest_final_469607b8340421b7` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720111 | 102 | yes |
| 2 | `ptc_backtest_final_3a4ac24107c77136` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720108 | 102 | no |
| 3 | `ptc_backtest_final_7d839944a8a4070a` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.720011 | 102 | no |
| 4 | `ptc_backtest_final_b23c6756bfb3263a` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.719438 | 102 | no |
| 5 | `ptc_backtest_final_f74e86444d2d96de` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.719073 | 102 | no |
| 6 | `ptc_backtest_final_624fdd85668e2c08` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718770 | 170 | yes |
| 7 | `ptc_backtest_final_b373e39b4e8f7af1` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.718422 | 102 | no |
| 8 | `ptc_backtest_final_854ad10b904e1ae9` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718305 | 170 | no |
| 9 | `ptc_backtest_final_0ca1c062615b416a` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.718285 | 102 | no |
| 10 | `ptc_backtest_final_7e197a281aada275` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.718041 | 170 | no |
| 11 | `ptc_backtest_final_d68a56480f7d5eac` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.717513 | 170 | no |
| 12 | `ptc_backtest_final_ad17bd73fcdf2a7d` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.717458 | 102 | no |
| 13 | `ptc_backtest_final_7567e1ec1fce9888` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.716509 | 102 | no |
| 14 | `ptc_backtest_final_32638f4eabddae6b` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.716440 | 102 | no |
| 15 | `ptc_backtest_final_9e67522067aeb917` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.715961 | 170 | no |
| 16 | `ptc_backtest_final_e6db1c5826c38663` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | 0.362% | 0.715905 | 170 | no |
| 17 | `ptc_backtest_final_18287c1f075e9230` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.715786 | 102 | no |
| 18 | `ptc_backtest_final_34a365f30c889811` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | n/a | 0.715761 | 170 | no |
| 19 | `ptc_backtest_final_b0b94052c4989a3f` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | n/a | 0.715372 | 170 | no |
| 20 | `ptc_backtest_final_758c67480dc36952` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | n/a | 0.715291 | 170 | no |
| 21 | `ptc_backtest_final_c3b841c7293b26cc` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | n/a | 0.714289 | 170 | no |
| 22 | `ptc_backtest_final_d5931b24bd391113` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | 0.342% | 0.714075 | 96 | no |
| 23 | `ptc_backtest_final_05ce7ac3641b38b6` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.714050 | 96 | no |
| 24 | `ptc_backtest_final_ce33abb5d7de02cb` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.713976 | 96 | no |
| 25 | `ptc_backtest_final_1da3d53fcc9ad34b` | local_candidate_result | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.537% | 0.438% | 0.713774 | 102 | no |
| 26 | `ptc_backtest_final_b40c8aa04b4b60a9` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.712254 | 96 | no |
| 27 | `ptc_backtest_final_5e01b95ae069b764` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.712160 | 96 | no |
| 28 | `ptc_backtest_final_990d036cd869f982` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | n/a | 0.712154 | 170 | no |
| 29 | `ptc_backtest_final_4df2e8e80685a054` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | 0.113% | 0.712148 | 371 | no |
| 30 | `ptc_backtest_final_ff7345652b802294` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.712139 | 371 | no |
| 31 | `ptc_backtest_final_e962558456a60109` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.712072 | 371 | no |
| 32 | `ptc_backtest_final_9c1d0b63444423ff` | local_candidate_result | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 12.277% | n/a | 0.712056 | 170 | no |
| 33 | `ptc_backtest_final_c185b04196a63848` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.711997 | 371 | no |
| 34 | `ptc_backtest_final_651cd169dd508c4e` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.711859 | 371 | no |
| 35 | `ptc_backtest_final_8529139d60dcbbf6` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.710825 | 88 | yes |
| 36 | `ptc_backtest_final_a48c07e0392e11dc` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.710531 | 371 | no |
| 37 | `ptc_backtest_final_b14a4da3d5f9849e` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.710486 | 88 | no |
| 38 | `ptc_backtest_final_67346da117cdf47e` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.710314 | 96 | no |
| 39 | `ptc_backtest_final_8edabf7988a79611` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.710185 | 371 | no |
| 40 | `ptc_backtest_final_a377609849aa09e6` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.710173 | 88 | no |
| 41 | `ptc_backtest_final_bd2edd5aa289c971` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.710085 | 96 | no |
| 42 | `ptc_backtest_final_7dd42dae2e8077ec` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.710006 | 88 | no |
| 43 | `ptc_backtest_final_12e0e8c0c73cff12` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.709772 | 96 | no |
| 44 | `ptc_backtest_final_eb02cf745abcee8a` | local_candidate_result | REVERSAL | READY_FOR_PAPER_FORWARD_OBSERVATION | 16.007% | n/a | 0.709594 | 371 | no |
| 45 | `ptc_backtest_final_b3e1a31555e0118e` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.709448 | 96 | no |
| 46 | `ptc_backtest_final_71f8f33d0d8867e9` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.709024 | 88 | no |
| 47 | `ptc_backtest_final_ec85ecb1a09724ff` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.708953 | 96 | no |
| 48 | `ptc_backtest_final_b6f0b8673f4168f2` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.708862 | 88 | no |
| 49 | `ptc_backtest_final_4951b825751299d1` | local_candidate_result | EVENT_REACTION | READY_FOR_PAPER_FORWARD_OBSERVATION | 13.610% | n/a | 0.708789 | 96 | no |
| 50 | `ptc_backtest_final_292baf4e63bd7910` | local_candidate_result | LIQUIDITY_SWEEP | READY_FOR_PAPER_FORWARD_OBSERVATION | 11.579% | n/a | 0.708569 | 88 | no |
