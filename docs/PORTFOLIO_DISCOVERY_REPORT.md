# Portfolio Discovery Report

## Executive Summary

- Main question: Did we find anything that could complement UltraSafe? Yes, conditionally. Current evidence found paper-forward-observation candidates and several distinct families that could complement UltraSafe as research subjects, but it did not prove allocation usefulness or an UltraSafe replacement.
- This report answers complementarity, not whether anything beat UltraSafe.
- UltraSafe benchmark context: CAGR 19.00%, Sharpe 1.000000, max drawdown -36.00%; source `benchmark_override`.
- Local discovery evidence reviewed 50 top rows from final candidate ranking; 50 are paper-forward-observation candidates.
- Complete CAGR/Sharpe/max-drawdown benchmark rows in the top 50: 0; incomplete rows excluded from strict UltraSafe dominance comparison: 50.
- Runtime truth context: `BLOCKED` in `HUMAN_REVIEWED_PAPER_MODE`. This is read-only research reporting, not trade advice, allocation, position sizing, or paper-trade authorization.

## Best New Challenger

`ptc_backtest_final_469607b8340421b7` (BREAKOUT, READY_FOR_PAPER_FORWARD_OBSERVATION): score 0.720111, max drawdown -13.54%, expectancy 0.438%, sample 102.

Why it matters: this is the strongest currently ranked local candidate for further observation. It is a challenger in the research queue sense, not an UltraSafe replacement.

## Most Different Candidate

`ptc_backtest_final_8529139d60dcbbf6` (LIQUIDITY_SWEEP, READY_FOR_PAPER_FORWARD_OBSERVATION): score 0.710825, max drawdown -11.58%, expectancy n/a, sample 88.

Why it matters: complementarity needs differentiated behavior. This pick favors less common mechanism/classification exposure inside the current top local set.

## Best Risk Adjusted Candidate

`ptc_backtest_final_8529139d60dcbbf6` (LIQUIDITY_SWEEP, READY_FOR_PAPER_FORWARD_OBSERVATION): score 0.710825, max drawdown -11.58%, expectancy n/a, sample 88.

Selection rule: highest `final_score / abs(max_drawdown)` among rows with drawdown evidence. This is a screening proxy, not a Sharpe substitute.

## Failure Pattern Summary

- Aggregate disqualification reasons from candidate failure patterns:
- final_score below 0.7: 490
- backtest classification BACKTEST_WEAK: 216
- backtest classification INSUFFICIENT_DATA: 156
- Rejected-preview reason mix:
- final_score below 0.7; backtest classification BACKTEST_WEAK: 28
- final_score below 0.7; backtest classification INSUFFICIENT_DATA: 15
- final_score below 0.7: 13
- Memory failure warnings: 1 active/generated failure pattern row(s).

## Candidate Graveyard Summary

- Candidates evaluated: 600
- Excluded candidates in final ranking: 580
- Classification counts:
- REJECT_FOR_NOW: 490
- READY_FOR_PAPER_FORWARD_OBSERVATION: 58
- TOO_FRAGILE: 44
- NEEDS_DATA_IMPROVEMENT: 6
- TOO_PROXY_DEPENDENT: 2
- Retired knowledge rows: 0

## UltraSafe Similarity Findings

- Strict UltraSafe comparison is blocked by missing local CAGR/Sharpe fields for 50 top local row(s).
- Family discovery found 30 candidate families, 22 near-duplicate groups, and 8 genuinely distinct family bucket(s).
- Portfolio relevance estimate: Plausibly yes, but only conditionally: the base case has enough family diversity and positive haircut-adjusted expectancy proxy to justify further validation, not allocation.
- Primary overlap risk: MEDIUM_HIGH; independent family estimate: 3.
- Similarity conclusion: the useful signal is not that a local row beats UltraSafe; it is that some rows may offer differentiated mechanisms/families worth validating alongside UltraSafe.

## Recommended Next Tests

- Run direct candidate-data validation for the best representative of each high-priority family before any portfolio inference.
- Paper-forward observe one representative per distinct family; avoid spending review cycles on near duplicates until the representative survives.
- Add CAGR and Sharpe fields to local candidate outputs so strict UltraSafe benchmark comparison can move from incomplete to comparable.
- Measure cross-family correlation/overlap against UltraSafe-like behavior before calling anything complementary.
- Re-run failure-pattern attribution after direct-data replay to separate proxy-data failures from true mechanism failures.

## Top Candidate Evidence Snapshot

| Candidate | Mechanism | Classification | Score | Expectancy | Max DD | N |
|---|---|---|---:|---:|---:|---:|
| `ptc_backtest_final_469607b8340421b7` | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.720111 | 0.438% | -13.54% | 102 |
| `ptc_backtest_final_3a4ac24107c77136` | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.720108 | 0.438% | -13.54% | 102 |
| `ptc_backtest_final_7d839944a8a4070a` | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.720011 | 0.438% | -13.54% | 102 |
| `ptc_backtest_final_b23c6756bfb3263a` | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.719438 | 0.438% | -13.54% | 102 |
| `ptc_backtest_final_f74e86444d2d96de` | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.719073 | 0.438% | -13.54% | 102 |
| `ptc_backtest_final_624fdd85668e2c08` | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.718770 | 0.362% | -12.28% | 170 |
| `ptc_backtest_final_b373e39b4e8f7af1` | BREAKOUT | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.718422 | 0.438% | -13.54% | 102 |
| `ptc_backtest_final_854ad10b904e1ae9` | MEAN_REVERSION | READY_FOR_PAPER_FORWARD_OBSERVATION | 0.718305 | 0.362% | -12.28% | 170 |

## Sources

- Final candidate ranking: `reports/atlas_v2_research_os/final_candidate_ranking/2026-06-05/final_candidate_ranking_report.json`
- Candidate failure patterns: `reports/atlas_v2_research_os/candidate_failure_patterns/latest.json`
- Candidate family discovery: `reports/atlas_v2_research_os/candidate_family_discovery/latest.json`
- Portfolio relevance estimate: `reports/atlas_v2_research_os/portfolio_relevance_estimate/latest.json`
- Memory failure patterns: `reports/atlas_v2_research_os/memory/failure_patterns.json`
- Retired knowledge: `reports/atlas_v2_research_os/memory/retired_knowledge.json`
- Verified runtime graph: `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-10/verified_runtime_graph.v1.json`
- Prior efficient-frontier report path: `docs/EFFICIENT_FRONTIER.md`
