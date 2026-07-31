# Qualification Failure Analysis 001

Date: 2026-06-05

Status: analysis-only

Companion table: `research_journal/reports/qualification_failure_table.csv`

## Scope

Analyze why 490 candidates fail final qualification with final score below 0.7.

Inputs reviewed:

- `research_journal/reports/candidate_funnel_attrition_analysis_001.md`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest_summary.md`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest_summary.md`
- `reports/atlas_v2_research_os/edge_qualification_gate_audit/latest.json`
- `reports/atlas_v2_research_os/edge_qualification_gate_audit/latest_summary.md`
- `reports/atlas_v2_research_os/candidate_backtests/latest.json`
- `reports/atlas_v2_research_os/candidate_review/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/historical_replay/latest.json`
- `reports/atlas_v2_research_os/replay_sample_yield/latest.json`

## Authority Boundary

This report is diagnostic only.

No candidate promotion is made.
No qualification changes are made.
No replay changes are made.
No governance changes are made.
No trading recommendation, broker execution, capital allocation, position sizing, portfolio construction, or paper placement is authorized.

## Source Limits

The full population count is available: 600 candidates evaluated, 110 final eligible, and 490 classified `REJECT_FOR_NOW`.

The final ranking artifact materializes only a preview of 100 excluded candidates. Within that preview, 56 rows are `REJECT_FOR_NOW`. Therefore:

- full-population rows in the CSV use aggregate counts from qualification and ranking reports;
- candidate-level rows use the 56 materialized `REJECT_FOR_NOW` preview rows;
- overlapping suppressor counts are not additive.

## Qualification Baseline

| Measure | Value |
| --- | ---: |
| candidates evaluated | 600 |
| final eligible candidates | 110 |
| final score below 0.7 / `REJECT_FOR_NOW` | 490 |
| average final score | 0.664326 |
| median final score | 0.666771 |
| average preliminary edge score | 0.682930 |
| backtest-supported candidates | 228 |
| backtest-supported final passes | 110 |
| backtest-supported pass rate | 48.25% |

Threshold sensitivity:

| Threshold | All Candidates Passing | Backtest-Supported Passing |
| ---: | ---: | ---: |
| 0.70 | 110 | 110 |
| 0.65 | 409 | 228 |
| 0.60 | 600 | 228 |
| 0.55 | 600 | 228 |

The threshold sensitivity shows most failures are near-threshold rather than deeply broken: 299 additional candidates would pass at 0.65, and all 600 would pass at 0.60. This is not a recommendation to change thresholds; it only describes score distribution.

## Top 10 Qualification Failure Reasons

Counts are overlapping where noted.

| Rank | Failure Reason | Count | Fixable vs True Rejection | Interpretation |
| ---: | --- | ---: | --- | --- |
| 1 | final score below 0.7 | 490 | Mixed | The direct qualification gate removes the largest group. |
| 2 | proxy dependency | 600 | Fixable validation gap | Universal penalty; affects both failed and eligible candidates. |
| 3 | fragile drawdown | 348 | Mixed / candidate fragility | Large evidence-quality risk; some cases may be true rejection. |
| 4 | `BACKTEST_WEAK` classification | 216 | True rejection or major redesign | Weak empirical support is the largest explicit backtest weakness. |
| 5 | intraday/daily mismatch | 180 | Fixable validation gap | Mechanisms are often intraday but tested through daily proxy evidence. |
| 6 | `INSUFFICIENT_DATA` classification | 156 | Fixable validation gap | Not enough usable evidence to qualify or reject robustly. |
| 7 | sample-size penalty | 156 | Fixable validation gap | Same count as insufficient-data classification; likely overlapping. |
| 8 | missing evidence penalty | 156 | Fixable validation gap | Evidence maturity remains incomplete. |
| 9 | evidence maturity component suppression | 600 average-basis | Fixable validation/evidence gap | Largest average component suppression in the edge audit. |
| 10 | historical replay component suppression | 600 average-basis | Fixable validation/evidence gap | Replay-derived evidence is below the max contribution on average. |

## Score Component Findings

Backtest-aware qualification suppressors:

| Component / Penalty | Value |
| --- | ---: |
| average backtest evidence score | 0.396288 |
| average gross backtest credit | 0.031703 |
| average total penalty | 0.050307 |
| proxy penalty count | 600 |
| intraday/daily mismatch penalty count | 180 |
| sample-size penalty count | 156 |
| missing evidence penalty count | 156 |

Edge qualification audit component suppressions:

| Component | Average Contribution | Max Contribution | Average Suppression |
| --- | ---: | ---: | ---: |
| evidence_maturity | 0.110500 | 0.170000 | 0.059500 |
| historical_replay | 0.092331 | 0.150000 | 0.057669 |
| research_effectiveness | 0.118999 | 0.170000 | 0.051001 |
| hypothesis_survival | 0.085800 | 0.130000 | 0.044200 |
| candidate_quality_signal | 0.089700 | 0.130000 | 0.040300 |
| regime_coverage | 0.044000 | 0.080000 | 0.036000 |

The dominant score components are not single catastrophic zeros. They are cumulative moderate suppressions across evidence maturity, replay quality, research effectiveness, hypothesis survival, candidate quality, and regime coverage.

## Candidate-Level Preview

The materialized excluded-candidate preview contains 56 `REJECT_FOR_NOW` rows.

Preview failure reasons:

| Preview Reason | Count |
| --- | ---: |
| final_score below 0.7; backtest classification `BACKTEST_WEAK` | 28 |
| final_score below 0.7; backtest classification `INSUFFICIENT_DATA` | 15 |
| final_score below 0.7 | 13 |

Preview mechanisms among `REJECT_FOR_NOW` rows:

| Mechanism | Count |
| --- | ---: |
| VWAP_OR_AVERAGE_RECLAIM | 10 |
| MEAN_REVERSION | 8 |
| OPENING_RANGE | 8 |
| TREND_CONTINUATION | 7 |
| LIQUIDITY_SWEEP | 6 |
| SESSION_TIMING | 6 |
| BREAKOUT | 5 |
| EVENT_REACTION | 2 |
| REVERSAL | 2 |
| VOLATILITY_EXPANSION | 2 |

Preview regimes among `REJECT_FOR_NOW` rows:

| Regime | Count |
| --- | ---: |
| UNKNOWN | 13 |
| LOW_VOLATILITY | 12 |
| TRENDING | 11 |
| CHOP | 11 |
| HIGH_VOLATILITY | 9 |

Preview final score range:

- minimum: 0.607147
- maximum: 0.699542
- average: 0.655547

## Fixable vs True Rejection

Fixable or validation-limited reasons:

- proxy dependency;
- intraday/daily mismatch;
- insufficient data;
- low sample size;
- missing evidence;
- evidence maturity suppression;
- historical replay suppression;
- regime coverage weakness where caused by vocabulary or data mismatch.

Likely true rejection or major-redesign reasons:

- `BACKTEST_WEAK`;
- fragile drawdown;
- weak expectancy/profit factor/backtest consistency;
- repeated score weakness despite adequate sample size;
- candidate-quality suppression not explained by missing data.

Mixed reasons:

- final score below 0.7 by itself;
- research effectiveness suppression;
- hypothesis survival suppression;
- candidate quality signal suppression;
- regime fit weakness where the regime mismatch is semantic rather than data-driven.

## Poor Candidates vs Poor Validation

The failures do not point to a single explanation.

Evidence for poor validation or immature evidence:

- proxy penalty applies to all 600 evaluated candidates;
- 180 candidates carry intraday/daily mismatch penalty;
- 156 candidates carry sample-size and missing-evidence penalties;
- the largest edge-audit suppressor is evidence maturity;
- historical replay contributes less than its max contribution on average;
- direct validation work elsewhere shows daily proxy, regime vocabulary, and sample-survival problems.

Evidence for genuinely weak candidates:

- 216 candidates are explicitly `BACKTEST_WEAK`;
- 348 candidates have fragile drawdown weakness;
- the preview contains candidates with adequate sample sizes that still fail due to weak backtest classification or cumulative score weakness;
- final score weakness remains after adding backtest-aware credit for many candidates.

Overall classification:

- The 490 failures are a mix of poor candidates and poor validation.
- The largest fixable class is evidence-fidelity failure: proxy dependence, intraday/daily mismatch, missing evidence, and low sample size.
- The largest true-rejection class is weak backtest support.
- Near-threshold behavior indicates the qualification gate is suppressing many candidates for cumulative moderate weaknesses rather than one uniform fatal defect.

## Table Notes

The CSV includes:

- aggregate rows for the full 490-failure population;
- subgroup rows for `BACKTEST_WEAK`, `INSUFFICIENT_DATA`, low-score-only, proxy dependency, fragile drawdown, intraday/daily mismatch, low sample size, missing evidence, and component suppressions;
- candidate-level rows for the 56 materialized `REJECT_FOR_NOW` preview candidates.

Rows with `GROUP::` IDs represent candidate groups or overlapping suppressor groups, not individual candidates.
