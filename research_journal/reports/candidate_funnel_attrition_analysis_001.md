# Candidate Funnel Attrition Analysis 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

## Scope

Determine where Atlas loses the most value in the current candidate funnel using `reports/atlas_v2_research_os/candidate_funnel_observatory_001.md`.

This report measures:

- stage attrition
- cumulative attrition
- bottleneck attribution
- failure attribution
- top 5 funnel losses

This report is diagnostic only. It does not change workflow, candidate ranking, replay, validation, qualification, governance, paper-forward state, trading, capital, or broker behavior.

## Source Baseline

Current funnel basis:

| Stage | Count |
| --- | ---: |
| Observations | 100000 |
| Claims | 600 |
| Hypotheses | 600 |
| Replays | 600 |
| Qualified | 110 |
| Focused candidates | 8 |
| Confirmed | 2 |
| Direct validation failures | 6 |
| Blocked backlog items | 13 |

Qualification detail:

| Measure | Count |
| --- | ---: |
| Candidates evaluated | 600 |
| Backtest-supported candidates | 228 |
| Final eligible candidates | 110 |
| Backtest classification `BACKTEST_WEAK` | 216 |
| Backtest classification `INSUFFICIENT_DATA` | 156 |
| Final score below 0.7 | 490 |
| Proxy penalty count | 600 |
| Intraday/daily mismatch penalty count | 180 |
| Sample-size penalty count | 156 |

Direct validation detail:

| Measure | Count |
| --- | ---: |
| Candidates reviewed | 8 |
| Direct replays run | 8 |
| Confirmed | 2 |
| Weakened | 0 |
| Insufficient data | 6 |
| Direct validation failure rate | 75.0% |

Throughput blockers:

| Blocked type | Count |
| --- | ---: |
| `REGIME_GAP` | 5 |
| `EDGE_QUALIFICATION_REVIEW` | 3 |
| `CLAIM_INVESTIGATION` | 2 |
| `FAILURE_ANALYSIS` | 2 |
| `DUPLICATE_REVIEW` | 1 |

## Stage Attrition

| Transition | From | To | Removed | Retained | Attrition |
| --- | ---: | ---: | ---: | ---: | ---: |
| Observations to Claims | 100000 | 600 | 99400 | 0.6% | 99.4% |
| Claims to Hypotheses | 600 | 600 | 0 | 100.0% | 0.0% |
| Hypotheses to Replays | 600 | 600 | 0 | 100.0% | 0.0% |
| Replays to Qualified | 600 | 110 | 490 | 18.3% | 81.7% |
| Qualified to Focused Candidates | 110 | 8 | 102 | 7.3% | 92.7% |
| Focused Candidates to Confirmed | 8 | 2 | 6 | 25.0% | 75.0% |

## Cumulative Attrition

| Stage | Count | Cumulative Retained From Observations | Cumulative Attrition From Observations |
| --- | ---: | ---: | ---: |
| Observations | 100000 | 100.000% | 0.000% |
| Claims | 600 | 0.600% | 99.400% |
| Hypotheses | 600 | 0.600% | 99.400% |
| Replays | 600 | 0.600% | 99.400% |
| Qualified | 110 | 0.110% | 99.890% |
| Focused candidates | 8 | 0.008% | 99.992% |
| Confirmed | 2 | 0.002% | 99.998% |

Candidate-stage cumulative view, using replayed hypotheses as the candidate-evaluation denominator:

| Stage | Count | Cumulative Retained From Replays | Cumulative Attrition From Replays |
| --- | ---: | ---: | ---: |
| Replays | 600 | 100.0% | 0.0% |
| Qualified | 110 | 18.3% | 81.7% |
| Focused candidates | 8 | 1.3% | 98.7% |
| Confirmed | 2 | 0.3% | 99.7% |

## Bottleneck Attribution

### Primary Absolute Funnel Bottleneck

Primary absolute funnel bottleneck: Observations to Claims.

It removes 99400 upstream observations.

Interpretation:

This is the largest count loss, but it is partly intentional compression. The observatory notes that the post-split flow improved from 50 to 600 claims, so the current loss is not automatically bad. The value risk is over-compression: useful observation diversity can disappear before hypothesis or replay.

### Primary Candidate Bottleneck

Primary candidate bottleneck: Replays to Qualified.

It removes 490 of 600 replayed hypotheses before final eligibility.

Interpretation:

This is the biggest candidate-stage removal by absolute count. It is also where the qualification suppressors concentrate: final score below 0.7, weak backtests, insufficient data, proxy penalties, intraday/daily mismatch, and sample-size penalties.

### Primary Focused-Candidate Bottleneck

Primary focused-candidate bottleneck: Focused Candidates to Confirmed.

It removes 6 of 8 focused candidates through direct validation failure.

Interpretation:

The count is smaller than qualification attrition, but the value density is high because these are already selected campaign candidates. The loss is dominated by direct validation `INSUFFICIENT_DATA`.

### Primary Throughput Bottleneck

Primary throughput bottleneck: `REGIME_GAP`.

It accounts for 5 of 13 blocked backlog items.

Interpretation:

Among explicit blocker types, `REGIME_GAP` removes or blocks the most current work. This aligns with the recent CHOP/RANGE_BOUND vocabulary and replay attrition findings.

## Failure Attribution

### Qualification Failure Attribution

Qualification suppressor counts are overlapping and should not be summed.

| Failure / Suppressor | Count | Attribution |
| --- | ---: | --- |
| final score below 0.7 | 490 | Largest explicit qualification suppressor; matches the 490-candidate Replays to Qualified loss. |
| proxy penalty count | 600 | Universal evidence-quality penalty, not a standalone removal count. |
| `BACKTEST_WEAK` | 216 | Large backtest evidence weakness group. |
| intraday/daily mismatch penalty | 180 | Mechanism/timeframe quality loss; likely contributes to weak or insufficient validation. |
| `INSUFFICIENT_DATA` backtest classification | 156 | Data sufficiency loss before or during qualification. |
| sample-size penalty | 156 | Same count as insufficient-data classification; likely overlapping. |

Single qualification suppressor removing the most candidates:

- final score below 0.7: 490 candidates

### Direct Validation Failure Attribution

| Failure | Count | Attribution |
| --- | ---: | --- |
| `INSUFFICIENT_DATA` | 6 | All direct validation failures. |
| `WEAKENED` | 0 | No current direct validation weakened candidates. |

Single direct-validation failure removing the most focused candidates:

- `INSUFFICIENT_DATA`: 6 candidates

### Throughput Failure Attribution

| Failure / Blocker | Count | Attribution |
| --- | ---: | --- |
| `REGIME_GAP` | 5 | Largest current blocked-backlog type. |
| `EDGE_QUALIFICATION_REVIEW` | 3 | Second-largest blocker. |
| `CLAIM_INVESTIGATION` | 2 | Lower-count research blocker. |
| `FAILURE_ANALYSIS` | 2 | Lower-count research blocker. |
| `DUPLICATE_REVIEW` | 1 | Smallest current blocker. |

Single throughput blocker removing the most blocked items:

- `REGIME_GAP`: 5 blocked items

## Direct Answers

### What Single Stage Removes The Most Candidates?

If using the full upstream funnel, the largest stage loss is:

- Observations to Claims: 99400 removed

If using the candidate-evaluation funnel, the largest candidate-stage loss is:

- Replays to Qualified: 490 removed

The most important candidate-stage answer is `Replays to Qualified`, because it removes the most evaluated candidate/hypothesis objects before eligibility.

### What Single Bottleneck Removes The Most Candidates?

The single bottleneck/suppressor removing the most candidates is:

- final score below 0.7: 490 candidates

For current operational blockers, the largest explicit blocker is:

- `REGIME_GAP`: 5 blocked backlog items

For focused direct validation, the largest failure is:

- `INSUFFICIENT_DATA`: 6 of 8 focused candidates

## Top 5 Funnel Losses

| Rank | Loss | Count Removed / Affected | Layer | Why It Matters |
| ---: | --- | ---: | --- | --- |
| 1 | Observations to Claims compression | 99400 removed | Upstream idea compression | Largest absolute loss; may be intended compression, but over-compression can erase useful diversity. |
| 2 | Replays to Qualified attrition / final score below 0.7 | 490 removed | Candidate qualification | Largest candidate-stage loss and largest explicit qualification suppressor. |
| 3 | `BACKTEST_WEAK` classification | 216 affected | Backtest evidence quality | Major evidence weakness before eligibility; likely contributes to final-score attrition. |
| 4 | Intraday/daily mismatch penalty | 180 affected | Replay/evidence fidelity | Large mechanism-timeframe mismatch that can distort replay support and block confidence. |
| 5 | Qualified to Focused Candidates narrowing | 102 removed | Campaign selection | Removes most already-qualified candidates from focused direct validation. |

Notable near-top losses:

- `INSUFFICIENT_DATA` / sample-size penalty: 156 affected in qualification.
- Focused Candidates to Confirmed: 6 removed, but high value density because they are already selected campaign candidates.
- `REGIME_GAP`: 5 blocked backlog items, largest explicit throughput blocker.

## Conclusion

Atlas loses the most absolute volume at `Observations to Claims`, but the most important candidate-value loss is at `Replays to Qualified`.

The strongest single candidate bottleneck is the final qualification threshold: `final score below 0.7`, removing 490 of 600 replayed hypotheses.

The strongest current focused-validation failure is `INSUFFICIENT_DATA`, removing 6 of 8 focused candidates.

The strongest current throughput blocker is `REGIME_GAP`, blocking 5 backlog items.

Recommended interpretation:

- Treat observation compression as a diversity-risk monitor.
- Treat qualification attrition as the largest candidate-value loss.
- Treat direct validation insufficiency and regime gaps as the highest-density near-term evidence bottlenecks.

## Authority Boundary

This report is analysis-only. It does not implement monitoring, modify Atlas or Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, or place paper trades.
