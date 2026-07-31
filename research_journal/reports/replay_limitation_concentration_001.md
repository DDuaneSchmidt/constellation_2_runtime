# Replay Limitation Concentration 001

Date: 2026-06-05

Status: ANALYSIS_ONLY

## Scope

Determine whether a few replay issues cause most blocked candidates.

This report measures:

- top replay limitation
- top 3 replay limitations
- candidate impact
- validation impact
- Pareto concentration

This report does not change replay behavior, validation, qualification, candidate state, governance, paper-forward state, trading, capital, broker execution, or position sizing.

## Inputs Reviewed

- `research_journal/reports/insufficient_data_root_cause_analysis_001.md`
- `research_journal/reports/replay_attrition_remediation_plan_001.md`
- `research_journal/reports/candidate_funnel_attrition_analysis_001.md`
- `research_journal/reports/vocabulary_bridge_effectiveness_001.md`
- `research_journal/reports/qualification_failure_analysis_001.md`
- `research_journal/reports/qualification_failure_table.csv`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/direct_validation_refresh/2026-06-05-daily-completion/validation_delta_report.md`

## Current Replay-Validation State

Focused validation state:

| Metric | Count |
| --- | ---: |
| direct validation candidates reviewed | 8 |
| confirmed | 2 |
| direct validation failures / insufficient data | 6 |
| remaining insufficient-data cases in root-cause report | 7 |
| direct validation failure rate in current funnel | 75.0% |

The source reports use slightly different snapshots:

- `insufficient_data_root_cause_analysis_001.md` analyzes 7 remaining `INSUFFICIENT_DATA` cases after staged data acquisition.
- `candidate_funnel_attrition_analysis_001.md` reports the current direct validation funnel as 8 reviewed, 2 confirmed, and 6 insufficient-data failures.

This report treats the 7-case root-cause set as the detailed replay-limitation denominator and uses the 8-candidate funnel for validation impact.

## Replay Limitation Counts

Counts are overlapping: one candidate can carry multiple limitations.

| Replay limitation | Affected candidates | Share of 7-case root-cause set | Candidate impact | Validation impact |
| --- | ---: | ---: | --- | --- |
| daily-vs-intraday timeframe mismatch | 7 | 100.0% | Touches every remaining insufficient-data case. | Daily proxy replay cannot fully validate attributed 5m, 15m, 30m, or 1h mechanisms. |
| universe aggregation issue | 5 | 71.4% | Candidate evidence may be based on one symbol while candidate thesis spans a universe. | Blocks reliable candidate-level interpretation even when a symbol-level result exists. |
| insufficient sample size after trigger/regime filtering | 4 | 57.1% | Four candidates produce zero usable post-filter samples despite trigger activity. | Keeps direct validation in `INSUFFICIENT_DATA`; prevents confirmed/weakened classification. |
| replay logic limitation | 4 | 57.1% | Four candidates need trigger/regime replay instrumentation or rewrite. | Current replay cannot distinguish true candidate failure from proxy/filter attrition. |
| missing symbol coverage | 3 | 42.9% | Three candidates still need daily files before first-pass direct replay or full-universe replay. | Prevents direct replay from running or prevents partial evidence from graduating. |
| missing event metadata | 1 | 14.3% | One event-reaction candidate cannot be evaluated by price bars alone. | Blocks event-window validation and keeps daily replay only contextual. |

Qualification-scale replay/evidence suppressors:

| Suppressor | Count | Interpretation |
| --- | ---: | --- |
| proxy penalty | 600 | Universal evidence-fidelity weakness across evaluated candidates. |
| intraday/daily mismatch penalty | 180 | Large replay-fidelity issue outside the focused 8-candidate set. |
| sample-size penalty | 156 | Overlaps with insufficient-data classification. |
| backtest `INSUFFICIENT_DATA` | 156 | Not enough usable replay/backtest evidence for qualification. |

## Top Replay Limitation

Top replay limitation: daily-vs-intraday timeframe mismatch.

Candidate impact:

- affects 7 of 7 detailed remaining insufficient-data cases
- affects every currently blocked candidate in the root-cause set
- also appears at qualification scale as 180 intraday/daily mismatch penalties

Validation impact:

- daily bars can establish basic direct symbol coverage
- daily bars cannot validate the attributed intraday mechanisms
- this prevents replay evidence from cleanly supporting or rejecting candidates

Interpretation:

This is the broadest replay limitation. It is a necessary fix for the remaining blocked set, but it is not sufficient by itself because candidates also carry universe aggregation, sample attrition, missing symbol, regime vocabulary, and event metadata issues.

## Top 3 Replay Limitations

Top 3 by candidate reach:

| Rank | Limitation | Affected candidates | Notes |
| ---: | --- | ---: | --- |
| 1 | daily-vs-intraday timeframe mismatch | 7 | Broadest candidate reach and largest validation-fidelity problem. |
| 2 | universe aggregation issue | 5 | Prevents one-symbol replay from standing in for multi-symbol candidate evidence. |
| 3 | insufficient sample size after trigger/regime filtering | 4 | Directly blocks validation status by producing zero or too few post-filter samples. |

Alternate third place:

- `replay logic limitation` also affects 4 candidates and is tightly coupled to trigger/regime filtering.

Practical top 3 remediation cluster:

1. use matched intraday bars for intraday mechanisms
2. add per-symbol replay rows plus explicit aggregation policy
3. add trigger/regime attrition ledger before loosening filters

## Candidate Impact

The detailed 7-case set is highly concentrated by candidate reach:

| Candidate | Daily-vs-intraday | Universe aggregation | Trigger/regime sample attrition | Replay logic limitation | Missing symbols | Event metadata |
| --- | --- | --- | --- | --- | --- | --- |
| `ptc_backtest_final_469607b8340421b7` | yes | yes | yes | yes | no | no |
| `ptc_backtest_final_3a4ac24107c77136` | yes | yes | yes | yes | no | no |
| `ptc_backtest_final_624fdd85668e2c08` | yes | no | no | no | yes | no |
| `ptc_backtest_final_d5931b24bd391113` | yes | yes | yes | yes | no | yes |
| `ptc_backtest_final_4df2e8e80685a054` | yes | yes | no | no | yes | no |
| `ptc_backtest_final_7d839944a8a4070a` | yes | no | no | no | yes | no |
| `ptc_backtest_final_b23c6756bfb3263a` | yes | yes | yes | yes | no | no |

Candidate-level conclusion:

- fixing daily-vs-intraday mismatch touches 100% of detailed blocked candidates
- fixing the top 3 limitations touches all 7 candidates and directly addresses the zero-sample cluster
- however, fixing those top 3 does not fully unblock all 7, because missing symbol coverage and event metadata remain separate blockers

## Validation Impact

Focused validation:

- current direct validation reports 6 of 8 candidates still insufficient-data in the current funnel
- the root-cause set shows 7 remaining insufficient-data cases in its snapshot
- four candidates with full daily coverage still fail because daily proxy replay produces zero usable post-filter samples or lacks event timing

Bridge impact:

- the `CHOP -> RANGE_BOUND` diagnostic bridge recovers 396 samples for 5 affected CHOP candidates
- 3 candidates become evaluable under the diagnostic simulation
- 2 become simulated `CONFIRMED`
- 3 remain blocked

This means replay limitations are not evenly distributed. A vocabulary/regime filter issue has high leverage for a subset, but it does not solve intraday, event, or universe aggregation limitations.

Qualification-scale impact:

- 600 evaluated candidates carry proxy penalty
- 180 carry intraday/daily mismatch penalty
- 156 carry sample-size penalty
- 156 are `INSUFFICIENT_DATA`

The qualification-scale view supports the same conclusion: replay/evidence limitations are broad, but the causes overlap rather than forming one independent blocker.

## Pareto Analysis

### Candidate-Reach Pareto

Using the 7-case root-cause set:

| Cause rank | Limitation | Affected candidates | Cumulative candidate reach |
| ---: | --- | ---: | ---: |
| 1 | daily-vs-intraday mismatch | 7 | 100.0% |
| 2 | universe aggregation issue | 5 | 100.0% |
| 3 | trigger/regime sample attrition | 4 | 100.0% |

By candidate reach, the first cause touches all blocked candidates. This looks Pareto-like, but it overstates removability because touching a candidate is not the same as removing all limitations for that candidate.

### Limitation-Instance Pareto

Using overlapping limitation instances from the root-cause counts:

| Rank | Limitation | Count | Cumulative Count | Cumulative Share |
| ---: | --- | ---: | ---: | ---: |
| 1 | daily-vs-intraday mismatch | 7 | 7 | 29.2% |
| 2 | universe aggregation issue | 5 | 12 | 50.0% |
| 3 | insufficient sample size after trigger/regime filtering | 4 | 16 | 66.7% |
| 4 | replay logic limitation | 4 | 20 | 83.3% |
| 5 | missing symbol coverage | 3 | 23 | 95.8% |
| 6 | missing event metadata | 1 | 24 | 100.0% |

There are 24 counted limitation instances across 6 replay/evidence causes.

20% of causes is approximately 1 to 2 causes:

- top 1 cause removes or addresses 29.2% of limitation instances
- top 2 causes remove or address 50.0% of limitation instances

To reach 80% of limitation instances, the top 4 causes are required:

- daily-vs-intraday mismatch
- universe aggregation issue
- insufficient sample size after trigger/regime filtering
- replay logic limitation

That is 4 of 6 causes, or 66.7% of the cause set.

## Answer

Can 80% of replay limitations be removed by fixing 20% of causes?

Answer: No, not under a strict limitation-instance Pareto test.

Reason:

- the top 20% of causes covers only about 29% to 50% of counted limitation instances, depending on whether one or two causes are allowed
- 80% coverage requires the top 4 of 6 causes
- the limitations overlap, so fixing one broad cause may touch many candidates without fully unblocking them

Qualified yes:

- if the question is candidate reach rather than limitation removal, the top cause touches 100% of the detailed blocked set
- if the goal is near-term triage, the top 3 remediation cluster is still highly valuable

Practical conclusion:

Atlas has replay limitation concentration, but not a clean 80/20 removable pattern.

## Recommended Focus

Highest-leverage replay remediation cluster:

1. matched intraday evidence for intraday mechanisms
2. per-symbol replay rows plus explicit universe aggregation
3. trigger/regime attrition ledger with precise insufficiency reason codes
4. diagnostic-only `CHOP -> RANGE_BOUND` bridge for affected CHOP candidates

Do not treat any one fix as sufficient. The evidence says replay failures are concentrated but layered.

## Authority Boundary

This report is analysis-only. It does not implement remediation, acquire data, change replay logic, relax filters, change validation classifications, change candidate state, alter qualification, alter governance, create paper positions, recommend trades, allocate capital, authorize broker execution, size positions, or modify production workflows.
