# Candidate Survival Scorecard 001

Date: 2026-06-05
Status: ANALYSIS_ONLY

## Scope

Create the current scoreboard for Atlas anomaly discovery quality.

Inputs used:

- `reports/atlas_v2_research_os/observation_cluster_split_experiment/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_v2_research_os/direct_candidate_data_validation/latest.json`
- `reports/atlas_v2_research_os/paper_forward_observation/latest.json`
- `reports/atlas_v2_research_os/paper_forward_outcomes/latest.json`
- `reports/atlas_v2_research_os/candidate_funnel_observatory_001.md`
- `research_journal/reports/candidate_funnel_attrition_analysis_001.md`

This report makes no implementation changes, does not modify candidate state, does not rerun replay, does not alter validation, does not change qualification, does not change governance, does not recommend trades, does not allocate capital, does not size positions, does not authorize broker execution, and does not place paper trades.

## Scoreboard

| Metric | Current count | Source | Interpretation |
| --- | ---: | --- | --- |
| Observations | 100000 | observation cluster split experiment | Broad post-split source observation universe. |
| Claims | 600 | observation cluster split experiment | Claims after splitting over-merged observation clusters. |
| Hypotheses | 600 | observation cluster split experiment | Hypotheses generated from post-split claims. |
| Candidates | 600 | backtest-aware final qualification | Candidate-like hypotheses evaluated in the qualification universe. |
| Qualified candidates | 110 | backtest-aware final qualification | Final eligible candidates at the 0.70 threshold. |
| Paper-forward candidates | 58 | final candidate ranking | Candidates classified `READY_FOR_PAPER_FORWARD_OBSERVATION`. |
| Paper-forward observation plans | 12 | paper-forward observation plan | Human-review-only paper-forward observation plans created. |
| Confirmed candidates | 2 | direct candidate data validation | Direct validation `CONFIRMED`. |
| Weakened candidates | 0 | direct candidate data validation | Direct validation `WEAKENED`. |
| Insufficient data | 6 | direct candidate data validation | Direct validation `INSUFFICIENT_DATA`. |
| Paper-forward survivors | 1 | paper-forward outcomes | Recorded outcome with status `SURVIVED`. |
| Paper-forward weakened outcomes | 1 | paper-forward outcomes | Recorded outcome with status `WEAKENED`. |

## Quality Ratios

| Ratio | Value | Reading |
| --- | ---: | --- |
| Claims / observations | 0.6% | Heavy compression from observations to claims. |
| Hypotheses / claims | 100.0% | Every post-split claim becomes a hypothesis. |
| Candidates / hypotheses | 100.0% | Every post-split hypothesis enters candidate-style evaluation. |
| Qualified / candidates | 18.3% | 110 of 600 clear final qualification. |
| Paper-forward-ready / candidates | 9.7% | 58 of 600 are ready for human-reviewed paper-forward observation. |
| Paper-forward plans / paper-forward-ready | 20.7% | 12 of 58 have observation plans. |
| Direct confirmed / focused direct validation | 25.0% | 2 of 8 focused candidates confirm under direct validation. |
| Direct insufficient-data / focused direct validation | 75.0% | 6 of 8 focused candidates remain insufficient-data. |
| Paper-forward survivors / recorded paper-forward outcomes | 50.0% | 1 of 2 recorded outcomes survived. |

## Candidate Survival Readout

Direct validation survival:

- reviewed: 8
- confirmed: 2
- weakened: 0
- insufficient data: 6
- confirmation rate: 25.0%
- direct-validation insufficiency rate: 75.0%

Paper-forward outcome survival:

- outcomes recorded: 2
- survived: 1
- weakened: 1
- falsified: 0
- needs more data: 0
- survivor rate among recorded outcomes: 50.0%

Important scope caveat:

The paper-forward outcomes are recorded demo outcome artifacts, while the 58 paper-forward-ready candidates and 12 paper-forward observation plans are broader current planning/readiness surfaces. Do not interpret the 1 survivor as 1 of 58 or 1 of 12 unless future reports explicitly link those exact planned candidates to recorded outcomes.

## Evidence Quality Signals

Positive signals:

- The split experiment increased breadth from 50 pre-split claims/hypotheses/replays to 600 post-split claims/hypotheses/replays.
- Backtest-supported candidates increased from 19 pre-split to 228 post-split.
- Final qualification produced 110 eligible candidates from 600 evaluated candidates.
- Final ranking found 58 candidates ready for human-reviewed paper-forward observation.
- Focused direct validation produced 2 confirmed candidates.
- Paper-forward outcomes include 1 survived outcome.

Negative or unresolved signals:

- Observation volume remains far larger than durable candidate evidence.
- 490 of 600 candidates are disqualified by final score below 0.7.
- All 600 evaluated candidates carry proxy penalty exposure.
- 180 candidates carry intraday/daily mismatch penalties.
- 156 candidates carry sample-size or insufficient-data penalties.
- Direct validation fails 6 of 8 focused candidates as `INSUFFICIENT_DATA`.
- Paper-forward survival evidence is tiny: 2 recorded outcomes total.
- Current paper-forward survivor evidence is not yet linked to the main 58 ready candidates as a mature outcome set.

## Volume Versus Quality

Atlas is clearly producing volume:

- 100000 observations
- 600 claims
- 600 hypotheses
- 600 candidate-style evaluations

Atlas is also improving some intermediate quality gates:

- the post-split pipeline preserves more diversity than the 50-claim pre-split compression
- 228 candidates are backtest-supported
- 110 candidates pass final qualification
- 58 candidates reach paper-forward-observation readiness

But the strongest quality evidence is still limited:

- only 8 focused candidates reached direct validation
- only 2 were confirmed
- 6 remained insufficient-data
- only 2 paper-forward outcomes are recorded
- only 1 paper-forward outcome survived

## Answer

Atlas is improving anomaly quality at the intermediate funnel stages, but the current evidence is not strong enough to say it is improving durable anomaly survival.

The strongest improvement is not raw volume alone: splitting over-merged clusters increased candidate breadth and produced 110 qualified candidates plus 58 paper-forward-ready candidates. That is a real quality improvement over a pure volume generator.

However, final survival evidence remains thin. The quality claim narrows sharply at direct validation and paper-forward outcome stages: only 2 direct confirmations and 1 recorded paper-forward survivor exist, while 6 focused candidates are still insufficient-data. Therefore the current conclusion is:

```text
Atlas is improving anomaly filtering and candidate selection, but survival-quality proof remains early and underpowered.
It is not merely producing volume, but it has not yet proven durable anomaly quality at scale.
```

## Next Scorecard Needs

Future scorecards should separate:

- paper-forward-ready candidates
- paper-forward observation plans
- paper-forward observations started
- paper-forward outcomes recorded
- survivors linked to current candidate ids
- weakened outcomes linked to current candidate ids
- direct confirmations with candidate-specific data
- direct confirmations still dependent on daily proxy approximations

The next useful quality milestone is not more observations. It is more linked direct-validation confirmations and more non-demo paper-forward outcomes.

## Authority Boundary

This scorecard is analysis only. It does not implement software, modify Atlas or Aegis behavior, modify manifests, alter verified runtime truth, promote candidates, reject candidates, override replay, override qualification, override governance, recommend trades, allocate capital, size positions, authorize broker execution, write memory automatically, or place paper trades.
