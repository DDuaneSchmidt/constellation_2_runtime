# Atlas Failure Pattern Analysis 001

Date: 2026-06-05
Status: Analysis only

Scope: mines recurring patterns across historical Atlas failures, failure registry entries, and historical validation reports. This report does not modify implementation, original failure records, replay state, qualification state, governance state, candidate state, paper-forward state, broker execution, live trading, position sizing, or capital allocation.

## Inputs

- `research_journal/failures/FAIL_0004.yaml` through `research_journal/failures/FAIL_0016.yaml`
- `reports/atlas_v2_research_os/failures/failure_registry.jsonl`
- `reports/atlas_v2_research_os/failures/failure_index.json`
- `reports/atlas_v2_research_os/certification/latest.json`
- `reports/atlas_v2_research_os/certification/latest_summary.md`
- `reports/atlas_v2_research_os/learning_validation/latest_summary.md`
- `reports/atlas_v2_research_os/candidate_review/latest.json`
- `reports/atlas_v2_research_os/candidate_review/latest_summary.md`
- `reports/atlas_v2_research_os/candidate_backtests/latest.json`
- `reports/atlas_v2_research_os/candidate_backtests/latest_summary.md`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest.json`
- `reports/atlas_v2_research_os/backtest_aware_final_qualification/latest_summary.md`
- `reports/atlas_v2_research_os/final_candidate_ranking/latest.json`
- `reports/atlas_candidate_quality/latest_summary.md`

## Executive Summary

Atlas failures cluster around one repeated theme: research artifacts often look more mature than the evidence, data, or authority boundary underneath them.

The dominant recurring mechanisms are:

- authority boundary leakage markers in generated reports
- proxy evidence standing in for candidate-specific evidence
- replay or paper workflow progress being mistaken for survival evidence
- weak regime or mechanism specificity
- insufficient data, missing artifacts, and worker compatibility gaps
- candidate quality being inferred from volume, architecture, or audit cleanliness

Candidate survival is most affected by evidence and data constraints. In the final qualification report, 600 of 600 candidates carried a proxy penalty, 180 carried intraday/daily mismatch penalties, 156 carried sample-size penalties, and only 110 of 228 backtest-supported candidates passed final qualification.

## Metrics

### Failure Frequency

Source counts:

- Journal failures reviewed: 13
- Failure registry entries reviewed: 17
- Registry unresolved failures: 17
- Candidate final-qualification candidates evaluated: 600
- Backtest-supported candidates: 228
- Final eligible candidates: 110

Failure registry frequency:

| Failure Type | Count | Share Of Registry | Notes |
| --- | ---: | ---: | --- |
| `CERTIFICATION_BLOCK` | 7 | 41.2% | Repeated certification failure with blocker counts 26, 34, 40, 20, 3, 3, 3. |
| `SAFETY_GATE_FAILED` | 4 | 23.5% | Autonomous research failed closed after safety checks. |
| `NO_COMPATIBLE_CONNECTED_WORKER` | 3 | 17.6% | Backlog item selected without a compatible connected worker. |
| `ArtifactStoreError` | 3 | 17.6% | Missing source artifacts broke autonomous research execution. |

Validation and candidate-survival frequency:

| Pattern | Count | Denominator | Notes |
| --- | ---: | ---: | --- |
| Proxy penalty | 600 | 600 candidates | Every final-qualification candidate depended on proxy evidence. |
| Final score below 0.7 | 490 | 600 candidates | Main final disqualification reason. |
| Backtest weak | 216 | 600 candidates | Backtest classification blocked or weakened many candidates. |
| Insufficient data | 156 | 600 candidates | Backtest classification or sample-size limitation. |
| Sample-size penalty | 156 | 600 candidates | Same count as insufficient-data class. |
| Intraday/daily mismatch penalty | 180 | 600 candidates | Daily proxy data used against intraday mechanisms. |
| Backtest-supported final passes | 110 | 228 supported candidates | 48.2% pass rate after final penalties. |
| Candidate review rejection reason: edge below 0.7 | 88 | 100 candidate-review pool | Reported common rejection reason. |

Authority marker frequency in the latest certification blockers:

| Authority Marker | Count |
| --- | ---: |
| position sizing | 14 |
| broker execution | 8 |
| capital allocation | 6 |
| sleeve deployment | 4 |
| trade recommendation | 2 |

### Failure Co-Occurrence

Recurring co-occurrence clusters:

| Co-Occurring Pattern | Evidence | Interpretation |
| --- | --- | --- |
| Certification block + authority markers | 7 registry certification failures; latest certification has 34 blockers, all from authority boundary marker detection. | Governance stays blocked when report artifacts contain forbidden authority language, even if many structural checks pass. |
| Safety gate failure + certification failure | `SAFETY_GATE_FAILED` entries carry `certification_status=FAIL`, `governance_status=PASS`, `lineage_status=PASS`. | Passing lineage and governance does not compensate for certification failure. |
| Missing artifact + autonomous research execution | 3 `ArtifactStoreError` entries during autonomous research. | Backlog and priority paths assumed artifacts existed before loading or influence checks. |
| Proxy evidence + intraday mismatch | 600 proxy penalties and 180 intraday/daily mismatch penalties. | Candidate evidence is weakened when daily SPY proxy data is used for intraday mechanisms. |
| Replay support + paper-forward uncertainty | Candidate review and backtest reports repeatedly state replay is not paper-forward evidence. | Historical replay can support observation, but not prove forward survival. |
| Mechanism duplication + regime weakness | Candidate review flags mechanism clusters and unknown or weak regime labels. | Candidate families may double-count the same structural idea across regimes. |
| Candidate throughput + certification/evidence blockers | FAIL_0007 and FAIL_0014 link volume and sleeve output to certification, portfolio, data, and conversion blockers. | Raw count increases do not imply candidate quality or survival. |

### Recurring Assumptions

| Recurring Assumption | Seen In | Failure Pattern |
| --- | --- | --- |
| Architecture maturity implies evidence maturity. | `FAIL_0004` | Evidence lagged architecture readiness. |
| Paper workflow progress implies runtime readiness. | `FAIL_0005` | Paper artifacts and authority gates answer different questions. |
| Observation accumulation implies outcome evidence. | `FAIL_0006` | Open observations accumulated faster than closed outcomes. |
| Candidate volume implies candidate quality. | `FAIL_0007` | Volume ignored governance and validation blockers. |
| Broad technical support implies relationship-specific signal quality. | `FAIL_0008`, `FAIL_0016` | Technical claims remained false-positive-prone or non-generalizing. |
| Active sleeve status implies candidate flow. | `FAIL_0009`, `FAIL_0014` | Sleeves can be healthy no-signal, data-blocked, or conversion-blocked. |
| Rich research artifacts imply capital-readiness. | `FAIL_0010` | Governance still found no hypotheses ready for capital review. |
| Desired architecture state can retire legacy dependencies. | `FAIL_0011` | Runtime truth still consumed legacy artifacts. |
| Workflow expansion can repair missing data. | `FAIL_0012` | Missing macro readiness stayed research-only. |
| Audit cleanliness implies understanding maturity. | `FAIL_0013`, `FAIL_0015` | Warnings persisted and later escalated. |
| Proxy evidence represents candidate-specific behavior. | Final ranking, candidate backtests, final qualification | Every candidate carried proxy evidence risk. |

### Recurring Constraints

| Constraint | Evidence | Candidate Survival Impact |
| --- | --- | --- |
| Evidence maturity lags artifact maturity. | Journal failures and validation summaries. | HIGH |
| Authority boundaries are strict and fail closed. | Certification latest status `FAIL`; authority marker blockers. | HIGH |
| Candidate-specific data is missing or incomplete. | Final ranking biggest risk; 600 proxy penalties. | HIGH |
| Daily proxy data cannot fully test intraday mechanisms. | Candidate backtest data limitations; 180 mismatch penalties. | HIGH |
| Sample size remains insufficient for many candidates. | 156 insufficient data and sample-size penalties. | HIGH |
| Mechanism/regime labels are weak or duplicated. | Candidate review common risks and mechanism clusters. | MEDIUM |
| Worker compatibility is required before execution. | 3 `NO_COMPATIBLE_CONNECTED_WORKER` registry failures. | MEDIUM |
| Artifact lineage must resolve before research execution. | 3 `ArtifactStoreError` registry failures. | MEDIUM |
| Closed outcomes mature slower than observations. | `FAIL_0006`; candidate reports lack realized paper-forward outcomes. | MEDIUM |
| Warnings can recur before becoming blockers. | `FAIL_0015`; certification warning count persists at 1. | MEDIUM |

### Recurring Blockers

| Blocker | Frequency Signal | Notes |
| --- | ---: | --- |
| Authority boundary marker blockers | 34 latest certification blockers | Certification remained failed despite 12 passes and 1 warning. |
| Final score below threshold | 490 of 600 | Largest final qualification disqualification reason. |
| Backtest weak | 216 of 600 | Large class of candidates remained unsupported after backtest. |
| Insufficient data / sample-size penalty | 156 of 600 | Prevented qualification or weakened score. |
| Proxy penalty | 600 of 600 | Universal suppressor in final qualification. |
| Intraday/daily mismatch | 180 of 600 | Specific to mechanisms requiring intraday evidence. |
| Missing artifacts | 3 registry entries | Blocks autonomous execution. |
| No compatible worker | 3 registry entries | Blocks selected backlog execution. |

### Recurring Warning Patterns

| Warning Pattern | Evidence | Interpretation |
| --- | --- | --- |
| Certification warning count persists while blocker count changes. | Registry certification failures show warning_count=1 across blocker_count 26, 34, 40, 20, 3, 3, 3. | A lower blocker count is not a pass; recurring warnings still deserve tracking. |
| Candidate-quality modules are measurement-only. | Certification warning: candidate-quality optional detection remains non-authoritative. | Measurement capability is not candidate promotion authority. |
| Learning validation improves, but remains measurement-only. | Learning validation reports 6 improvements, 0 regressions, confidence 0.5, with no trading/capital/candidate authority. | Improving trend does not remove authority constraints. |
| Duplicate signal and regime mismatch remain monitored categories. | Candidate quality report tracks `DUPLICATE_SIGNAL` and `REGIME_MISMATCH`. | Quality can improve while duplicate/regime risks remain active. |
| Candidate review recommends human review despite positive replay. | Top candidates are paper-forward observation only, with invalidation if replay weakens or forward observations fail. | Positive replay is a warning to observe, not a survival guarantee. |

## Top 10 Failure Mechanisms

Impact classification estimates effect on candidate survival, not severity of operational incident.

| Rank | Failure Mechanism | Evidence | Impact On Candidate Survival | Why |
| ---: | --- | --- | --- | --- |
| 1 | Proxy evidence substituted for candidate-specific evidence | 600 proxy penalties; final ranking says all selected candidates depend on SPY daily proxy evidence. | HIGH | Directly suppresses final scores and weakens every candidate’s survival claim. |
| 2 | Authority boundary leakage in artifacts | 34 certification blockers; 7 registry `CERTIFICATION_BLOCK` failures. | HIGH | Blocks certification and prevents any state from becoming authoritative. |
| 3 | Backtest weakness after replay support | 216 `BACKTEST_WEAK`; 4 of 12 reviewed candidates weak in candidate backtests. | HIGH | Replay-positive candidates can fail or weaken under deeper backtest. |
| 4 | Insufficient data and sample-size fragility | 156 insufficient-data/sample-size penalties. | HIGH | Candidates cannot survive qualification when evidence is underpowered. |
| 5 | Intraday mechanism tested with daily proxy data | 180 intraday/daily mismatch penalties; backtest data limitation for opening range, session timing, VWAP. | HIGH | Mechanism evidence can be structurally mismatched to the data used. |
| 6 | Evidence maturity mistaken for artifact, architecture, or audit maturity | `FAIL_0004`, `FAIL_0013`, certification and validation reports. | HIGH | Creates false survival confidence before empirical evidence is mature. |
| 7 | Mechanism duplication and regime ambiguity | Candidate review common risks; candidate quality tracks duplicate signal and regime mismatch. | MEDIUM | Reduces independence and interpretability, but can be managed with clustering and controls. |
| 8 | Missing artifact lineage in autonomous research | 3 `ArtifactStoreError` registry failures. | MEDIUM | Blocks execution and can prevent validation, but does not directly prove candidate failure. |
| 9 | Worker compatibility gap | 3 `NO_COMPATIBLE_CONNECTED_WORKER` registry failures. | MEDIUM | Prevents research execution for selected backlog items; indirect survival impact. |
| 10 | Throughput and sleeve-output misdiagnosis | `FAIL_0007`, `FAIL_0009`, `FAIL_0014`; candidate volume and sleeve flow assumptions. | MEDIUM | Can misallocate review effort and overstate progress, but is less direct than data/backtest blockers. |

No recurring mechanism in the reviewed set is classified LOW in the top 10 because each pattern either directly suppresses candidate qualification, blocks certification, or repeatedly distorts interpretation. Lower-impact one-off wording or report-format issues were not included in the top 10.

## Cross-Pattern Interpretation

The recurring failure model is:

```text
artifact progress
  -> mistaken as evidence progress
  -> candidate survives early review
  -> validation applies data, replay, certification, and authority constraints
  -> candidate is weakened, blocked, or demoted to observation-only review
```

The most common failure co-occurrence is not market-mechanism failure alone. It is evidence/authority mismatch:

- evidence is proxy rather than candidate-specific
- data timeframe is mismatched to the mechanism
- sample size is not sufficient
- artifact language contains forbidden authority markers
- research systems fail closed when certification, lineage, worker, or artifact checks are incomplete

## Implications For Candidate Survival

Candidate survival improves when the review process separates:

- research artifact maturity from empirical evidence maturity
- replay support from paper-forward outcomes
- paper workflow state from runtime authority
- candidate volume from candidate quality
- proxy evidence from candidate-specific evidence
- architecture preference from runtime truth dependency

The highest-leverage survival filter is candidate-specific evidence replacement. The second-highest is authority-safe artifact language, because certification can remain failed even when most other checks pass.

## Analysis-Only Recommendations

- Track proxy dependency as a first-class failure pattern for every candidate review.
- Treat any intraday mechanism tested on daily proxy data as structurally weakened until intraday candidate-specific data exists.
- Keep certification blockers and warnings visible as research risks, not operational noise.
- Add co-occurrence tags to future failure records: proxy, sample-size, regime, duplicate, authority-marker, artifact-lineage, worker-compatibility.
- Require candidate review summaries to state whether evidence is candidate-specific, proxy-only, or mixed.
- Continue treating candidate quality reports as measurement-only unless runtime truth and governance explicitly say otherwise.

## Authority Boundary

This report is analysis only. It does not create candidates, recommend trades, allocate capital, size positions, place paper trades, approve live trading, change replay results, change qualification, change governance, or alter paper-forward state.
