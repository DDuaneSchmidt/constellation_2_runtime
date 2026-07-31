# Atlas Failure Taxonomy Expansion Plan 001

Date: 2026-06-05
Status: DESIGN_ONLY
Mode: evaluation-only, offline-only

## Purpose

Convert `taxonomy_recall_results_001.md` into a controlled expansion plan for Atlas Failure Taxonomy use in Research Adversary evaluation.

This plan does not implement taxonomy integration, change runtime behavior, create candidates, run replay, change qualification, change governance, allocate capital, recommend trades, execute through a broker, or write memory automatically.

All taxonomy output remains GENERATED_ONLY until a later implementation request explicitly changes code and passes the Aegis manifest, test, and audit requirements.

## Inputs

- `research_journal/reports/taxonomy_recall_results_001.md`
- `research_journal/design/atlas_failure_taxonomy_001.md`
- `research_journal/reports/taxonomy_integration_evaluation_001.md`
- latest verified runtime graph reviewed: `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-05/verified_runtime_graph.v1.json`

## Recall Result Summary

The taxonomy-enhanced replay improved failure recall from 23.7% to 86.8%, assumption recall from 39.1% to 81.2%, and constraint recall from 13.2% to 86.8%, with zero false positives and zero authority violations under the frozen mapping.

The expansion is not unrestricted. The recall report identifies:

- ambiguous taxonomy cases: 5
- multi-category cases needing reviewer confirmation: 6

The controlled expansion objective is to preserve the recall lift while preventing category inflation, false certainty, and authority leakage.

## Expansion Scope

Allowed expansion:

- add controlled category selection rules
- add category precedence rules
- add tie-breaking rules
- add category confidence scoring guidance
- define when multiple categories may be emitted
- define when UNKNOWN must be emitted
- keep secondary categories as reviewer hints

Forbidden expansion:

- no candidate approval or rejection
- no replay, qualification, certification, or governance override
- no capital, sizing, trading, broker, or portfolio authority
- no automatic memory write
- no runtime readiness inference from taxonomy labels
- no consumer may invent truth from a taxonomy label

## Ambiguous Cases

The five ambiguous cases are the MEDIUM-confidence recall mappings:

| Failure | Current taxonomy recall | Expansion treatment |
| --- | --- | --- |
| `FAIL_0009` | `FLOW_CONVERSION_MISDIAGNOSIS` | require evidence/data versus flow-conversion split |
| `FAIL_0010` | `AUTHORITY_READINESS_CONFUSION` | require authority versus evidence maturity split |
| `FAIL_0012` | `DATA_READINESS_GAP` | require data readiness versus context/artifact split |
| `FAIL_0013` | `AUDIT_UNDERSTANDING_CONFUSION` | require audit-warning versus evidence maturity split |
| `failure-demo-opening-range` | `REGIME_MISMATCH` | require regime versus proxy/mechanism split |

Rule: ambiguous cases must not be promoted from reviewer hints to primary labels unless the required source signal is explicit in the reviewed artifact or verified truth artifact. If the signal is inferred from narrative language only, the primary label is UNKNOWN and the suspected labels remain secondary hints.

## Multi-Category Cases

The six multi-category cases should remain controlled multi-label outputs:

| Failure | Likely category conflict | Expansion treatment |
| --- | --- | --- |
| `FAIL_0006` | outcome maturity, data quality, insufficient evidence | primary label follows resolved-outcome state |
| `FAIL_0007` | candidate volume, duplicate cluster, qualification mismatch | primary label follows quality gate failure before volume symptom |
| `FAIL_0010` | authority readiness, qualification mismatch, insufficient evidence | primary label follows strongest blocked gate |
| `FAIL_0012` | data readiness, context omission, artifact lineage | primary label follows missing required source type |
| `FAIL_0016` | fixture baseline, regime, proxy, mechanism mismatch | primary label follows falsification target |
| `failure-demo-opening-range` | regime mismatch, proxy dependency, mechanism mismatch | primary label requires predeclared regime evidence |

Rule: emit multiple categories only when the artifact contains independent evidence for each category. Do not emit multiple categories just because categories are historically related in the crosswalk.

## Precedence Rules

Apply precedence from highest to lowest:

1. `CERTIFICATION_BLOCKER` or safety/certification gate labels when the artifact explicitly says certification or safety gate failed.
2. `RUNTIME_DEPENDENCY` or artifact lineage labels when the workflow cannot load, verify, or trace a required runtime artifact.
3. `WORKER_COMPATIBILITY` when execution cannot start because no compatible connected worker exists.
4. `QUALIFICATION_MISMATCH` or authority readiness labels when narrative readiness conflicts with a gate, threshold, eligibility, or authority state.
5. `DATA_QUALITY` or data readiness labels when required data is missing, stale, immature, unresolved, or not reconstructable.
6. `INSUFFICIENT_EVIDENCE` or evidence maturity labels when artifacts exist but evidence level is too weak for the claim.
7. `OBSERVATION_DRIFT` or outcome maturity labels when forward observations no longer answer the original hypothesis or remain unresolved.
8. `DUPLICATE_CLUSTER` or selection/volume labels when repeated candidates, samples, or mechanisms inflate support.
9. `MECHANISM_MISMATCH`, `PROXY_DEPENDENCY`, or `REGIME_DEPENDENCY` when the claimed mechanism depends on a competing explanation, proxy, or regime split.
10. `WARNING_RECURRENCE` or audit understanding labels when recurring warnings are interpreted incorrectly but no higher gate is explicitly blocked.

Precedence is not authority. It selects a generated-only primary category for review readability.

## Category Tie-Breaking

Tie-breaking order:

1. Prefer the category tied to an explicit blocker code, status field, or verified runtime truth artifact.
2. Prefer the category whose falsification question would most directly change the review conclusion.
3. Prefer the category closest to the failed workflow boundary: certification, runtime, worker, qualification, data, evidence, observation, mechanism.
4. Prefer the narrower Atlas-specific category over a generic critique category.
5. If two categories remain equally supported, emit the higher-precedence category as primary and the other as secondary.
6. If neither category has explicit evidence, emit UNKNOWN.

Tie-breaking must not use desired recall improvement as evidence.

## Confidence Scoring

Use bounded generated-only confidence:

| Confidence | Score range | Required support |
| --- | ---: | --- |
| HIGH | 0.80-1.00 | explicit blocker/status/source signal and category-specific falsification question both match |
| MEDIUM | 0.50-0.79 | strong symptom match, but either source evidence is indirect or another category is plausible |
| LOW | 0.20-0.49 | weak symptom match with missing source evidence; keep as reviewer hint only |
| UNKNOWN | 0.00-0.19 | insufficient signal, contradictory signal, or no category-specific falsification path |

Confidence is category suspicion only. It is not readiness, approval, rejection, qualification, governance, safety, capital, or trading confidence.

## Multiple Category Emission

Emit multiple categories only when all conditions hold:

- each category has a distinct evidence signal
- the secondary category changes a reviewer question or follow-up action
- the secondary category is not merely a parent, synonym, or historical neighbor of the primary category
- the output clearly marks one primary category
- all secondary categories are marked as reviewer hints

Maximum output:

- one primary category
- up to two secondary categories
- one UNKNOWN primary with up to two suspected secondary hints when primary evidence is insufficient

## UNKNOWN Emission

Emit UNKNOWN when:

- no explicit source signal supports a category
- the result depends on inference from workflow progress rather than verified truth
- two or more categories are plausible and no precedence rule resolves the tie
- category assignment would require assuming readiness, qualification, certification, governance, or data availability
- source artifacts are missing, stale, non-reconstructable, or unavailable to the reviewer
- the artifact contains generic critique language without Atlas-specific failure evidence

UNKNOWN is a valid controlled output, not a miss. It prevents taxonomy labels from fabricating certainty.

## Category Split Requirements

Authority readiness versus evidence maturity:

- use authority readiness when a gate, threshold, eligibility, certification, or governance state contradicts readiness language
- use evidence maturity when the issue is sample size, replay-only support, generated-only support, or lack of closed forward outcomes

Data readiness versus artifact lineage:

- use data readiness when required market, macro, event, sleeve, or observation data is missing or stale
- use artifact lineage when the workflow cannot load, trace, hash, or verify a referenced artifact

Regime versus proxy versus mechanism:

- use regime when support depends on market state separation or predeclared regime identity
- use proxy when evidence uses a substitute instrument, fixture, index, or indirect artifact
- use mechanism when an alternate causal explanation better fits the same samples

Warning recurrence versus audit understanding:

- use warning recurrence when the same warning category repeats across review windows
- use audit understanding when the review misreads audit status, audit scope, or pass-with-warning semantics

## Acceptance Criteria For Later Implementation

A later implementation should be accepted only if:

- the 19-case frozen recall set remains available
- direct hits do not fall below the current 14-case result without documented tradeoff
- false positives remain zero on the frozen mapping or are explicitly reviewed
- MEDIUM cases remain reviewer-confirmation cases unless upgraded by explicit source signal
- all authority boundaries remain unchanged
- generated output clearly separates primary category, secondary hints, confidence, and UNKNOWN
- Aegis module manifests and tests are updated if behavior, schema, commands, evidence, policies, or UI surfaces change
- `npm run aegis:audit` passes after the implementation

## Authority Boundary

This expansion plan has no authority to:

- create, approve, reject, promote, or qualify candidates
- modify replay, governance, certification, or paper-forward state
- allocate capital or size positions
- recommend trades or submit broker orders
- write memory automatically
- infer runtime readiness from code or generated taxonomy labels

Consumers must query verified truth. No consumer may invent truth from taxonomy output.
