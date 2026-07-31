# Atlas Research OS Architecture Reconciliation 001

Date: 2026-06-04
Status: Design reconciliation only

Scope: reconciles the Session 1-5 Atlas Research OS design outputs:

- `design_001_evidence_levels.md`
- `design_002_information_gain_metric.md`
- `design_003_knowledge_retirement.md`
- `design_004_candidate_impact_measurement.md`
- `design_005_worker_interfaces.md`

This document does not implement code, workers, commands, runtime modules, candidate promotion, paper setup, sleeve mutation, trade advice, broker execution, or capital authorization.

Runtime baseline: the 2026-06-04 runtime truth kernel reports `PARTIAL_CONTEXT` with highest readiness layer `BLOCKED`. The latest verified runtime graph at `/home/node/constellation_runtime_data/truth/reports/aegis_verified_runtime_graph_v1/2026-06-04/verified_runtime_graph.v1.json` reports `graph_status: BLOCKED`, `active_mode: HUMAN_REVIEWED_PAPER_MODE`, and `active_mode_readiness_status: BLOCKED`. Some read-only/query capabilities may be locally `READY` or `ALLOWED`, but global runtime truth remains blocked. No consumer may infer readiness from these design documents.

Core invariant: no consumer invents truth. Consumers query verified truth.

## Executive Decision

The Session 1-5 designs are directionally compatible, but they mix several planes that must be separated before implementation:

1. Evidence maturity: what kind and strength of support an artifact has.
2. Research lifecycle: whether a claim, hypothesis, learning node, or memory object is usable, stale, weakened, falsified, retired, reopened, or quarantined.
3. Worker/run status: whether a worker invocation succeeded, failed, skipped, partially completed, or quarantined its output.
4. Authority/readiness: what runtime truth, governance, operator approval, and capability states allow.
5. Scoring: advisory prioritization, evidence maturity measurement, and candidate-impact analytics.

Accepted architecture: Atlas Research OS must use separate fields and schemas for each plane. Any implementation that collapses these into one `status`, `state`, `score`, or `approval` field is rejected.

## Conflict Inventory

### Conflicting Terminology

| Conflict | Found in | Problem | Reconciliation |
| --- | --- | --- | --- |
| `OPERATOR_APPROVED` as evidence level vs approval state | Sessions 1, 3, 4 | Operator approval is human authority over a bounded disposition, not empirical evidence. Treating it as top evidence maturity launders authority into evidence. | Remove `OPERATOR_APPROVED` from evidence maturity. Replace with `operator_disposition.approval_status` and scoped approval fields. |
| `evidence_level`, `evidence_status`, and `maturity_score` | Sessions 1, 4, 5 | These names are used as if interchangeable. | Canonical field is `evidence_maturity_level`. `evidence_status` is rejected. `evidence_maturity_score` is a derived metric only. |
| `allowed_uses`, `forbidden_uses`, `prohibited_uses`, `forbidden_interpretations` | Sessions 1, 3, 4, 5 | Same concept appears under multiple names. | Use `allowed_uses` and `forbidden_uses` for machine policy. Use `limitations` for human report caveats. Reject `prohibited_uses` and `forbidden_interpretations` as canonical field names. |
| `lineage_refs`, `EvidenceTrail`, parent ids/hashes | Sessions 1, 5 | The docs alternate between embedded references and a named evidence object. | Canonical object is `LineageBundle`. `lineage_refs` may point to bundles but is not itself sufficient lineage. |
| `claim`, `hypothesis`, `learning node`, `research artifact`, `memory object` | Sessions 1, 3, 5 | Terms are valid but their object boundaries are not consistently explicit. | Use `ResearchObject` as the abstract parent type. Specialized types are `Claim`, `Hypothesis`, `ExperimentDesign`, `ExperimentResult`, `LearningNode`, `FailurePattern`, and `MemoryRecord`. |
| Candidate impact vs candidate quality | Sessions 2, 4 | `CandidateQualityRelevance` is an information-gain term, while candidate impact metrics are retrospective analytics. | `candidate_quality_relevance` remains an IG input. `candidate_impact_metric` remains an outcome measurement. They cannot be reused as candidate approval. |

### Conflicting Lifecycle States

| Conflict | Problem | Reconciliation |
| --- | --- | --- |
| Research lifecycle states and worker statuses both include terminal-sounding words. | `SUPPORTED`, `RETIRED`, and `QUARANTINED` describe object consumption, while `SUCCESS`, `FAILED`, and `PARTIAL` describe worker execution. | Maintain separate fields: `research_lifecycle_state`, `worker_run_status`, and `artifact_consumability`. |
| `QUARANTINED` appears both as lifecycle state and worker output status. | This makes it unclear whether the object is unsafe, the run failed, or both. | `research_lifecycle_state: QUARANTINED` is valid for unsafe research objects. Worker runs must use `worker_run_status: SUCCESS|FAILED|SKIPPED|PARTIAL`; unsafe outputs use `artifact_consumability: QUARANTINED`. |
| `REOPENED` can be misread as restored support. | Session 3 states reopening does not restore confidence, but that rule must become canonical. | `REOPENED` creates a new scoped research question. It does not restore `SUPPORTED`, evidence maturity, or candidate influence. |
| `STALE`, `WEAKENED`, `FALSIFIED`, and `RETIRED` are negative consumption states, but Session 5 workers can emit lifecycle recommendations. | A recommendation could be mistaken for a state transition. | Workers may emit `lifecycle_recommendation`; only an approved lifecycle controller may write `research_lifecycle_state`. |

Canonical research lifecycle states:

```text
SUPPORTED
STALE
WEAKENED
FALSIFIED
RETIRED
REOPENED
QUARANTINED
```

Canonical worker run statuses:

```text
SUCCESS
FAILED
SKIPPED
PARTIAL
```

Canonical artifact consumability states:

```text
CONSUMABLE
AUDIT_ONLY
QUARANTINED
SUPERSEDED
```

### Conflicting Evidence Models

Session 1 correctly states that evidence labels are not authorization labels, but it then includes `OPERATOR_APPROVED` in the evidence table. Session 4 repeats the issue by placing `OPERATOR_APPROVED` at the top of an evidence maturity order.

Accepted evidence maturity levels:

```text
GENERATED_ONLY
MOCK_ONLY
HISTORICAL_REPLAY
PAPER_FORWARD_OBSERVATION
EXTERNALLY_VALIDATED
```

Rejected evidence maturity level:

```text
OPERATOR_APPROVED
```

Canonical authority fields:

```text
operator_disposition.approval_status
operator_disposition.approved_scope
operator_disposition.approved_by
operator_disposition.approved_at
operator_disposition.review_due_at
authority_boundary
runtime_truth_ref
verified_graph_ref
capability_state_ref
```

Evidence maturity describes support. Operator approval describes bounded human disposition. Runtime truth describes current system readiness. None may substitute for another.

Required evidence object model:

```text
EvidenceRecord
  evidence_id
  evidence_maturity_level
  source_type
  source_artifact_ids
  source_content_hashes
  lineage_bundle_id
  scope
  regime_context
  uncertainty
  contradictions
  allowed_uses
  forbidden_uses
  created_at
  policy_version
```

Required lineage object model:

```text
LineageBundle
  lineage_bundle_id
  parent_artifact_ids
  parent_content_hashes
  source_spans
  worker_id
  worker_version
  run_id
  runtime_truth_ref
  verified_graph_ref
  label_integrity_result
  hash_validation_result
  created_at
```

### Conflicting Scoring Models

Session 2 defines `IG(t)` as advisory expected learning value. Session 4 defines candidate impact measurements and evidence maturity deltas. These are different scores and must not share field names or thresholds.

Accepted scoring domains:

| Domain | Canonical name | Purpose | Forbidden use |
| --- | --- | --- | --- |
| Backlog/task prioritization | `information_gain_score` | Rank eligible research work after gates pass. | Runtime readiness, evidence maturity, candidate approval, capital approval. |
| Evidence support measurement | `evidence_maturity_score` | Derived numeric summary for reporting only. | Replacing `evidence_maturity_level`, operator approval, or lifecycle state. |
| Candidate-factory analytics | `candidate_impact_metrics` | Retrospectively measure whether learning changed candidate-factory quality. | Candidate creation, promotion, paper setup, portfolio allocation, trade advice. |

Accepted `information_gain_score` shape:

```text
IG(t) =
  Wn * novelty
+ Wu * uncertainty_reduction
+ Wf * failure_pattern_reduction
+ Wr * regime_coverage_improvement
+ Wq * candidate_quality_relevance
- Wc * cost_penalty
- Wd * duplicate_penalty
```

Required interpretation: `information_gain_score` is computed only after eligibility gates pass. A high score cannot override runtime truth, verified graph state, worker authority, lineage failure, duplicate retirement, or forbidden artifact risk.

Accepted candidate impact metrics:

```text
candidate_conversion_rate
rejection_rate_reduction
evidence_maturity_delta
hypothesis_survival_improvement
portfolio_scoring_rejection_reduction
repeated_failure_reduction
```

Required metric rule: every metric must include baseline window, post-learning window, sample size, lineage references, confidence, confounders, and forbidden interpretation.

### Conflicting Artifact Definitions

Session 5 defines common worker output metadata, while Sessions 1-4 define concepts such as evidence, lifecycle transition, impact report, and learning object. These need a single artifact taxonomy.

Canonical artifact taxonomy:

| Artifact type | Meaning | Writes allowed before implementation? | Consumption |
| --- | --- | ---: | --- |
| `ResearchObject` | Abstract parent for claims, hypotheses, experiments, learning nodes, failure patterns, and memory records. | No | Not consumable without schema and lineage. |
| `EvidenceRecord` | Evidence maturity and source-support record. | No | Supports research only. |
| `LineageBundle` | Parent/hash/runtime/source trace. | No | Required by all consumable artifacts. |
| `LifecycleTransitionRecord` | Append-only transition for a research object. | No | Only lifecycle controller may emit. |
| `WorkerRunRecord` | Execution attempt and status. | No | Audit and idempotency. |
| `ResearchReport` | Human-readable summary or design report. | Yes, design-only | Not runtime truth. |
| `CandidateImpactReport` | Retrospective candidate-factory measurement. | No | Measurement only. |
| `BacklogRecommendation` | Advisory ranked work recommendation. | No | Advisory only. |

Required common metadata for future machine-consumable artifacts:

```text
artifact_id
artifact_type
schema_id
schema_version
created_at
content_hash
lineage_bundle_id
evidence_record_ids
research_lifecycle_state
artifact_consumability
allowed_uses
forbidden_uses
runtime_truth_ref
verified_graph_ref
policy_version
```

Worker-only metadata belongs in `WorkerRunRecord`, not every persisted research object:

```text
worker_id
worker_version
run_id
input_hash
idempotency_key
worker_run_status
reason_codes
safety_gate_results
```

## Accepted Standards

1. Evidence maturity labels are source-support labels only.
2. Operator approval is an authority/disposition record, not evidence maturity.
3. Runtime truth kernel and verified runtime graph are mandatory references for any future worker or machine-consumable artifact.
4. Research lifecycle, worker run status, artifact consumability, and authority state are separate fields.
5. `LineageBundle` is mandatory for consumption. Missing, stale, cyclic, or hash-mismatched lineage makes the artifact `QUARANTINED` or `AUDIT_ONLY`.
6. Information gain is advisory task prioritization only.
7. Candidate impact measurement is retrospective and observational only.
8. Quarantine is fail-closed and audit-only until a repair or reopening process explicitly changes state.
9. Reopening retired or stale knowledge creates a new scoped research question; it does not restore prior confidence.
10. All reports must state their forbidden uses, including no candidate promotion, no sleeve mutation, no trade advice, no broker execution, and no real capital allocation.

## Rejected Standards

1. `OPERATOR_APPROVED` as an evidence maturity level.
2. Any single generic `status` field that blends lifecycle, run status, readiness, approval, and consumability.
3. `evidence_status` as a canonical field name.
4. `prohibited_uses` and `forbidden_interpretations` as machine policy field names.
5. Evidence maturity scores that silently convert unsupported, missing-lineage, generated-only, or mock-only evidence into positive support.
6. Candidate conversion rate as a success metric without rejection quality, downstream evidence quality, and forbidden artifact checks.
7. Worker lifecycle recommendations as automatic lifecycle transitions.
8. Any implementation where memory records, reports, or scores can be consumed as runtime truth.

## Canonical Glossary

`Artifact`: A persisted object with id, type, schema, content hash, lineage, allowed uses, forbidden uses, and runtime references.

`ResearchObject`: The abstract object being studied or curated. Includes claims, hypotheses, experiment designs, experiment results, learning nodes, failure patterns, and memory records.

`Claim`: A normalized assertion extracted from a source, model output, operator question, or research note. A claim is not a hypothesis and is not evidence by itself.

`Hypothesis`: A scoped, testable research proposition with assumptions and falsification criteria.

`ExperimentDesign`: A planned test with declared data sources, measurement windows, success thresholds, failure thresholds, leakage controls, and cost estimate.

`ExperimentResult`: Output of executing an approved experiment design. It must preserve source lineage and uncertainty.

`LearningNode`: A distilled lesson derived from evidence-linked claims, experiments, and failures.

`FailurePattern`: A repeated or material way research, candidate generation, lineage, data, or policy failed.

`MemoryRecord`: A durable research-memory entry used to prevent duplicate work and preserve prior learning.

`EvidenceRecord`: A record describing the maturity, source, scope, uncertainty, and lineage of support for a research object.

`EvidenceMaturityLevel`: One of `GENERATED_ONLY`, `MOCK_ONLY`, `HISTORICAL_REPLAY`, `PAPER_FORWARD_OBSERVATION`, or `EXTERNALLY_VALIDATED`.

`LineageBundle`: The canonical source, parent, hash, worker/run, runtime truth, and verified graph trace for an artifact.

`ResearchLifecycleState`: One of `SUPPORTED`, `STALE`, `WEAKENED`, `FALSIFIED`, `RETIRED`, `REOPENED`, or `QUARANTINED`.

`WorkerRunStatus`: One of `SUCCESS`, `FAILED`, `SKIPPED`, or `PARTIAL`.

`ArtifactConsumability`: One of `CONSUMABLE`, `AUDIT_ONLY`, `QUARANTINED`, or `SUPERSEDED`.

`OperatorDisposition`: A bounded human decision about research handling, such as continue, retire, reopen, review, or escalate. It is not evidence and not capital authority.

`InformationGainScore`: Advisory score estimating expected learning value for an eligible research task.

`CandidateImpactMetric`: Retrospective metric measuring whether research learning changed candidate-factory quality, rejection patterns, evidence maturity, or repeated failure.

`RuntimeTruthRef`: Reference to the runtime truth kernel artifact used by the producer.

`VerifiedGraphRef`: Reference to the verified runtime graph artifact used by the producer.

`AuthorityBoundary`: Explicit statement of what an artifact or worker may and may not do.

## Canonical Architecture Model

Atlas Research OS should be implemented as a research-only control plane with explicit boundaries:

```text
Runtime Truth Kernel + Verified Runtime Graph
  -> Authority and readiness gates

Research Backlog
  -> eligibility gates
  -> information_gain_score
  -> selected research task

Research Workers
  -> WorkerRunRecord
  -> ResearchObject / EvidenceRecord / LineageBundle / ResearchReport

Lineage and Label Integrity
  -> artifact_consumability
  -> quarantine or audit-only when unsafe

Lifecycle Controller
  -> LifecycleTransitionRecord
  -> research_lifecycle_state

Memory Index
  -> duplicate detection
  -> retirement/reopening awareness
  -> negative knowledge preservation

Evaluation Layer
  -> candidate_impact_metrics
  -> no candidate creation or promotion

Reports
  -> human summaries
  -> no runtime truth
```

Architecture rules:

1. Workers cannot write lifecycle state directly unless the worker is the future approved lifecycle controller.
2. Workers cannot consume their own outputs as truth without a separate lineage and label-integrity pass.
3. Memory can influence research prioritization through duplicate and retirement checks, but memory cannot authorize candidates or capital.
4. Candidate impact analytics can observe candidate-factory outputs from approved systems, but cannot create or promote candidate artifacts.
5. Any artifact that touches candidate, sleeve, trade, broker, portfolio, or capital semantics must fail closed unless the verified graph and authority boundary explicitly allow that action.

## Required Changes Before Implementation Continues

1. Update Session 1 and Session 4 designs to remove `OPERATOR_APPROVED` from evidence maturity levels and move it to `OperatorDisposition`.
2. Define schemas for `EvidenceRecord`, `LineageBundle`, `ResearchObject`, `WorkerRunRecord`, `LifecycleTransitionRecord`, and `CandidateImpactReport`.
3. Rename all canonical machine fields to `evidence_maturity_level`, `allowed_uses`, `forbidden_uses`, `research_lifecycle_state`, `worker_run_status`, and `artifact_consumability`.
4. Add validation rules that reject artifacts containing a generic overloaded `status` without typed state fields.
5. Add label-integrity tests for generated/mock laundering and operator-approval laundering.
6. Add lifecycle tests proving `REOPENED` does not restore `SUPPORTED` or evidence maturity.
7. Add worker tests proving lifecycle recommendations do not mutate lifecycle state.
8. Add scoring tests proving `information_gain_score` cannot override failed gates or blocked runtime truth.
9. Add candidate-impact tests proving metrics do not create candidates, promote candidates, create paper positions, mutate sleeves, emit trade advice, or allocate capital.
10. Add artifact-consumability tests proving missing or mismatched lineage produces `QUARANTINED` or `AUDIT_ONLY`.
11. Add verified graph and runtime truth references to every future machine-consumable artifact schema.
12. Define an artifact authority registry before any implementation writes machine-consumable Research OS artifacts.

Implementation must not proceed until the accepted standards above are encoded as schemas and tests. Design-only research reports may continue, but they must remain non-authoritative and explicitly forbidden from runtime consumption.
