# Atlas Research OS Design 005: Worker Interfaces

Date: 2026-06-04
Status: Design only

Scope: defines standard interfaces for future Research OS workers. This document does not implement workers, commands, runtime modules, candidate promotion, sleeve mutation, paper setup, trade advice, broker execution, or capital authorization.

Runtime posture: the verified runtime graph for 2026-06-04 is `BLOCKED`. Future workers must fail closed when runtime truth, lineage, or authority is blocked.

## Common Artifact Metadata

Every worker output must include:

- `artifact_id`
- `artifact_type`
- `schema_id`
- `schema_version`
- `worker_id`
- `worker_version`
- `run_id`
- `created_at`
- `input_hash`
- `content_hash`
- `parent_artifact_ids`
- `parent_content_hashes`
- `evidence_level`
- `lineage_refs`
- `mechanism_tags`
- `regime_context`
- `allowed_uses`
- `forbidden_uses`
- `runtime_truth_ref`
- `verified_graph_ref`
- `idempotency_key`

Common forbidden uses: trade advice, broker execution, live trading, real capital allocation, sleeve mutation, candidate promotion, paper-position creation, and bypassing verified truth.

## Worker Specifications

| Worker | Inputs | Outputs | Safety constraints | Failure modes | Idempotency | Lineage |
| --- | --- | --- | --- | --- | --- | --- |
| `ClaimWorker` | source artifact, operator question, generated idea, external text, prior claim clusters | normalized claims, claim clusters, duplicate flags, evidence labels | cannot assert truth; cannot raise evidence above source; must flag generated/mock content | hallucinated claim, duplicate miss, source parse failure, label mismatch | key by normalized source hash, extraction policy, prompt/config hash | each claim links to source span, source hash, extraction run |
| `HypothesisWorker` | claim clusters, mechanisms, regime context, prior hypotheses, retirement state | scoped hypotheses, assumptions, falsification criteria, duplicate/reopen flags | cannot create candidate; cannot treat hypothesis as validated; must preserve retired/falsified warnings | vague hypothesis, untestable scope, duplicate laundering, ignored retirement | key by claim cluster, mechanism, scope, worker version | hypothesis links to claims, mechanisms, prior retired or related hypotheses |
| `ExperimentDesignWorker` | hypothesis, data availability, evidence level, prior experiments, failure patterns | experiment design, measurement plan, success/failure thresholds, cost estimate | cannot execute experiment; cannot use unavailable data as available; must declare mock vs real source | leakage risk, unbounded search, missing falsification metric, forbidden artifact path | key by hypothesis, design parameters, data source set, policy hash | design links to hypothesis, source requirements, prior experiments |
| `ExperimentExecutionWorker` | approved experiment design, data sources, runtime truth refs, execution budget | execution record, results, diagnostics, source coverage, error report | cannot run if sources are stale or authority blocked; cannot mutate candidates/sleeves/capital; must separate mock from historical | source missing, runtime blocked, timeout, leakage detection, partial write | key by design hash, data snapshot hashes, execution config | results link to design, data snapshots, runtime refs, logs |
| `LearningWorker` | experiment results, observations, failure patterns, hypothesis state | learning nodes, confidence updates, contradiction notes, lifecycle recommendations | cannot auto-retire unless future policy permits; cannot approve capital; must carry uncertainty | overclaiming, unsupported causality, ignored contradictions, label laundering | key by result hashes, learning policy, prior state hash | learning links to all supporting and contradicting evidence |
| `EvaluationWorker` | learning nodes, candidate-factory measurements, outcomes, rejection reasons | impact metrics, evaluation report, attribution confidence, test gaps | measurement only; cannot promote candidates; cannot alter scoring or allocation | causal overstatement, sample-size weakness, metric drift, stale baseline | key by metric spec, baseline/post windows, input hashes | metrics link to learning, candidate records, outcomes, exclusions |
| `AttentionWorker` | backlog, information-gain inputs, stale/retired knowledge, operator priorities | ranked backlog recommendations, skip reasons, attention deltas | advisory only; cannot schedule autonomous runs without approved orchestrator; cannot bypass gates | duplicate priority, cost underestimation, stale runtime refs, unsafe recommendation | key by backlog snapshot, score weights, runtime graph hash | recommendations link to scoring terms and eligibility gates |
| `MemoryCuratorWorker` | claims, hypotheses, experiments, failures, learning nodes, lifecycle states | merged clusters, retired/reopened records, quarantine recommendations, memory index updates | cannot erase audit trail; cannot unquarantine without repair evidence; cannot convert memory into truth | incorrect merge, lost lineage, unsafe reopening, stale threshold error | key by memory snapshot, curation policy, object hashes | every merge/split/retire/reopen links to prior objects and decision evidence |

## Required Outputs By Worker

All outputs must include:

- artifact metadata above
- structured `status`: `SUCCESS`, `FAILED`, `SKIPPED`, `PARTIAL`, or `QUARANTINED`
- `reason_codes`
- `safety_gate_results`
- `lineage_validation_result`
- `uncertainty`
- `next_allowed_actions`
- `forbidden_interpretations`

## Failure Handling

Workers fail closed. On failure:

- preserve logs and partial artifacts for audit
- mark partial artifacts as not consumable
- do not advance lifecycle state
- do not complete backlog items
- do not create candidate, paper, sleeve, trade, or capital artifacts
- emit a structured failure reason

## Idempotency Requirements

Each worker must be deterministic for the same inputs, policy version, model/config version, data snapshot, and runtime graph hash. Re-running the same work should return the same artifact or a duplicate-detected skip unless an input hash changed.

## Lineage Requirements

No output is consumable without lineage. Lineage must include parent ids, parent hashes, source scopes, evidence levels, worker version, runtime truth reference, and verified graph reference.

If lineage is missing, cyclic, stale, or mismatched, the output must be quarantined.
