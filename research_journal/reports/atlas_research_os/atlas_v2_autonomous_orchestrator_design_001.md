# Atlas V2 Autonomous Orchestrator Design 001

Date: 2026-06-04
Status: Design only
Runtime posture: Not implemented, not enabled, not certified

## 1. Purpose

The orchestrator runs bounded research jobs selected from the backlog.
It does not become the research logic itself.

The orchestrator is a coordination layer for the Atlas Research OS foundation:

- Research Backlog
- Priority Engine
- Artifact Store
- Lineage Model
- Learning Lifecycle
- Governance Layer
- Research Workers
- Reports
- Scheduler

Its role is to select eligible work, invoke approved workers, record artifacts, validate lineage, enforce governance, update lifecycle state, and emit auditable run records. It must not generate investment truth, promote candidates, allocate capital, open paper positions, create sleeves, or bypass the verified runtime graph.

Core invariant: no consumer invents truth. Consumers query verified truth.

The orchestrator is therefore a bounded control plane. It coordinates research operations only when the runtime truth kernel, verified runtime graph, backlog state, and governance layer all permit the action. If runtime truth is blocked, stale, missing, or ambiguous, the orchestrator skips or fails closed.

## 2. Trigger Model

Triggers describe why an orchestrator run is requested. They do not grant authority by themselves. Every trigger must pass the same eligibility, safety, lineage, and governance gates before any worker runs.

| Trigger | Classification | Intended meaning | Required behavior |
| --- | --- | --- | --- |
| manual trigger | MANUAL | Operator explicitly requests one bounded run. | Run at most one eligible backlog item unless the operator request names a narrower dry-run or audit-only scope. |
| hourly scheduled trigger | SCHEDULED | Future timer asks whether bounded work is available. | Skip cleanly when no eligible backlog item exists or when any runtime gate is not ready. |
| new journal entry | EVENT_DRIVEN | A new research journal artifact may create follow-up research demand. | Evaluate backlog candidates derived from the journal entry; do not create candidate, capital, sleeve, or trade artifacts. |
| failed candidate batch | EVENT_DRIVEN | Existing candidate-related evidence indicates a research failure pattern worth studying. | Route only to diagnosis or learning workers; never retry candidate creation or promotion. |
| stale hypothesis | CONDITION_WATCH | A hypothesis has exceeded freshness or review thresholds. | Select refresh, closure, or evidence-review work only after lineage and authority checks. |
| new paper-forward observation | EVENT_DRIVEN | Existing paper-forward evidence may require research follow-through. | Produce observation, learning, or review artifacts only; do not alter trade state. |
| new market regime snapshot | EVENT_DRIVEN | A market context snapshot may change research priorities. | Re-score backlog priority or request research review without creating live recommendations. |
| operator-requested research question | MANUAL | Operator submits a bounded research question. | Convert to backlog item or run dry evaluation only after governance approval and duplicate checks. |

Condition-watch triggers are evaluated by watchers that inspect state and request a run when thresholds are crossed. Watchers do not execute research. Scheduled triggers may call the same watcher evaluation path, but the run ledger must preserve the originating trigger type and source.

## 3. Run Ledger

Every attempted run writes one ledger record, including clean skips and hard failures. The run ledger is append-only and is the canonical operational trace for orchestrator execution.

Required fields:

| Field | Type | Description |
| --- | --- | --- |
| `run_id` | string | Stable unique run identifier, preferably time-sortable and content-linked to trigger and selected work. |
| `started_at` | ISO-8601 timestamp | UTC timestamp when the run attempt entered lock acquisition or execution. |
| `completed_at` | ISO-8601 timestamp or null | UTC timestamp when the run reached terminal status. |
| `status` | enum | One of `SUCCESS`, `FAILED`, `SKIPPED`, `PARTIAL`, `DRY_RUN_SUCCESS`, `DRY_RUN_FAILED`, `AUDIT_ONLY_SUCCESS`, `AUDIT_ONLY_FAILED`. |
| `trigger_type` | enum | One of `EVENT_DRIVEN`, `SCHEDULED`, `MANUAL`, `CONDITION_WATCH`. |
| `trigger_source` | object | Source-specific details such as operator id, journal entry id, snapshot id, timer id, or watcher name. |
| `selected_backlog_items` | array | Backlog item ids selected for this run, including skipped selections when applicable. |
| `workers_invoked` | array | Worker ids, versions, inputs, and terminal statuses. Empty for dry-run eligibility checks that do not invoke workers. |
| `artifacts_created` | array | Artifact ids, paths, hashes, schemas, and lineage parents created during the run. Empty when no artifacts are written. |
| `safety_gate_results` | array | Gate id, gate version, result, evidence references, and failure details. |
| `lineage_validation_result` | object | Lineage validator version, result, checked artifacts, parent references, and hash validation details. |
| `errors` | array | Structured errors with stage, code, message, and recoverability. |
| `skip_reason` | string or null | Required when `status` is `SKIPPED`; null otherwise. |
| `duration_seconds` | number | Wall-clock duration from `started_at` to `completed_at`. |

The ledger must distinguish:

- a run that did no work because it was ineligible
- a run that failed before invoking a worker
- a run that invoked a worker but created no artifact
- a run that created artifacts but failed post-write validation
- a dry-run that would have executed but intentionally did not write artifacts

## 4. Locking and Idempotency

### No Overlapping Runs

The orchestrator must hold a single global run lock before selecting backlog work. The lock should contain:

- lock id
- owner process id or host id
- acquired timestamp
- heartbeat timestamp
- intended trigger
- lease expiration timestamp

A new run must not start while a valid lock exists. If a valid lock exists, the attempted run writes a `SKIPPED` ledger entry with `skip_reason` set to `LOCK_HELD`.

### Clean Skip Behavior

Skips are expected operational outcomes, not failures. Skip reasons should be enumerated, including:

- `LOCK_HELD`
- `NO_ELIGIBLE_BACKLOG_ITEM`
- `RUNTIME_TRUTH_BLOCKED`
- `VERIFIED_GRAPH_BLOCKED`
- `GOVERNANCE_NOT_READY`
- `DUPLICATE_RUN_DETECTED`
- `DRY_RUN_ONLY`
- `TRIGGER_OUT_OF_SCOPE`
- `FORBIDDEN_ARTIFACT_RISK`

Each skip must record the evaluated trigger, the decision evidence, and the exact gate or condition that prevented execution.

### Retry Behavior

Retries must be explicit and bounded. A retry may occur only when:

- the prior failure is classified as transient
- no forbidden artifact was created
- lineage state can be proven consistent
- the selected backlog item remains eligible
- retry count is below the configured maximum

Retry identity must link to the original `run_id`. Retries must not reuse the original run id. They must record `retry_of_run_id`, attempt number, and the failure class being retried.

### Duplicate Prevention

Duplicate prevention should use an idempotency key derived from:

- trigger type
- trigger source id
- selected backlog item id
- worker id and version
- normalized worker input hash
- effective governance policy hash
- verified runtime graph hash

Before executing a worker, the orchestrator checks whether an equivalent completed or in-progress run exists. If so, it skips with `DUPLICATE_RUN_DETECTED` and links to the prior run.

### Crash Recovery

Crash recovery begins by scanning locks and non-terminal ledger records. A stale lock may be reclaimed only when:

- its heartbeat is older than the configured lease timeout
- no live owner can be verified
- the associated ledger record is non-terminal

Recovered records become `PARTIAL` or `FAILED` based on artifact and lineage inspection. Recovery must not delete artifacts. It must either validate them as committed evidence or mark them as orphaned and blocked from consumption.

### Partial-Run Handling

Partial runs are permitted only as observed recovery states. They are not successful outcomes.

If a worker creates artifacts and a later gate fails, the orchestrator must:

- mark the run `FAILED` or `PARTIAL`
- block the new artifacts from downstream consumption
- record the failed gate
- preserve the artifacts for audit
- avoid lifecycle or backlog advancement
- require operator or certified repair before retry

## 5. Stage Controller

Future stages run in a fixed order. Each stage receives immutable inputs from prior stages and emits structured stage results to the run ledger.

1. Select backlog item
2. Validate eligibility
3. Invoke worker
4. Write artifacts
5. Validate lineage
6. Run governance checks
7. Update lifecycle
8. Update backlog state
9. Write run ledger
10. Write report

Stage responsibilities:

| Stage | Responsibility | Failure posture |
| --- | --- | --- |
| select backlog item | Query backlog and priority engine for the highest-value eligible item. | Skip if none exists. |
| validate eligibility | Check runtime truth, verified graph, authority, duplicate keys, and worker availability. | Fail closed or skip for expected ineligibility. |
| invoke worker | Run one approved bounded research worker with declared inputs. | Fail if worker exceeds scope, timeout, or contract. |
| write artifacts | Persist only approved research artifacts with schema, hash, and lineage parent references. | Fail if artifact path, schema, or type is forbidden. |
| validate lineage | Confirm every artifact links to accepted parents and hashes. | Hard fail if invalid. |
| run governance checks | Apply mandatory safety gates and policy checks. | Hard fail if any gate fails. |
| update lifecycle | Move learning lifecycle state only after artifacts and governance pass. | Skip lifecycle update on any prior failure. |
| update backlog state | Mark item completed, blocked, skipped, or retryable. | Do not advance on failed validation. |
| write run ledger | Record final outcome. | Must be best-effort even on prior stage failure. |
| write report | Emit human-readable and machine-readable summaries. | Report failure should not transform a failed run into success. |

Dry-run mode executes selection, eligibility, duplicate checks, lineage preflight, and governance preflight without invoking workers or writing research artifacts.

Audit-only mode validates existing ledger, artifacts, lineage, and governance state without selecting or executing new work.

## 6. Safety Gates

Every non-skip run must pass all mandatory gates. Any failed mandatory gate is a hard failure.

| Gate | Requirement | Hard-failure condition |
| --- | --- | --- |
| label integrity | Labels must come from approved schemas and cannot copy outcome labels into pre-outcome decisions. | Missing, circular, fabricated, or authority-expanding labels. |
| authority boundary | Worker actions must stay within declared capability and governance authority. | Worker attempts forbidden action or consumes unauthorized truth source. |
| forbidden artifact audit | Created artifacts must not include capital, sleeve, trade, candidate promotion, broker, or live recommendation artifacts. | Any forbidden artifact path, schema, name, or semantic type is detected. |
| lineage integrity | Artifacts must declare valid parents, hashes, schema ids, and creation context. | Broken, missing, cyclic, or unverifiable lineage. |
| evidence-level validation | Claims must be tagged with supported evidence level and uncertainty. | Unsupported claim presented as validated truth. |
| candidate/capital isolation | Research outputs must remain isolated from candidate promotion and capital allocation surfaces. | Any output can be consumed as candidate, sleeve, capital, or trade authority. |

Gate results must be included in the run ledger and report. A failed gate prevents lifecycle advancement, backlog completion, and downstream artifact consumption.

## 7. Scheduler Model

This section defines future command shape only. No scheduler is implemented or enabled by this design.

Future command interface:

```bash
python -m constellation_2.common.atlas_v2_research_os.orchestrator --run-once
python -m constellation_2.common.atlas_v2_research_os.orchestrator --dry-run
python -m constellation_2.common.atlas_v2_research_os.orchestrator --audit-only
```

Expected semantics:

- `--run-once`: acquire lock, select one eligible backlog item, execute one bounded worker, validate artifacts, update state, write ledger and reports.
- `--dry-run`: evaluate what would run and why, but do not invoke workers, write research artifacts, update lifecycle, or update backlog completion state.
- `--audit-only`: inspect existing orchestrator state, ledgers, artifacts, lineage, and safety gates without selecting new work.

Future systemd timer shape, not enabled:

```ini
# atlas-v2-autonomous-orchestrator.service
[Unit]
Description=Atlas V2 Autonomous Orchestrator Run Once

[Service]
Type=oneshot
WorkingDirectory=/home/node/constellation
ExecStart=/usr/bin/python3 -m constellation_2.common.atlas_v2_research_os.orchestrator --run-once
```

```ini
# atlas-v2-autonomous-orchestrator.timer
[Unit]
Description=Atlas V2 Autonomous Orchestrator Hourly Timer

[Timer]
OnCalendar=hourly
Persistent=false
RandomizedDelaySec=120

[Install]
WantedBy=timers.target
```

Future cron shape, not enabled:

```cron
# Not enabled. Example only.
7 * * * * cd /home/node/constellation && /usr/bin/python3 -m constellation_2.common.atlas_v2_research_os.orchestrator --run-once
```

Hourly operation must remain disabled until the certification plan in this document is complete.

## 8. Observability

The orchestrator should emit machine-readable and human-readable reports for every attempted run.

Report paths:

```text
reports/atlas_v2_autonomous_orchestrator/YYYY-MM-DD/run_<run_id>.json
reports/atlas_v2_autonomous_orchestrator/YYYY-MM-DD/run_<run_id>_summary.md
reports/atlas_v2_autonomous_orchestrator/latest.json
reports/atlas_v2_autonomous_orchestrator/latest_summary.md
```

The JSON report should mirror the run ledger and include normalized metrics. The summary report should explain the trigger, selected work, gates, outputs, skip or failure reason, and next required operator action if any.

Required metrics:

| Metric | Description |
| --- | --- |
| run count | Total orchestrator attempts over the reporting window. |
| success/failure count | Count by terminal status, including skipped and partial runs. |
| safety failures | Count by failed safety gate. |
| duplicate skips | Count of skipped attempts caused by idempotency checks. |
| artifacts created | Count and type of artifacts created by successful or partial runs. |
| lifecycle transitions | Count of learning lifecycle state transitions caused by successful runs. |
| backlog progress | Items completed, blocked, skipped, newly eligible, and remaining. |
| learning value estimate | Estimated learning value of completed work, with method and uncertainty. |
| candidate-impact proxy, if available | Non-authoritative estimate of downstream candidate relevance, explicitly not promotion authority. |

Observability must preserve the separation between research insight and operational authority. Metrics can describe potential learning value or candidate-impact proxy, but cannot become candidate promotion, capital allocation, or trade advice.

## 9. Certification Plan

Hourly operation is prohibited until all certifications pass and are recorded as evidence.

### Dry-Run Certification

Requirements:

- `--dry-run` performs selection and gate preflight without invoking workers.
- It writes only dry-run ledger/report artifacts.
- It creates no research, candidate, capital, sleeve, paper-position, or trade artifacts.
- It produces deterministic decisions for equivalent inputs.

### Single-Run Certification

Requirements:

- `--run-once` executes exactly one eligible backlog item.
- It invokes only an approved worker.
- It writes expected research artifacts with valid schemas and lineage.
- It updates lifecycle and backlog state only after all gates pass.
- It writes complete ledger and report records.

### Repeated-Run Certification

Requirements:

- Repeated runs do not reprocess completed equivalent work.
- Locking prevents overlap.
- Backlog state advances monotonically.
- Metrics aggregate correctly across runs.

### Safety-Gate Failure Certification

Requirements:

- Each mandatory safety gate has a fixture that fails closed.
- Gate failures prevent lifecycle advancement and backlog completion.
- Failed artifacts, if any, are blocked from downstream consumption.
- Ledger and reports identify the failed gate and evidence.

### No-Forbidden-Artifact Certification

Requirements:

- Test fixtures attempt to create candidate, capital, sleeve, paper-position, trade, broker, and live recommendation artifacts.
- Each attempt is blocked before downstream consumption.
- The forbidden artifact audit records the exact blocked path, schema, or semantic type.

### Lineage Integrity Certification

Requirements:

- Valid artifacts pass hash, parent, schema, and lineage checks.
- Missing parent, invalid hash, cyclic lineage, and unknown schema fixtures fail.
- Lineage validation result is included in every run ledger.

### Duplicate-Control Certification

Requirements:

- Equivalent trigger, backlog item, worker, input, governance, and runtime graph hashes produce a duplicate skip.
- Different legitimate inputs produce different idempotency keys.
- Concurrent duplicate attempts result in one lock holder and one or more clean skips.

## Explicit Non-Goals For This Design

- No scheduler is implemented.
- No autonomous loop is enabled.
- No production code is changed.
- No existing runtime modules are modified.
- No capital artifacts are created.
- No sleeve artifacts are created.
- No trade artifacts are created.
- No candidate-promotion artifacts are created.
- No readiness is inferred from code.

