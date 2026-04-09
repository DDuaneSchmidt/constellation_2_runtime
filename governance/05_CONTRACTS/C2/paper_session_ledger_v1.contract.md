---
id: C2_PAPER_SESSION_LEDGER_V1
title: "C2 Paper Session Ledger Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_ledger
---

# C2 Paper Session Ledger Contract (V1)

## Purpose

This contract defines the canonical paper-session ledger object.

The ledger is the only authoritative paper-session control surface.

For the current source-authoritative design, it remains a supporting canonical authority input to
`trading_day_state_machine_v1`.

It owns:

- session identity
- frozen evidence usability
- monotonic control state
- submit eligibility
- submit lifecycle status
- post-submit lineage linkage where safely provable
- derived operator summary

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_paper_session_ledger_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/paper_session_ledger_v1/<DAY>/paper_session_ledger.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json`

## Required inputs

- `startup_materialization_v1`
- `paper_trading_posture_v1`
- `submit_boundary_status_v1`
- `sleeve_rollup_v1` when present as the strongest governed execution-completion fact
- governed execution-evidence latest-pointer and reconciliation families where safely resolvable for the requested day

## Required meaning

The ledger must identify:

- `day_utc`
- `session_id`
- deterministic `ledger_id`
- one captured `evaluated_at_utc`
- writer provenance
- stable `fact_refs[]`
- embedded `evidence_freeze`
- embedded `control_state`
- embedded `submit_lifecycle`
- embedded `post_submit_lifecycle`
- embedded derived `operator_summary`

### Embedded evidence-freeze section

This section is non-authority evidence usability only.

It must identify:

- deterministic `evidence_digest`
- closed `overall_evidence_status`
- stable `blocking_codes[]`
- per-input presence, schema, linkage, freshness, and duplicate-resolution verdicts
- explicit lookup evidence when an input is missing, malformed, or ambiguous

`READY` means only that the frozen required evidence set is usable.

It must never be interpreted as session authority granted.

### Embedded control-state section

This section is the only authoritative answer to:

- whether the paper session is operationally authorized
- whether submit eligibility is granted
- what the canonical current session state is

It must identify:

- closed `authority_status`
- boolean `system_ready`
- boolean `submission_authorized`
- stable `blocking_codes[]`
- monotonic `transition_history[]`
- current ledger state

## Allowed evidence status values

- `READY`
- `DENY`

## Allowed authority values

- `GRANTED`
- `DENIED`

## Required state machine

Legal transitions only:

- `SESSION_CREATED -> EVIDENCE_FROZEN`
- `EVIDENCE_FROZEN -> INPUTS_VALIDATED`
- `INPUTS_VALIDATED -> EXECUTION_EVALUATED`
- `EXECUTION_EVALUATED -> SUBMIT_BOUNDARY_EVALUATED`
- `SUBMIT_BOUNDARY_EVALUATED -> AUTHORITY_GRANTED | AUTHORITY_DENIED`
- `AUTHORITY_GRANTED -> SUBMIT_SKIPPED | SUBMIT_ATTEMPTED`
- `AUTHORITY_DENIED -> SUBMIT_SKIPPED`
- `SUBMIT_SKIPPED -> SESSION_FINALIZED`
- `SUBMIT_ATTEMPTED -> SESSION_FINALIZED`

Illegal transitions must fail closed.

## Fail-closed rules

- Missing, stale, malformed, schema-incompatible, ambiguous, duplicated, unlinked, or unknown required fact inputs must deny the evidence-freeze section.
- `GRANTED` is allowed only when the frozen evidence section is `READY`.
- `GRANTED` is allowed only when:
  - `startup_materialization_v1` proves `SUCCESS`
  - `paper_trading_posture_v1` proves `system_ready = true`
  - `submit_boundary_status_v1` proves `submission_authorized = true`
- The ledger must not infer success from partial evidence.
- `sleeve_rollup_v1` and post-submit lineage inputs may extend lifecycle visibility when present, but must not silently fabricate completion or reconciliation semantics when absent.

## Freshness and linkage

- Shared paper-session linkage convention:
  - `paper_session:<DAY>:PAPER`
- Required authority inputs must bind the requested `day_utc` and shared session identity.
- Post-submit lineage references must bind the requested day when present. If not safely provable, the ledger must emit explicit gap codes instead of implying successful lineage closure.

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `fact_refs[]` and embedded evidence inputs must be stably sorted by canonical logical name.
- `blocking_codes[]`, `reason_codes[]`, and `gap_codes[]` must be stable and sorted.
- `ledger_id` and `evidence_digest` construction rules must be deterministic and reproducible.
- One evaluation instant must be captured once and reused during ledger generation.
- The writer must resolve all inputs once and freeze them; no lazy rereads for authority.

## Immutability and lifecycle

- The ledger is the only authoritative paper-session object.
- A day-keyed ledger may be recomputed at the same canonical path while its `finalization_status` remains `OPEN`.
- Such same-day rewrites must preserve `ledger_id` and `session_id` and use a non-decreasing `evaluated_at_utc`.
- A day-keyed ledger may advance only monotonically under the legal transition rules until finalized.
- A finalized ledger is immutable.
- No alternate second authority path exists in this version. The canonical day-keyed path remains the only authoritative session-ledger surface.

## Consumers

- `ops/tools/run_paper_day_control_plane_v1.py`
- `ops/tools/run_trading_day_control_plane_v1.py`
- `ops/tools/run_trading_day_execution_control_plane_v1.py`
- `ops/tools/run_trading_day_state_machine_v1.py`
- `ops/tools/run_paper_session_admission_v1.py`
- `ops/tools/run_c2_multi_sleeve_orchestrator_v1.py`
- `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/tools/run_testing_evidence_plane_v1.py`
- derived compatibility views including `paper_open_readiness_v1`

## Downstream prohibition

No downstream surface may claim independent paper-session authority once the ledger exists.

Readiness, admission, refresh, and operator-summary surfaces are derived-only and must not contradict the ledger.

## Migration note

This contract converges prior paper-session control concerns into one canonical session object.

It supersedes public first-class authority semantics previously split across `paper_session_evidence_manifest_v1`, `paper_session_kernel_v1`, and legacy readiness or admission compatibility surfaces.

`trading_day_state_machine_v1` is now the canonical day-start authority owner for the touched
startup path.

`paper_session_execution_ledger_v1` is intentionally not implemented in this contract version.

Future execution-ledger unification remains blocked until source-authoritative paper-session proof,
replay, promotion, and bootstrap semantics are separately proven and governed.
