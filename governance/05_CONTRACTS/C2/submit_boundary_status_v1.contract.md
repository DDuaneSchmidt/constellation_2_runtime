---
id: C2_SUBMIT_BOUNDARY_STATUS_V1
title: "C2 Submit Boundary Status Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_fact_plane
---

# C2 Submit Boundary Status Contract (V1)

## Purpose

This contract defines the canonical source-authoritative fact surface that records whether the governed paper submit boundary is satisfied for a requested paper-session scope.

This surface is narrow submit-boundary fact only.

It is not the submission itself, not session admission, and not paper-session operational authority.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_submit_boundary_status_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/submit_boundary_status_v1/<DAY>/submit_boundary_status.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json`

## Required inputs

- `startup_materialization_v1`
- `paper_trading_posture_v1`
- `global_kill_switch_state_v1`
- `trade_submit_readiness_c2_v1` for the canonical PAPER account

The runtime-control kernel may be used internally by the canonical writer to derive
submit consequence, but `runtime_control_record_v1` is local submit-boundary evidence
and must not be promoted as a target-day build/admission dependency.

## Required meaning

The artifact must identify:

- `day_utc`
- `session_id`
- `submission_authorized`
- closed `boundary_status`
- `required_boundary_checks[]`
- `failed_checks[]`
- stable `blocking_codes[]`
- producer identity
- `produced_at_utc`
- explicit `freshness_verdict`
- explicit `linkage_verdict`
- explicit `authority_scope = NON_AUTHORITY_FACT`

## Allowed status values

- `AUTHORIZED`
- `DENIED`
- `BLOCKED`
- `UNKNOWN`
- `STALE`
- `MALFORMED`

## Fail-closed rules

- Missing startup materialization must not yield `AUTHORIZED`.
- Missing paper-trading posture must not yield `AUTHORIZED`.
- Missing global kill-switch state must not yield `AUTHORIZED`.
- `global_kill_switch_state_v1.state != INACTIVE` must not yield `AUTHORIZED`.
- `global_kill_switch_state_v1.allow_entries != true` must not yield `AUTHORIZED`.
- Missing or incompatible trade submit readiness must not yield `AUTHORIZED`.
- `submission_authorized=true` is allowed only when all required boundary checks pass.
- Missing or unknown checks must not yield authorization.
- This surface must never publish `READY`, `ADMITTED`, or paper-session operational authority claims.

## Freshness and linkage

- Freshness is current only when all required input artifacts bind the requested `day_utc`.
- Linkage is valid only when all required input artifacts bind the same day-scoped paper-session identity convention:
  - `paper_session:<DAY>:PAPER`

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `blocking_codes[]`, `required_boundary_checks[]`, and `failed_checks[]` must be stable and sorted.

## Consumers

- `ops/tools/run_session_readiness_refresh_v1.py`
- `paper_session_ledger_v1`

## Downstream prohibition

Consumers must not infer submit-boundary satisfaction directly from raw startup, posture, or trade-submit-readiness inputs when this governed surface is available.
Consumers must not treat submit-boundary-local runtime-control evidence as an undeclared
activation dependency when this governed surface is available.

## Migration note

This contract formalizes a submit-boundary fact surface that did not previously exist in source authority. It narrows current boundary evidence into one governed fact without introducing paper-session authority semantics.
