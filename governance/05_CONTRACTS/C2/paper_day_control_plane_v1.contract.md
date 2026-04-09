---
id: C2_PAPER_DAY_CONTROL_PLANE_V1
title: "C2 Paper Day Control Plane Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_day_control_plane
---

# C2 Paper Day Control Plane Contract (V1)

## Purpose

This contract defines the supporting daily regeneration-and-control artifact for paper trading.

`paper_day_control_plane_v1` is not the top-level operator-facing daily answer once
`trading_day_state_machine_v1` exists for the day.

It remains the governed supporting control artifact that records:

- prerequisite gate outcome
- canonical regeneration results
- supporting paper-session authority outcome
- startup proof outcome

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_paper_day_control_plane_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/paper_day_control_plane_v1/<DAY>/paper_day_control_plane.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json`

## Required inputs

- `intents_day_completeness_v1`
- `startup_materialization_v1`
- `paper_trading_posture_v1`
- `submit_boundary_status_v1`
- `paper_session_ledger_v1`
- `startup_proof_validation_v1`

## Authority classification

- This surface is a supporting daily control artifact.
- `trading_day_state_machine_v1` is the sole top-level day-start authority owner where present.
- `paper_session_ledger_v1` remains a supporting canonical paper-session authority input.
- Legacy startup/day-state surfaces are diagnostic only when `trading_day_state_machine_v1` exists.

## Allowed final decisions

- `READY_NOW`
- `BLOCKED_VALID`
- `BLOCKED_BY_DEFECT`

## Fail-closed rules

- Missing, stale, malformed, contradictory, unlinked, or partial prerequisite inputs must not
  yield `READY_NOW`.
- `READY_NOW` is allowed only when:
  - prerequisite gate passes
  - `paper_session_ledger_v1` is `GRANTED`
  - `startup_proof_validation_v1` is `STARTUP_READY`
- Regeneration defects, schema failures, missing writer outputs, or contradictory canonical results
  must yield `BLOCKED_BY_DEFECT`.

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- The canonical regeneration sequence must run in fixed order:
  1. `startup_materialization_v1`
  2. `paper_trading_posture_v1`
  3. `submit_boundary_status_v1`
  4. `paper_session_ledger_v1`
  5. `startup_proof_validation_v1`
- `blocking_codes[]` and ignored legacy surface records must be stable and sorted.

## Consumers

- `ops/tools/run_trading_day_control_plane_v1.py`
- `ops/run/c2_preopen_preflight_v1.sh`
- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/tools/run_operator_summary_v1.py`

## Legacy-surface suppression

When `trading_day_state_machine_v1` exists for the day, the following surfaces are not
authoritative for startup/day-start:

- `paper_day_control_plane_v1`
- `trading_day_state_v1`
- `session_readiness_refresh_v1`
- `paper_session_admission_certificate_v1`
- `preopen_operator_summary_v1`
- `preopen_preflight_v1`
- `day_start_blocked_v1`

## Migration note

This contract remains as a supporting internal control artifact after the introduction of
`trading_day_state_machine_v1`.

Longer-term execution-ledger unification remains a separate design step.
