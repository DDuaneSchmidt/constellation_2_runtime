---
id: C2_TRADING_DAY_EXECUTION_CONTROL_PLANE_V1
title: "C2 Trading Day Execution Control Plane Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_trading_day_execution_control_plane
---

# C2 Trading Day Execution Control Plane Contract (V1)

## Purpose

This contract defines the supporting migration-era daily execution-control artifact for
paper-trading day start.

`trading_day_execution_control_plane_v1` remains a supporting internal answer to:

- whether the paper-trading day may start
- whether intents generation completed or valid-zero intent was established
- what the first true canonical blocker is
- whether the block is valid or caused by defect
- what the supporting daily execution-control verdict was during migration

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_trading_day_execution_control_plane_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/trading_day_execution_control_plane_v1/<DAY>/trading_day_execution_control_plane.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_execution_control_plane.v1.schema.json`

## Required inputs

- `intents_day_completeness_v1`
- supporting `paper_day_control_plane_v1`
- supporting `trading_day_control_plane_v1`
- supporting `paper_session_ledger_v1`
- supporting `startup_proof_validation_v1`

## Authority classification

- This surface is not the top-level daily startup/day-start authority owner once
  `trading_day_state_machine_v1` exists.
- `paper_day_control_plane_v1` remains a supporting internal daily control artifact.
- `trading_day_control_plane_v1` remains a supporting migration-era daily control artifact.
- `trading_day_state_machine_v1` is the sole top-level daily startup/day-start authority owner.
- `paper_session_ledger_v1` remains a supporting canonical paper-session authority input.
- Legacy and superseded startup/day-state surfaces are diagnostic only when this artifact exists.

## Required meaning

The execution control plane must identify:

- `day_utc`
- deterministic `day_attempt_id`
- deterministic `execution_control_plane_id`
- one captured `evaluated_at_utc`
- explicit same-day latest-authoritative supersession semantics
- upstream completeness result
- supporting daily control references
- supporting canonical regeneration results
- supporting session-authority outcome
- first true blocker with classification
- startup proof outcome
- final daily execution start decision
- ignored legacy and superseded startup/day-start surfaces
- embedded derived daily summary

## Allowed final decisions

- `READY_NOW`
- `BLOCKED_VALID`
- `BLOCKED_BY_DEFECT`

## Allowed blocker classifications

- `UPSTREAM_PREREQUISITE`
- `CANONICAL_POLICY_OR_INPUT`
- `REGENERATION_DEFECT`
- `SUPPORTING_AUTHORITY_DENY`
- `UNKNOWN`

## Fail-closed rules

- Missing, stale, malformed, contradictory, unlinked, or partial required inputs must not yield
  `READY_NOW`.
- `READY_NOW` is allowed only when:
  - `intents_day_completeness_v1` is `COMPLETE`
  - supporting regeneration artifacts are current-schema compatible
  - supporting `paper_session_ledger_v1` is `GRANTED`
  - supporting `startup_proof_validation_v1` is `STARTUP_READY`
- Missing supporting outputs, schema failures, writer dependency failures, or contradictory
  supporting control results must yield `BLOCKED_BY_DEFECT`.
- No legacy or superseded surface may override this execution control plane.

## Day-attempt and supersession discipline

- `day_attempt_id` must identify the current top-level trading-day execution control run.
- `execution_control_plane_id` must be deterministic from the frozen execution-control contents.
- The canonical per-day artifact path is latest-authoritative for the requested day.
- If a prior same-day artifact existed at the canonical path, the writer must record the prior
  control-plane identifier and `day_attempt_id`.
- This version does not create a historical multi-artifact supersession ledger.

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `blocking_codes[]`, regeneration rows, and ignored surface records must be stable and sorted.
- The supporting daily chain must remain fixed and explicit:
  1. `intents_day_completeness_v1`
  2. `paper_day_control_plane_v1`
  3. `trading_day_control_plane_v1`
- One `evaluated_at_utc` instant must be captured once and reused for the artifact.

## Consumers

- `ops/run/c2_preopen_preflight_v1.sh`
- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/tools/run_operator_summary_v1.py`
- `constellation_2/common/operator_summary_v1.py`

## Legacy and superseded-surface suppression

When `trading_day_state_machine_v1` exists for the day, this artifact and the following surfaces
are not authoritative for startup/day-start:

- `paper_day_control_plane_v1`
- `trading_day_control_plane_v1`
- `trading_day_state_v1`
- `session_readiness_refresh_v1`
- `paper_session_admission_certificate_v1`
- `preopen_operator_summary_v1`
- `preopen_preflight_v1`
- `day_start_blocked_v1`

## Migration note

This contract remains during migration as a supporting execution-control artifact under
`trading_day_state_machine_v1` without removing the supporting `paper_day_control_plane_v1`,
`trading_day_control_plane_v1`, or `paper_session_ledger_v1` artifacts.
