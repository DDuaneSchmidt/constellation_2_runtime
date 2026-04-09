---
id: C2_STARTUP_PROOF_VALIDATION_V1
title: "C2 Startup Proof Validation Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_startup_proof_validation
---

# C2 Startup Proof Validation Contract (V1)

## Purpose

This contract defines the bounded source-authoritative startup proof-validation surface for the
touched paper-session control plane.

It is not session authority.

It validates only whether the beginning-of-day control-plane prerequisites already owned by the
current paper-session ledger path are present and consistent.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_startup_proof_validation_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/startup_proof_validation_v1/<DAY>/startup_proof_validation.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_proof_validation.v1.schema.json`

## Authority classification

- This surface is non-authoritative.
- `trading_day_state_machine_v1` is the authoritative day-start control owner for the touched
  startup path.
- `paper_day_control_plane_v1` remains a supporting daily control artifact.
- `paper_session_ledger_v1` remains the supporting canonical paper-session authority input.

## Required inputs

- authoritative-source repo proof in `authoritative_source_only` mode
- `startup_materialization_v1`
- `paper_session_ledger_v1`

## Allowed status values

- `STARTUP_READY`
- `STARTUP_BLOCKED`

## Fail-closed rules

- Missing, malformed, stale, unlinked, inconsistent, or denied required inputs must produce
  `STARTUP_BLOCKED`.
- This surface must not claim full paper-session proof completeness.
- This surface must not publish readiness, admission, or authority semantics independent of the
  ledger.

## Freshness and linkage

- Shared paper-session linkage convention:
  - `paper_session:<DAY>:PAPER`
- Required ledger and startup-materialization inputs must bind the requested `day_utc` and session.

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `checks[]` and `blocking_codes[]` must be stable and sorted.
- One `produced_at_utc` instant must be captured once and reused for the artifact.

## Consumers

- `ops/tools/run_paper_day_control_plane_v1.py`
- `ops/tools/run_trading_day_control_plane_v1.py`
- `ops/tools/run_trading_day_execution_control_plane_v1.py`
- `ops/tools/run_trading_day_state_machine_v1.py`
- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/run/c2_preopen_preflight_v1.sh`
- `constellation_2/common/operator_summary_v1.py`

## Migration note

This contract exists to repair the current startup control plane without inventing paper-session
execution-ledger semantics that are not yet source-authoritatively proven.
