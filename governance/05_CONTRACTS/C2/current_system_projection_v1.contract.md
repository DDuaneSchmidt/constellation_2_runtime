---
id: C2_CURRENT_SYSTEM_PROJECTION_V1
title: "C2 Current System Projection Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_current_system_projection
---

# C2 Current System Projection Contract (V1)

## Purpose

This contract defines the derived current-status view for the touched Constellation operational
path.

It answers what is happening now for operators without becoming a second control plane.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_current_system_projection_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/current_system_projection_v1/<DAY>/current_system_projection.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json`

## Authority classification

- This surface is derived-only and non-authoritative.
- `deployment_state_machine_v1`, `trading_day_state_machine_v1`, and `paper_session_ledger_v1`
  remain the relevant authority owners.
- This surface must not recompute readiness, blocker precedence, or admission semantics from raw
  operational inputs.

## Required inputs

- `execution_journal_v1`
- `deployment_state_machine_v1`
- `startup_materialization_v1`
- `startup_proof_validation_v1`
- `paper_session_ledger_v1`
- `trading_day_state_machine_v1`

## Required outputs

The projection must include:

- current deployment status
- current startup status
- current ledger/submission status
- first true blocker code and source
- operator action required
- contradiction details
- provenance for every source artifact used

## Fail-closed rules

- Missing required journal events must fail closed.
- Missing or cross-identity-mismatched supporting snapshots must fail closed.
- Contradictions must be recorded explicitly and must not be hidden inside a single boolean.

