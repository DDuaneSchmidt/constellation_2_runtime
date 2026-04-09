---
id: C2_RECURRENCE_KILL_GATE_V1
title: "C2 Recurrence Kill Gate Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_recurrence_kill_gate
---

# C2 Recurrence Kill Gate Contract (V1)

## Purpose

This contract defines the top-level operational closure gate that prevents false closure.

It is not a business-logic control plane and must only consume existing authoritative sources.

## Truth owner

- Canonical writer:
  - `ops/tools/run_recurrence_kill_gate_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/recurrence_kill_gate_v1/<DAY>/recurrence_kill_gate.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/recurrence_kill_gate.v1.schema.json`

## Required proof conditions

No run may be considered recurrence-safe unless the gate proves all of the following for the
actual live path and target day:

- live entrypoint verified
- target day verified
- exactly one valid terminal state reached
- rerun is idempotent
- recurrence fingerprint computed
- recurrence status determined

## Allowed outputs

- `RECURRENCE_SAFE`
- `MITIGATED_NOT_RECURRENCE_SAFE`
- `INVALID_PROOF`

## Fail-closed rules

- Historical-day proof alone must never yield `RECURRENCE_SAFE`.
- Wrong entrypoint, wrong day, identity mismatch, missing required authoritative artifacts,
  ambiguous terminal state, or missing rerun evidence must yield `INVALID_PROOF`.
- The gate must not recompute readiness or blocker precedence from raw operational inputs.
- `deployment_state_machine_v1` and `trading_day_state_machine_v1` remain the relevant decision
  authorities.

