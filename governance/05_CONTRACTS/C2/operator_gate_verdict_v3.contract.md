---
id: C2_OPERATOR_GATE_VERDICT_CONTRACT_V3
title: "Operator Gate Verdict v3 (SAFE_IDLE aware; forward-only readiness)"
status: CANONICAL
version: 3
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - operator
  - verdict
  - readiness
  - audit-grade
  - deterministic
  - fail-closed
  - safe-idle
  - quarantine
  - forward-only
---

# Operator Gate Verdict v3 Contract

## 0. Motivation

Verdict v2 is coupled to reconciliation_report_v2 and legacy inputs.
v3 is forward-only and enforces the institutional readiness rule using the updated artifacts:
- pipeline_manifest_v2 (updated for envelope v2 / regime v3 / recon v3)
- operator_daily_gate_v2
- reconciliation_report_v3
- quarantine registry

## 1. Output

Writes immutable truth:

- `constellation_2/runtime/truth/reports/operator_gate_verdict_v3/<DAY>/operator_gate_verdict.v3.json`

Schema:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v3.schema.json`

## 2. Policy: operational day invariant

Producer MUST refuse future-day writes via `enforce_operational_day_key_invariant_v1`.

## 3. Quarantine (fail-closed)

If day is present in:
- `governance/02_REGISTRIES/TEST_DAY_KEY_QUARANTINE_V1.json`

Then:
- ready MUST be false
- exit_code MUST be non-zero

## 4. READY rule (status-aware; no silent READY)

ready is true iff ALL are true:

- pipeline_manifest_v2 exists AND status == OK
- operator_daily_gate_v2 exists AND status == PASS
- reconciliation_report_v3 exists AND status == OK

SAFE_IDLE relaxation:
- intents_day_rollup is NOT required when submissions_total == 0

## 5. Determinism

produced_utc is deterministic: `<DAY>T00:00:00Z`.
