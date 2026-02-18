---
id: C2_OPERATOR_DAILY_GATE_CONTRACT_V2
title: "Operator Daily Gate v2 (SAFE_IDLE aware; forward-only readiness)"
status: CANONICAL
version: 2
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - operator
  - gate
  - audit-grade
  - deterministic
  - fail-closed
  - safe-idle
  - forward-only
---

# Operator Daily Gate v2 Contract

## 0. Motivation

Operator Daily Gate v1 references reconciliation_report_v1 and legacy assumptions that are not SAFE_IDLE compatible.
v2 is a forward-only artifact that evaluates day readiness inputs using reconciliation v3 and envelope v2.

## 1. Output

Writes immutable truth:

- `constellation_2/runtime/truth/reports/operator_daily_gate_v2/<DAY>/operator_daily_gate.v2.json`

Schema:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_daily_gate.v2.schema.json`

## 2. Determinism

- `produced_utc = <DAY>T00:00:00Z`
- reason_codes are de-duplicated and sorted.

## 3. Required checks for PASS

PASS requires ALL:

- Cash ledger snapshot exists AND day-integrity holds AND no failure.json exists
- Positions snapshot exists (v3 preferred else v2)
- Allocation summary exists
- Capital risk envelope v2 exists AND status == PASS
- Reconciliation report v3 exists AND status == OK

## 4. SAFE_IDLE

If submissions_total == 0 and the above PASS checks hold, status MUST be PASS.
