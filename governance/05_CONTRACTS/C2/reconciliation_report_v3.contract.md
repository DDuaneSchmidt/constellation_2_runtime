---
id: C2_RECONCILIATION_REPORT_CONTRACT_V3
title: "Reconciliation Report v3 (SAFE_IDLE Semantics; Forward-Only Readiness)"
status: CANONICAL
version: 3
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - reconciliation
  - broker-truth
  - audit-grade
  - deterministic
  - fail-closed
  - safe-idle
  - forward-only
---

# Reconciliation Report v3 Contract

## 0. Motivation (Forward-Only)

Reconciliation v2 is permanently FAIL in SAFE_IDLE because it always requires broker truth and unimplemented cash/positions captures.
v3 provides forward-only readiness semantics while preserving immutable v2 history.

## 1. Output (immutable truth)

Writes an immutable artifact at:

- `constellation_2/runtime/truth/reports/reconciliation_report_v3/<DAY>/reconciliation_report.v3.json`

Schema:

- `governance/04_DATA/SCHEMAS/C2/REPORTS/reconciliation_report.v3.schema.json`

## 2. Inputs

Truth side:
- `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/` directory

Broker side (required only if submissions_total > 0):
- `execution_evidence_v1/broker_events/<DAY>/broker_event_log.v1.jsonl`
- `execution_evidence_v1/broker_events/<DAY>/broker_event_day_manifest.v1.*.json` with `status == OK`

## 3. Determinism

- `produced_utc` MUST be deterministic: `<DAY>T00:00:00Z`
- All lists must be sorted deterministically.

## 4. SAFE_IDLE rule (required)

Let `submissions_total` be the count of internal submission directories for day.

If `submissions_total == 0`:
- Broker truth is NOT required.
- Cash/positions broker truth capture is NOT required.
- Report MUST be `status == OK`.
- Comparisons MUST set `status == SKIPPED_SAFE_IDLE` with an explanatory reason.

## 5. Active-trading rule (required)

If `submissions_total > 0`:
- Broker truth is REQUIRED.
- Missing broker truth MUST FAIL.
- Missing cash/positions capture MUST FAIL until implemented.

## 6. Prohibited behavior

- v3 MUST NOT overwrite any v2 artifacts.
- v3 MUST NOT declare OK when submissions_total > 0 and broker truth is missing.
