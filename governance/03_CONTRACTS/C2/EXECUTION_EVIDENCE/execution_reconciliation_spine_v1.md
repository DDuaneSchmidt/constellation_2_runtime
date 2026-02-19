---
id: C2_EXECUTION_RECONCILIATION_SPINE_V1
title: "Execution Reconciliation Spine v1"
status: DRAFT
version: 1
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Execution Reconciliation Spine v1

## Purpose
Compare broker-truth snapshots against internal truth artifacts to detect drift.

## Inputs
- Submission evidence:
  `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/`
- Execution stream:
  `constellation_2/runtime/truth/execution_stream_v1/<DAY>/`
- Fill ledgers:
  `constellation_2/runtime/truth/fill_ledger_v1/<DAY>/`

## Output
- Reconciliation report:
  `constellation_2/runtime/truth/reports/execution_reconciliation_v1/<DAY>/execution_reconciliation.v1.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/REPORTS/execution_reconciliation.v1.schema.json`

## Required checks (fail-closed)
- Every broker order_id/perm_id maps to a known submission_id
- Every submission_id has a consistent broker status path
- Fill ledger matches broker executions (qty + price consistency)
- Unknown broker orders => FAIL

## Determinism
- produced_utc is deterministic: `<DAY>T00:00:00Z`
- input_manifest includes sha256 for all referenced roots/files
