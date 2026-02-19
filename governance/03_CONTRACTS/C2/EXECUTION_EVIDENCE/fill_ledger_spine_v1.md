---
id: C2_FILL_LEDGER_SPINE_V1
title: "Fill Ledger Spine v1"
status: DRAFT
version: 1
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Fill Ledger Spine v1

## Purpose
Aggregate execution stream records deterministically into per-submission fill ledgers.

## Inputs
- Execution stream records:
  `constellation_2/runtime/truth/execution_stream_v1/<DAY>/`
- Submission evidence identity sets:
  `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/<submission_id>/`

## Outputs
- Fill ledger per submission_id:
  `constellation_2/runtime/truth/fill_ledger_v1/<DAY>/<submission_id>.fill_ledger.v1.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json`

## Invariants (fail-closed)
- Sum(fill_qty) <= order_qty (no overfill)
- filled_qty monotonic non-decreasing across recomputation
- lifecycle_status derived deterministically from order_state + fills
- unknown event_hash or missing broker_ids => FAIL

## Determinism
- Aggregation uses canonical ordering by event_time_utc then event_hash
- produced_utc is deterministic: `<DAY>T00:00:00Z`
