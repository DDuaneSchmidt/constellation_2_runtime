---
id: C2_CASH_LEDGER_FAILURE_CONTRACT_V1
title: "C2 Cash Ledger Failure Contract v1"
status: DRAFT
version: 1
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Cash Ledger Failure Contract v1

## Purpose

Define a fail-closed, audit-grade mechanism for recording invariant violations in the Cash Ledger spine without overwriting immutable truth artifacts.

This contract introduces the canonical failure artifact:

- `constellation_2/runtime/truth/cash_ledger_v1/failures/<DAY_UTC>/failure.json`

Governed schema:

- `governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_failure.v1.schema.json`

## Non-negotiable invariants

For Cash Ledger Snapshot v1 at:

- `constellation_2/runtime/truth/cash_ledger_v1/snapshots/<DAY_UTC>/cash_ledger_snapshot.v1.json`

The following invariants must hold:

1) **Day integrity — observed_at_utc**
- The snapshot field `snapshot.observed_at_utc` MUST begin with `<DAY_UTC>T` (same day).

2) **Day integrity — produced_utc**
- The top-level `produced_utc` MUST be deterministic and MUST equal:
  - `<DAY_UTC>T00:00:00Z`

3) **Operator statement day integrity**
- If operator-statement mode is used:
  - Operator statement `observed_at_utc` MUST begin with `<DAY_UTC>T`.
  - Otherwise the run FAILS CLOSED and emits a failure artifact.

4) **Immutability**
- No snapshot rewrite is permitted if bytes differ.
- If a prior snapshot exists and violates invariants, a failure artifact MUST be emitted for that day and downstream gates MUST reject readiness.

## Failure behavior

On any invariant violation or corrupt input:

- The runner MUST write:
  - `constellation_2/runtime/truth/cash_ledger_v1/failures/<DAY_UTC>/failure.json`
- The runner MUST exit non-zero.
- Downstream readiness logic MUST treat the cash ledger for that day as invalid.

## Downstream enforcement requirements

At minimum, the following must fail closed when cash snapshot invariants are violated:

- `ops/tools/run_operator_daily_gate_v1.py`
- `ops/tools/run_pipeline_manifest_v2.py`

They MUST validate that:
- snapshot exists
- snapshot day integrity invariants hold
- otherwise record blocking reason codes and prevent READY.
