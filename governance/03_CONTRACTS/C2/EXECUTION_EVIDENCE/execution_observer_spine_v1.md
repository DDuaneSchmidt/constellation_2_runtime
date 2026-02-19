---
id: C2_EXECUTION_OBSERVER_SPINE_V1
title: "Execution Observer Spine v1"
status: DRAFT
version: 1
created_utc: 2026-02-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Execution Observer Spine v1

## Purpose
Produce deterministic, audit-proof broker-truth ingestion artifacts for a given day.

This spine is a **pull-based** observer suitable for hostile review:
- No daemon requirement
- Deterministic output naming
- Idempotent ingestion (no duplicates)
- Fail-closed on unknown submissions or schema violations

## Inputs (authoritative)
- IB Paper session reachable (host/port/client_id)
- Existing submission evidence directories under:
  `constellation_2/runtime/truth/execution_evidence_v1/submissions/<DAY>/<submission_id>/`

## Outputs (authoritative)
Writes one file per broker-truth event:

`constellation_2/runtime/truth/execution_stream_v1/<DAY>/<event_hash>.execution_event_stream_record.v1.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json`

## Fail-closed conditions
- Broker event references unknown submission_id / broker_ids not attributable to known submission evidence
- Duplicate event_hash with different bytes
- Any schema validation error
- Any non-JSON-safe raw payload fields (floats forbidden)

## Determinism
- event_hash is derived from canonical fields only (no local clock randomness)
- produced_utc is deterministic: `<DAY>T00:00:00Z`

## Security posture
- No secrets in output
- No credentials persisted
- Only broker metadata and normalized numeric strings
