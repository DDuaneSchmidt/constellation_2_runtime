---
id: C2_MULTI_SLEEVE_ROLLUP_POINTER_INDEX_CONTRACT_V1
title: "C2 Multi-Sleeve Rollup Pointer Index Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Multi-Sleeve Rollup Pointer Index Contract v1

## Purpose

Define an append-only, day-keyed pointer index for the global multi-sleeve rollup so operators and automation can deterministically resolve the authoritative rollup artifact for a day without scanning directories.

## Canonical truth locations

Global rollup artifact (day-keyed):
- `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<DAY>/sleeve_rollup.v1.json`

Global rollup pointer index (append-only, day-keyed):
- `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<DAY>/canonical_pointer_index.v1.jsonl`

Pointer lock file (exclusive create/delete, day-keyed):
- `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<DAY>/.canonical_pointer_index.v1.lock`

## Invariants

### C2_ROLLUP_POINTER_APPEND_ONLY_V1
- Pointer index is JSONL.
- Writes are append-only.
- Each entry increments `pointer_seq` monotonically for the day.

### C2_POINTER_ENTRY_MIN_FIELDS_V1
Each pointer entry MUST include at least:
- `schema_id` = `C2_SLEEVE_ROLLUP_POINTER_INDEX_V1`
- `pointer_seq` (int)
- `day_utc` (YYYY-MM-DD)
- `produced_utc` (ISO-8601 UTC Z)
- `status` in {PASS, DEGRADED, FAIL, ABORTED}
- `points_to` (absolute path)
- `points_to_sha256` (sha256 of bytes at points_to)
- `producer` (repo + module)

### C2_FAIL_CLOSED_V1
- If pointer index is missing, malformed, or points_to is missing, the system MUST fail closed in preopen gates.

## Non-claims
- This contract does not define the internal schema of the sleeve rollup beyond required fields.
- This contract defines pointer index determinism and audit semantics only.
