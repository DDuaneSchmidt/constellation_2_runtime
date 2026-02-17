---
id: C2_NAV_HISTORY_LEDGER_V1
title: "Constellation 2.0 — NAV History Ledger v1 (Derived Index From NAV Snapshot Truth)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - nav
  - ledger
  - index
  - deterministic
  - audit_grade
  - fail_closed
---

# NAV History Ledger v1 — Contract

## 1. Objective
Produce a deterministic day-indexed ledger to accelerate audits/replays.

This ledger MUST be derived ONLY from NAV Snapshot Truth artifacts.

No external data. No direct reads from accounting nav other than via snapshot truth.

## 2. Canonical output paths
- Day ledger:
  - `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_history_ledger/<DAY>/nav_history_ledger.v1.json`
- Latest pointer:
  - `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_history_ledger/latest.json`

## 3. Input source (required)
- NAV Snapshot Truth:
  - `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json`

The ledger generator MUST scan only within the canonical nav_snapshot root and MUST sort day keys deterministically.

## 4. Ledger contents (required)
The day ledger file MUST include:
- `schema_id` = `"C2_NAV_HISTORY_LEDGER_V1"`
- `schema_version` = `1`
- `asof_day_utc` = `<DAY>` (the day this ledger snapshot is written for)
- `days` array (sorted ascending), each element:
  - `day_utc`
  - `snapshot_path`
  - `snapshot_sha256`
  - `end_nav`
  - `peak_nav_to_date`
  - `drawdown_pct`

- `input_manifest` including at minimum:
  - `type: nav_snapshot_day_dir` for the nav snapshot root scanned
  - and `type: output_schema` for the ledger schema

- `produced_utc`, `producer`, and `canonical_json_hash`.

## 5. Latest pointer contents (required)
`latest.json` MUST include:
- `schema_id` = `"C2_NAV_HISTORY_LEDGER_LATEST_POINTER_V1"`
- `schema_version` = `1`
- `day_utc` (the asof day referenced)
- `pointers.ledger_path` (absolute path)
- `pointers.ledger_sha256`
- `produced_utc`, `producer`, and `canonical_json_hash`

## 6. Immutability
- The day ledger is immutable per `asof_day_utc`.
- Latest pointer is also immutable per day; overwrite is prohibited unless identical bytes.

## 7. Schema requirement
The produced artifacts MUST validate against:
- `governance/04_DATA/SCHEMAS/C2/MONITORING/nav_history_ledger.v1.schema.json`
