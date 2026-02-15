---
id: C2_F_ACCOUNTING_SCHEMA_V1
title: "Bundle F — Accounting Spine Schemas (Audit Grade)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Purpose

This document defines the **audit-grade schema contracts** for Bundle F output artifacts.

Bundle F outputs must:
- be deterministic and replayable,
- embed input bindings (paths + sha256),
- embed producer identity (git sha),
- support fail-closed semantics via explicit status codes and reason codes.

These schemas are **JSON object shapes** with strict field requirements. Bundle F implementation must validate outputs against these schemas before writing truth.

# 2. Canonical Output Paths (Schema Targets)

All paths are under:

`constellation_2/runtime/truth/accounting_v1/`

Artifacts:

1) NAV:
- `nav/<DAY_UTC>/nav.json`

2) Exposure:
- `exposure/<DAY_UTC>/exposure.json`

3) Attribution:
- `attribution/<DAY_UTC>/engine_attribution.json`

4) Latest pointer:
- `latest.json`

5) Failure artifact:
- `failures/<DAY_UTC>/failure.json`

# 3. Common Conventions

## 3.1 Canonical JSON serialization (determinism)

Implementation must serialize JSON deterministically:
- UTF-8
- newline at end of file
- sorted object keys
- no NaN/Infinity
- numbers must be finite
- consistent formatting (avoid scientific notation)

## 3.2 Required common fields

All Bundle F artifacts must include:

- `schema_id` (string)
- `schema_version` (integer)
- `produced_utc` (string) — ISO-8601 UTC timestamp
- `day_utc` (string) — day key `YYYY-MM-DD`
- `producer` (object)
- `status` (string)
- `reason_codes` (array[string])
- `input_manifest` (array[object])

## 3.3 Producer object schema

`producer` object:

- `repo` (string)
- `git_sha` (string)
- `module` (string)
- `build` (object) — optional; if present must be deterministic

## 3.4 Input manifest entry schema

Each entry must include:

- `type` (string) — one of allowlist
- `path` (string)
- `sha256` (string)
- `day_utc` (string|null)
- `producer` (string)

## 3.5 Status codes (allowed)

- `OK`
- `DEGRADED_MISSING_MARKS`
- `DEGRADED_MISSING_LIFECYCLE`
- `FAIL_CORRUPT_INPUTS`
- `FAIL_SCHEMA_VIOLATION`

# 4. NAV Artifact Schema (`nav.json`)

## 4.1 Schema identity

- `schema_id`: `C2_ACCOUNTING_NAV_V1`
- `schema_version`: 1

## 4.2 Required fields

Top-level object:

- common fields (§3.2)
- `nav` (object)
- `history` (object)

### nav object

- `currency` (string)
- `nav_total` (number)
- `cash_total` (number)
- `gross_positions_value` (number)
- `realized_pnl_to_date` (number)
- `unrealized_pnl` (number)
- `components` (array[object])
- `notes` (array[string])

### history object (DRAW DOWN — CANONICAL)

Canonical drawdown convention is governed by:
- `C2_DRAWDOWN_CONVENTION_V1`

Fields:
- `peak_nav` (number)
- `drawdown_abs` (number)
- `drawdown_pct` (string, decimal with exactly 6 dp; e.g. `-0.080000`)

Rules (Blocker A hard requirement):
- Once `nav_total` exists for the day, `peak_nav`, `drawdown_abs`, and `drawdown_pct` **must be populated**.
- `drawdown_pct` must be computed deterministically and quantized to 6 decimals (ROUND_HALF_UP).
- If drawdown cannot be computed due to missing or corrupt NAV history inputs, Bundle F must FAIL-CLOSED and write a failure artifact (no nav.json written for that day).

# 5. Exposure Artifact Schema (`exposure.json`)

(unchanged)

# 6. Attribution Artifact Schema (`engine_attribution.json`)

(unchanged)

# 7. Mark Record Schema (embedded)

(unchanged)

# 8. Latest Pointer Schema (`latest.json`)

(unchanged)

# 9. Failure Artifact Schema (`failure.json`)

(unchanged)

# 10. Versioning & Backward Compatibility

- All schema ids are versioned with `_V1`.
- Breaking schema change requires `_V2`.

# 11. Acceptance Tests (Schema-level)

Bundle F must include tests proving:
- produced artifacts always contain required common fields
- drawdown fields are populated and non-null when nav exists
- determinism: repeated runs yield identical bytes
