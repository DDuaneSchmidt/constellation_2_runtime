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
- consistent float formatting (implementation-defined but stable; avoid scientific notation if possible)

## 3.2 Required common fields

All Bundle F artifacts must include:

- `schema_id` (string) — identifies the schema version
- `schema_version` (integer) — must equal 1 for this doc
- `produced_utc` (string) — ISO-8601 UTC timestamp
- `day_utc` (string) — day key `YYYY-MM-DD`
- `producer` (object) — identity of producing code
- `status` (string) — one of allowed status codes
- `reason_codes` (array[string]) — may be empty
- `input_manifest` (array[object]) — hash-bound inputs

## 3.3 Producer object schema

`producer` object:

- `repo` (string) — fixed identifier, e.g. `constellation_2_runtime`
- `git_sha` (string) — git commit sha of producer
- `module` (string) — producing module name (e.g. `ops/accounting/run_accounting_v1.py`)
- `build` (object) — optional, can include python version, etc. If present must be deterministic

## 3.4 Input manifest entry schema

Each entry must include:

- `type` (string) — one of:
  - `cash_ledger`
  - `marks_underlying`
  - `marks_options`
  - `positions_truth`
  - `fills_truth`
  - `lifecycle_truth`
  - `trade_intents`
  - `order_plans`
  - `broker_submissions`
  - `other`

- `path` (string) — repo-relative canonical path
- `sha256` (string) — lowercase hex sha256 of file bytes
- `day_utc` (string|null) — day key if day-scoped, else null
- `producer` (string) — contract id or module name if known, else `unknown`

## 3.5 Status codes (allowed)

- `OK`
- `DEGRADED_MISSING_MARKS`
- `DEGRADED_MISSING_LIFECYCLE`
- `FAIL_CORRUPT_INPUTS`
- `FAIL_SCHEMA_VIOLATION`

Additional reason codes may be used; they must be stable strings and documented either here or in Bundle G reason registry for cross-bundle alignment.

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

- `currency` (string) — e.g. `USD`
- `nav_total` (number) — total NAV
- `cash_total` (number) — cash component
- `gross_positions_value` (number) — sum of marked position values (can be negative if net short)
- `realized_pnl_to_date` (number) — realized P&L through day (may be incomplete if degraded)
- `unrealized_pnl` (number) — unrealized P&L at marks
- `components` (array[object]) — optional breakdown entries (may be empty)
- `notes` (array[string]) — optional (may be empty)

Each `components[]` entry (if present):
- `kind` (string) — e.g. `equity`, `option_leg`, `spread`
- `symbol` (string)
- `qty` (number)
- `mark` (object) — see §7 marking record
- `mv` (number) — marked value contribution

### history object

- `peak_nav` (number|null)
- `drawdown_abs` (number|null)
- `drawdown_pct` (number|null)

Rules:
- If `peak_nav` is null, drawdown fields must be null and `reason_codes` must include `DEGRADED_HISTORY_INCOMPLETE`.

# 5. Exposure Artifact Schema (`exposure.json`)

## 5.1 Schema identity

- `schema_id`: `C2_ACCOUNTING_EXPOSURE_V1`
- `schema_version`: 1

## 5.2 Required fields

Top-level object:
- common fields (§3.2)
- `exposure` (object)

`exposure` object:
- `currency` (string)
- `defined_risk_total` (number)
- `by_engine` (array[object])
- `by_underlying` (array[object])
- `by_expiry_bucket` (array[object])
- `notes` (array[string])

Each breakdown entry:
- `key` (string) — engine id / symbol / bucket
- `defined_risk` (number)

Rules:
- If any position cannot be classified into defined-risk deterministically, status must be `FAIL_SCHEMA_VIOLATION` OR `FAIL_CORRUPT_INPUTS` (implementation decides based on root cause), and the run must not write `latest.json`.

# 6. Attribution Artifact Schema (`engine_attribution.json`)

## 6.1 Schema identity

- `schema_id`: `C2_ACCOUNTING_ATTRIBUTION_V1`
- `schema_version`: 1

## 6.2 Required fields

Top-level object:
- common fields (§3.2)
- `attribution` (object)

`attribution` object:
- `currency` (string)
- `by_engine` (array[object])
- `notes` (array[string])

Each `by_engine` entry:
- `engine_id` (string)
- `realized_pnl_to_date` (number)
- `unrealized_pnl` (number)
- `defined_risk_exposure` (number)
- `positions_count` (integer)
- `symbols` (array[string])

Degraded rule:
- If attribution inputs are missing, this artifact may still be produced with reduced fields only if schema remains satisfied. Otherwise use `DEGRADED_*` with explicit reason codes.

# 7. Mark Record Schema (embedded)

Wherever a mark is included (e.g., in nav components), the mark object must be:

- `bid` (number|null)
- `ask` (number|null)
- `last` (number|null)
- `source` (string) — producer/source identifier
- `asof_utc` (string) — ISO timestamp

Rules:
- In `OK` status, `bid` and `ask` must be non-null for instruments where bid/ask is required by policy.
- If bid/ask missing and fallback used, set status `DEGRADED_MISSING_MARKS`.

# 8. Latest Pointer Schema (`latest.json`)

## 8.1 Schema identity

- `schema_id`: `C2_ACCOUNTING_LATEST_POINTER_V1`
- `schema_version`: 1

## 8.2 Required fields

Top-level object:
- `schema_id`, `schema_version`
- `produced_utc`
- `day_utc`
- `producer` (object, §3.3)
- `status`
- `reason_codes`
- `pointers` (object)

`pointers` object:
- `nav_path` (string)
- `nav_sha256` (string)
- `exposure_path` (string)
- `exposure_sha256` (string)
- `attribution_path` (string)
- `attribution_sha256` (string)

Rule:
- `latest.json` must only be written if the run status is `OK` or an explicitly allowed `DEGRADED_*` (policy defined in Bundle F design; default: write for DEGRADED, but Bundle G will block if not OK).

# 9. Failure Artifact Schema (`failure.json`)

## 9.1 Schema identity

- `schema_id`: `C2_ACCOUNTING_FAILURE_V1`
- `schema_version`: 1

## 9.2 Required fields

Top-level object:
- common fields (§3.2) except:
  - `status` must be one of `FAIL_*`
- `failure` (object)

`failure` object:
- `code` (string) — e.g. `FAIL_CORRUPT_INPUTS`
- `message` (string) — human-readable
- `details` (object) — optional structured details (must be JSON-serializable)
- `attempted_outputs` (array[object]) — list of output paths and their candidate hashes if computed

Each attempted output entry:
- `path` (string)
- `sha256` (string|null)

# 10. Versioning & Backward Compatibility

- All schema ids are versioned with `_V1`.
- Any breaking schema change requires a new schema id `_V2` and a new design doc.
- Bundle G must only depend on fields declared stable here.

# 11. Acceptance Tests (Schema-level)

Bundle F must include tests proving:
- produced artifacts always contain required common fields
- status codes are within allowlist
- pointer file only references immutable day artifacts
- input manifest always includes sha256 and path
