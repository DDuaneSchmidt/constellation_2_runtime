---
doc_kind: contract
doc_id: C2_OPERATOR_FUTURE_DAY_OVERRIDE_CONTRACT_V1
title: "Constellation 2.0 — Operator Future-Day Override v1 (PAPER-only, explicit, immutable)"
version: 1
status: draft
created_utc: 2026-02-28T00:00:00Z
repo_root_authoritative: /home/node/constellation_2_runtime
scope:
  - "Allows a PAPER-only, explicit override to permit future-day operational writes for a specific DAY_UTC"
non_goals:
  - "No override for LIVE mode"
  - "No implicit rollover to future day without explicit operator artifact"
  - "No bypass of other gates"
---
# Operator Future-Day Override v1

## Motivation

Operational writers are required to fail-closed when `day_utc > today_utc` (UTC) via `enforce_operational_day_key_invariant_v1`. This prevents accidental future-day truth creation.

However, paper-trading operations can encounter a *poisoned today* where immutable, historically produced gate artifacts make the current day unusable. In that case, continuing paper operations requires an explicit, auditable, operator-approved exception.

## Contract

### 1) Override artifact (authoritative)

Path:
- `constellation_2/runtime/truth/reports/operator_future_day_override_v1/<DAY_UTC>/operator_future_day_override.v1.json`

Schema:
- `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_future_day_override.v1.schema.json`

Properties:
- Immutable per day key (must not be rewritten)
- Must explicitly bind:
  - `override_day_utc` == `<DAY_UTC>`
  - `mode` == `"PAPER"`
  - `operator_id` non-empty
  - `reason` non-empty
  - `acknowledgments[]` includes at least:
    - "I acknowledge future-day truth creation is normally forbidden."
    - "I authorize PAPER-only future-day writes for the specified day key."

### 2) Enforcement behavior

The helper `enforce_operational_day_key_invariant_v1(day_utc)` MUST:
- Continue to fail-closed for any future day by default.
- Permit `day_utc > today_utc` ONLY when:
  - `C2_MODE` environment variable equals `"PAPER"`, AND
  - the override artifact exists at the path above for `day_utc`, AND
  - the artifact is schema-valid and binds `override_day_utc == day_utc` and `mode == "PAPER"`.

If any of these conditions are not satisfied:
- Fail closed with `FAIL: FUTURE_DAY_UTC_DISALLOWED ...` or `FAIL: FUTURE_DAY_OVERRIDE_INVALID ...`

### 3) Producer tool

A producer tool MUST exist to write the override immutably:
- `ops/tools/run_operator_future_day_override_v1.py`

It MUST:
- validate against schema at write time
- write immutable artifact under the required path
- print `OK: OPERATOR_FUTURE_DAY_OVERRIDE_V1_WRITTEN ... sha256=...`

## Security / Audit

- Override artifacts are PAPER-only and must never be used to relax LIVE protections.
- Override is day-scoped and must not enable arbitrary future-day writes across multiple days.
- Override must be explicit and immutable, ensuring audit traceability.
