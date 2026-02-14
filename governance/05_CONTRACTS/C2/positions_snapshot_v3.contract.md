---
schema_id: C2_GOV_CONTRACT_V1
schema_version: 1
title: "Positions Snapshot v3 Contract: Instrument Identity from order_plan"
owner: "constellation_2"
status: "ACTIVE"
created_utc: "2026-02-14T00:00:00Z"
---

# Scope

This contract governs the deterministic truth spine output:

- Snapshot: `constellation_2/runtime/truth/positions_v1/snapshots/{DAY_UTC}/positions_snapshot.v3.json`
- Latest pointer: `constellation_2/runtime/truth/positions_v1/latest_v3.json`
- Failure artifact: `constellation_2/runtime/truth/positions_v1/failures/{DAY_UTC}/failure_v3.json`

# Inputs

- Execution Evidence Truth day directory:
  - `constellation_2/runtime/truth/execution_evidence_v1/submissions/{DAY_UTC}/`
- For each submission directory `{SUB_ID}` under the day:
  - `execution_event_record.v1.json`
  - `order_plan.v1.json`

# Deterministic Instrument Identity Rules

Instrument identity MUST be derived exclusively from `order_plan.v1.json` (mirrored into execution evidence truth).

## Underlying

- If `order_plan.underlying` is a dict, `instrument.underlying` MUST equal `order_plan.underlying.symbol`.
- If `order_plan.underlying` is a string, `instrument.underlying` MUST equal that string.
- Otherwise: FAIL_CLOSED.

## Instrument kind

- `instrument.kind` MUST be:
  - `OPTION_SINGLE` if `len(order_plan.legs) == 1`
  - `OPTION_MULTI` if `len(order_plan.legs) > 1`

## Legs

For each `order_plan.legs[i]`, `instrument.legs[i]` MUST include:

- `action` in `{BUY, SELL}`
- `expiry_utc` as a non-empty string
- `strike` as a non-empty string (no floats)
- `right` normalized to `C` or `P`
  - accepted inputs: `C`, `P`, `CALL`, `PUT` (case-insensitive)
- `ratio` integer >= 1
- `ib_conId` integer
- `ib_localSymbol` non-empty string

## Summary

- For `OPTION_SINGLE`, `instrument.summary` MUST equal the sole leg’s `expiry_utc`, `strike`, `right`.
- For `OPTION_MULTI`, `instrument.summary.expiry_utc/strike/right` MUST be null.

# Fail-Closed Semantics

- If execution evidence day directory is missing: write failure artifact and exit non-zero.
- If any submission has an execution event but missing `order_plan.v1.json`: fail closed.
- If any required identity fields are missing/invalid: fail closed.
- No `"UNKNOWN"` placeholder is permitted in v3 output.

# Immutability and Producer Lock

- Outputs are immutable (no overwrites). Any attempted rewrite MUST hard-fail.
- For an existing v3 snapshot for a day, producer `git_sha` is locked and mismatch MUST hard-fail.

# Validation

- Snapshot MUST validate against:
  - `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json`
- Latest pointer MUST validate against:
  - `governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_latest_pointer.v3.schema.json`

# Known Legacy Artifact Note

The first emitted v3 snapshot for `2026-02-14` may contain an older underlying encoding if it was produced prior to the underlying-symbol normalization rule. Subsequent v3 outputs MUST follow the Underlying rule above.
