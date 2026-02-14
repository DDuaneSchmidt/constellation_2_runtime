---
id: C2_E_POSITIONS_SNAPSHOT_TRUTH_V1
title: "Prerequisite E — Positions Snapshot Truth Spine (Deterministic, Audit Grade)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Purpose

Bundle F requires a provable set of **open positions** and their deterministic cost basis / exposure classification.

C2 currently has:
- execution event schema (execution_event_record)
- position lifecycle schema (position_lifecycle)
- Phase D produces broker submission + execution event evidence (but not in runtime truth yet)

This spine defines how Constellation 2.0 produces a daily, immutable **Positions Snapshot Truth** artifact suitable for:
- accounting NAV components (Bundle F)
- defined-risk exposure calculation (Bundle F)
- budget enforcement inputs (Bundle G)
- hostile audit reconstruction

# 2. Scope and Non-Claims

## 2.1 In scope

- Daily immutable positions snapshot under canonical truth root
- Deterministic position identity and ordering
- Deterministic quantities and cost basis fields (no floats)
- Defined-risk classification where provable
- Explicit degraded vs fail semantics
- Reconstruction guarantee and acceptance tests

## 2.2 Out of scope

- Broker reconciliation correctness (unless reconciliation spine exists)
- Corporate actions
- Tax lots (v1 uses deterministic aggregate lots unless governed otherwise)
- Intraday positions (v1 is day snapshot)

# 3. Inputs (Hash-Bound)

This spine consumes only hash-bound truth:

Primary inputs (preferred):
1) Execution Evidence Truth Spine outputs (canonicalized):
   - broker_submission_record.v2.json
   - execution_event_record.v1.json
   - veto_record.v1.json (ignored for positions, but tracked)

2) Position lifecycle truth, if present (optional but improves accuracy):
   - `constellation_2/schemas/position_lifecycle.v1.schema.json` defines format

If lifecycle truth is absent, the spine may operate in degraded mode by inferring position state from execution events only, but must label the degradation.

# 4. Outputs (Canonical Truth)

Truth root:
`constellation_2/runtime/truth/positions_v1/`

Daily snapshots:
- `constellation_2/runtime/truth/positions_v1/snapshots/<DAY_UTC>/positions_snapshot.v1.json`

Latest pointer (pointer-only):
- `constellation_2/runtime/truth/positions_v1/latest.json`

Failure artifact:
- `constellation_2/runtime/truth/positions_v1/failures/<DAY_UTC>/failure.json`

Immutability rules:
- no overwrite; identical bytes may be skipped; mismatched bytes hard-fail.

# 5. Deterministic Position Model (v1)

## 5.1 Position identity

A `position_id` must be deterministic and reproducible.

Preferred:
- position_id derived from governed binding hash / intent hash chain where available.

Fallback (degraded):
- derive position_id from a canonical seed object:
  - underlying symbol
  - expiry
  - strike
  - right (C/P)
  - strategy family (if known)
  - opening execution event id
  - engine_id (if available)

Hashing must use canonical json hashing rules and must not include floats.

## 5.2 Required fields per position entry

Each position entry must include (minimum):

- `position_id` (string)
- `engine_id` (string or "unknown")
- `instrument` (object describing underlying and option contract if applicable)
- `qty` (integer contracts/shares)
- `avg_cost_cents` (integer) OR equivalent deterministic cost basis representation
- `market_exposure_type` (e.g. DEFINED_RISK / UNDEFINED_RISK)
- `max_loss_cents` (integer or null if unknown)
- `opened_day_utc`
- `status` (OPEN/CLOSED)

All monetary values must be integer cents (no floats).

# 6. Failure Semantics

Status codes:

- `OK`
- `DEGRADED_MISSING_LIFECYCLE` (execution-only position state)
- `FAIL_CORRUPT_INPUTS`
- `FAIL_SCHEMA_VIOLATION`
- `FAIL_UNDEFINED_POSITION_IDENTITY` (cannot derive stable id)

Rules:
- Missing required inputs for the chosen mode → FAIL_CORRUPT_INPUTS
- Schema invalid → FAIL_SCHEMA_VIOLATION
- If position_id cannot be derived deterministically → FAIL_UNDEFINED_POSITION_IDENTITY
- In FAIL:
  - write failure artifact
  - do not update latest pointer
  - exit non-zero

# 7. Reconstruction Guarantee

Given:
- immutable positions snapshot
- embedded input manifest (paths + sha256)
- producing git sha

An auditor can replay and reproduce identical output bytes.

# 8. Acceptance Tests

1) Determinism: same inputs → identical bytes
2) Immutability: attempted rewrite fails closed
3) Degraded mode triggers when lifecycle truth missing
4) Position_id stable across reruns
5) Defined-risk classification consistent for vertical spreads where width/credit/debit is provable from inputs

# 9. Dependency Contract

Bundle F must consume positions snapshot truth (required). Without it, Bundle F cannot claim NAV.

Bundle G must consume Bundle F exposure outputs, which depend on this positions truth.
