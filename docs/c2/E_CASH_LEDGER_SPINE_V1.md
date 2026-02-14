---
id: C2_E_CASH_LEDGER_SPINE_V1
title: "Prerequisite E — Cash Ledger Spine (Audit Grade, Deterministic)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
truth_root: constellation_2/runtime/truth
---

# 1. Purpose

The Cash Ledger Spine is the **authoritative cash truth source** for Constellation 2.0.

Bundle F (Accounting) and Bundle G (Allocation/Throttle) require a provable cash ledger to:
- compute NAV deterministically,
- enforce risk budgets based on portfolio capital,
- support hostile audit reconstruction.

This spine exists because cash/NLV truth is currently absent in C2 (proven).

# 2. Scope and Non-Claims

## 2.1 In scope

- Immutable daily cash snapshots under the canonical C2 truth root
- Deterministic canonical JSON encoding (no floats)
- Hash-bound provenance to input records (broker account values or operator-provided statements)
- Explicit failure semantics (fail-closed vs degraded)
- Reconstruction guarantee (input hashes → same output bytes)
- Acceptance tests

## 2.2 Out of scope (non-claims)

- Tax accounting
- FX conversion unless explicitly provided by governed input (future)
- Intraday streaming cash (v1 is daily snapshot)
- Broker reconciliation beyond “this is what inputs reported” (unless reconciliation spine exists)

# 3. Authority and Governance

Truth authority:
- Governance defines schema + policy.
- Git defines producing code.
- Runtime truth under `constellation_2/runtime/truth/` is immutable output.

Hard invariants:
- Immutable writes (no overwrite; identical bytes allowed to skip).
- No floats anywhere in outputs.
- Every input must be hash-bound or the run fails closed.

# 4. Inputs (Hash-Bound)

The Cash Ledger Spine can operate in one of two governed modes:

## 4.1 Broker-derived mode (preferred)

Input is a broker account values record (paper or live) that includes:
- net liquidation value (NLV)
- cash balance(s)
- available funds / excess liquidity (optional but recommended)
- currency

This requires a governed upstream producer (Phase D or separate broker-account probe). If absent, v1 may use operator-provided inputs (see below).

## 4.2 Operator-statement mode (fallback)

Operator supplies a day-scoped JSON file (governed schema) containing:
- cash balance
- NLV
- account id (if multi-account)
- observed_at_utc timestamp

This mode is allowed for bootstrap and testing.

All inputs must be listed in `input_manifest` with sha256.

# 5. Outputs (Immutable Truth)

Truth root:
`constellation_2/runtime/truth/cash_ledger_v1/`

Daily snapshots:
- `constellation_2/runtime/truth/cash_ledger_v1/snapshots/<DAY_UTC>/cash_ledger_snapshot.v1.json`

Latest pointer (pointer-only):
- `constellation_2/runtime/truth/cash_ledger_v1/latest.json`

Failure artifact:
- `constellation_2/runtime/truth/cash_ledger_v1/failures/<DAY_UTC>/failure.json`

# 6. Deterministic Data Model (high level)

The snapshot must include, at minimum:

- `day_utc`
- `observed_at_utc`
- `currency`
- `cash_total_cents` (integer)
- `nlv_total_cents` (integer)
- optional:
  - `available_funds_cents`
  - `excess_liquidity_cents`
  - `account_id`
  - `notes`

All monetary values are integers in **cents** to avoid floats.

# 7. Failure Semantics

Status codes:

- `OK`
- `DEGRADED_OPERATOR_INPUT` (operator-statement mode used)
- `FAIL_CORRUPT_INPUTS`
- `FAIL_SCHEMA_VIOLATION`

Rules:
- Missing input → FAIL_CORRUPT_INPUTS
- Schema invalid → FAIL_SCHEMA_VIOLATION
- Operator mode is allowed but must be labeled DEGRADED_OPERATOR_INPUT

In FAIL states:
- write failure artifact
- do not update latest pointer
- exit non-zero

# 8. Reconstruction Guarantee

Given:
- the immutable snapshot artifact for day D
- embedded input_manifest (paths + sha256)
- producing git sha
- canonical JSON rules

An auditor can:
- verify input hashes
- rerun producer
- reproduce identical bytes

# 9. Acceptance Tests (must be runnable)

1) Determinism: same input → identical bytes
2) Immutability: attempted rewrite fails closed
3) Operator fallback produces DEGRADED status
4) Required fields present and types are correct (integers, no floats)
5) Latest pointer is pointer-only and references immutable artifacts by sha256

# 10. Dependency Contract to Bundle F/G

Bundle F:
- requires cash snapshot for the accounting day; absence → FAIL_CORRUPT_INPUTS.

Bundle G:
- requires Bundle F accounting latest NAV; but Bundle F itself requires cash ledger. Therefore cash ledger is a hard upstream dependency for F→G chain.
