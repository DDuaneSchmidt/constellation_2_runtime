---
id: C2_SINGLE_IB_ACCOUNT_MODE_CONTRACT_V1
title: "C2 Single IB Account Mode Contract (Topology Invariant)"
status: ACTIVE
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Single IB Account Mode Contract (Topology Invariant)

## Purpose

This contract establishes a **single-account topology invariant** for Constellation 2.0:

> **All sleeves/engines MUST use the same Interactive Brokers account identifier.**

This reduces operational complexity and prevents cross-account ambiguity in:

- reconciliation
- readiness gating
- accounting truth spines
- deterministic replay certification
- governance/audit interpretation

## Invariant

### C2_SINGLE_ACCOUNT_MODE_V1

1. **One account id**
   - The system MUST operate with exactly one `ib_account` identifier for a given environment (`PAPER` or `LIVE`) at any given time.

2. **No per-sleeve account overrides**
   - No sleeve/engine may specify a different `ib_account` than the system-wide `ib_account` for that run/day.

3. **Authoritative binding**
   - The `ib_account` used by orchestrators, readiness gates, and reconciliation artifacts is the authoritative binding for the day.

4. **Fail-closed**
   - If evidence indicates multiple account ids are being used for the same environment/day, the system MUST fail closed (block submission and mark readiness/gates as FAIL).

## Required evidence surfaces (minimum)

For any day-scoped run that claims readiness or produces allocations/OMS decisions, the following MUST be consistent with the single account id:

- broker reconciliation artifacts
- trade submit readiness artifacts
- submission spine / execution evidence artifacts

(Each artifact MAY contain its own copy of the account id, but they must match exactly.)

## Allowed exceptions

None for production topology.

Testing exceptions are permitted only if:
- they are explicitly governed by a separate ACTIVE contract that names the exception, and
- they are restricted to non-submission test modes (no broker submission).

## Consequences of violation

If violated, the system must be treated as **not institutionally reviewable**, because replay and reconciliation semantics become ambiguous across multiple brokers/accounts.

## Non-claims

- This contract does not claim profitability, fill quality, or broker uptime.
- This contract defines topology only.
