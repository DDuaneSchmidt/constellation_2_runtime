---
id: C2_PER_SLEEVE_READINESS_CONTRACT_V1
title: "C2 Per-Sleeve Readiness Contract"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Per-Sleeve Readiness Contract

## Purpose

This contract requires readiness to be computed **per sleeve**, attempt-scoped, and published into the sleeve’s truth partition with canonical pointers.

Readiness is a gate that must prove:

- the sleeve is bound to the correct IB account and mode
- broker connectivity evidence exists for the configured gateway profile
- NAV / accounting prerequisites exist where required
- the sleeve is deterministic and pointer-indexed

## Invariants

### C2_READINESS_IS_SLEEVE_SCOPED_V1

1. **Inputs**
   - Readiness MUST accept `sleeve_id` and MUST resolve all paths via the sleeve truth root resolver.

2. **Attempt-scoped**
   - Readiness runs are attempt-scoped and must publish:
     - attempt artifact
     - canonical pointer index inside the sleeve partition

3. **Account + mode match**
   - Readiness MUST embed and verify:
     - `ib_account` matches the sleeve registry binding
     - `mode` matches the sleeve registry binding

4. **Minimum gate criteria**
   - At minimum, readiness MUST fail closed if:
     - registry binding cannot be verified
     - broker connectivity evidence missing/invalid for the configured gateway
     - accounting/nav evidence required by the system is missing (e.g. `nav_total <= 0` when the sleeve is not explicitly NO_ACTIVITY)

5. **No global truth writes**
   - Readiness MUST NOT write into `constellation_2/runtime/truth/...`.

## Required evidence surfaces (minimum)

- readiness status artifact (sleeve partition)
- readiness pointer head (sleeve partition)
- proof line showing:
  - pointer head → target path → sha256

## Non-claims

- This contract does not define strategy logic.
- This contract defines readiness scoping, gating, and truth placement only.
