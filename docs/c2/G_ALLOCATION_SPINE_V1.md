---
doc_id: C2_G_ALLOCATION_SPINE_V1
title: "Bundle G: Allocation Spine v1 (Audit-Grade, Deterministic)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Bundle G: Allocation Spine v1

## 1. Scope

Bundle G produces **deterministic, immutable allocation decisions** for each candidate intent, under explicit caps and multipliers, suitable for hostile review.

This document defines:
- Inputs, outputs, and directory layout under `constellation_2/runtime/truth/allocation_v1/`
- Deterministic allocation math: caps + multipliers + ordering
- Fail-closed vs degraded semantics
- Acceptance tests and invariants

## 2. Non-Claims

Bundle G does **not** claim:
- Profitability or edge
- Optimality of sizing
- Correctness of broker execution
- Correctness of upstream market data, positions, or accounting beyond schema validity and declared statuses

Bundle G claims only:
- Deterministic decisions given declared inputs
- Immutable output artifacts (no overwrite)
- Fail-closed behavior when required upstream truth is missing or invalid
- Explicit and complete reason codes for decisions

## 3. Authorities and Required Inputs

### 3.1 Truth Authority
- Governance: `governance/` (contracts, registries, schemas)
- Runtime truth: `constellation_2/runtime/truth/`
- Git: producer identity and deterministic code provenance

### 3.2 Required inputs (fail-closed if missing)
- **Accounting latest** from Bundle F:
  - Must exist and be schema-valid.
  - If `accounting.status != OK` → **BLOCK all new entries** (hard rule).
- Engine execution mode registry (governed):
  - If engine is not LIVE → **BLOCK**.
- Candidate intent (Phase A) and/or order plan (Phase C), hash-bound:
  - Allocation binds to a deterministic intent identifier and its hash.
- Positions / exposure (Bundle F output):
  - Used for cap enforcement (existing exposure).
- Governance risk budget contract inputs:
  - Per-engine and portfolio caps.

### 3.3 Optional inputs (degraded if missing)
- Volatility regime signals (Phase B):
  - If missing, **reduce multiplier** (do not block) and record degraded reason.

## 4. Output Artifacts and Layout

### 4.1 Immutable truth outputs

All outputs are immutable: reruns must either produce byte-identical outputs (skip) or fail closed.

- `constellation_2/runtime/truth/allocation_v1/decisions/<DAY_UTC>/<INTENT_ID>.json`
- `constellation_2/runtime/truth/allocation_v1/summary/<DAY_UTC>/summary.json`
- `constellation_2/runtime/truth/allocation_v1/latest.json` (pointer-only)

### 4.2 Decision object (high-level)
Each decision includes:
- `intent_id`
- `engine_id`
- `day_utc`
- `contracts_allowed` (integer >= 0)
- `effective_risk_budget` (number; deterministic integer if possible)
- `binding_constraints` (ordered list)
- `reason_codes` (ordered list; complete)
- `status` = `ALLOW` or `BLOCK` (block includes reasons)
- `inputs` (hash-bound pointers to accounting latest, exposure snapshot, risk budget registry version, and the intent hash)

### 4.3 Summary object (high-level)
Summary is deterministic:
- lists all decisions produced for the day
- includes counts by status, and top binding constraints
- includes stable hashes for decision files

## 5. Deterministic Allocation Math

### 5.1 Definitions

Let:
- `NAV` = accounting NAV total (from Bundle F)
- `DD` = drawdown metric from Bundle F output
- `VOL` = volatility regime scalar from Phase B (optional)
- `BaseRiskPerContract` = defined by the intent type and contract spec (governed or derived)
- `BudgetPortfolio` = portfolio defined-risk cap
- `BudgetEngine[e]` = per-engine cap
- `BudgetUnderlying[u]` = per-underlying cap
- `BudgetExpiryBucket[b]` = per-expiry-bucket cap
- `BudgetPerTrade` = per-trade cap

### 5.2 Multipliers
Multipliers are deterministic and applied as:

`mult_final = mult_drawdown * mult_volatility`

- `mult_drawdown` is derived from accounting drawdown using governed piecewise function
- `mult_volatility` is derived from volatility regime; if missing, use conservative floor value and record degraded reason

### 5.3 Caps (mandatory)
The allowed contracts are the minimum of all binding caps:

- Cap 1: portfolio defined-risk cap remaining
- Cap 2: per-trade cap
- Cap 3: per-engine remaining cap
- Cap 4: per-underlying remaining cap
- Cap 5: per-expiry bucket remaining cap
- Cap 6: max positions remaining
- Cap 7: max expiry buckets remaining

### 5.4 Deterministic ordering of binding constraints
Constraints are evaluated in this exact order (first binding wins, but all bindings are recorded):

1. Engine status gate (LIVE required)
2. Accounting status gate (OK required)
3. Portfolio max positions cap
4. Portfolio defined-risk cap
5. Per-trade cap
6. Per-engine cap
7. Per-underlying cap
8. Per-expiry bucket cap
9. Max expiry buckets cap
10. Volatility throttle multiplier (degraded allowed)

This ordering is a contract: decisions must not reorder constraints.

### 5.5 Sizing formula
Let:
- `risk_budget_effective = BudgetPerTrade * mult_final`
- `contracts_raw = floor(risk_budget_effective / BaseRiskPerContract)`
- `contracts_allowed = min(contracts_raw, caps...)`, integer ≥ 0

If any required gate fails:
- `contracts_allowed = 0`
- `status = BLOCK`

## 6. Fail-Closed and Degraded Semantics

### 6.1 Fail-Closed conditions
Fail and exit non-zero; do not write latest pointer:
- Accounting latest missing or schema invalid
- Any schema validation fails for outputs
- Attempted overwrite of immutable artifacts
- Unknown fields or corrupt inputs

### 6.2 Block conditions (valid output, status=BLOCK)
- Accounting status != OK (hard block)
- Engine status != LIVE (hard block)
- Risk budget registry missing or inconsistent (block, but still write decision with reasons)

### 6.3 Degraded conditions (write decision with reduced sizing)
- Volatility regime missing: apply conservative multiplier floor and record `DEGRADED_MISSING_VOLATILITY_INPUT`

## 7. Acceptance Tests

### 7.1 Determinism
- Same inputs → byte-identical output JSON files
- Rerun should skip identical outputs or fail closed on overwrite attempts

### 7.2 Constraint ordering stable
- Construct tests where multiple caps bind; ensure binding list is stable and ordered.

### 7.3 Accounting gate blocks
- If accounting status != OK, all new entries are BLOCK with correct reason codes.

### 7.4 Vol throttle reduces sizing
- Missing or high-vol regime reduces `contracts_allowed` deterministically.

## 8. Operational Notes

- Bundle G is not permitted to infer missing data.
- Bundle G must emit complete, stable reason codes for every decision.
- Latest pointer is written only if all daily decisions and summary validate and are immutable.

