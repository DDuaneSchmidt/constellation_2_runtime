---
doc_id: C2_G_RISK_BUDGET_CONTRACT_V1
title: "Bundle G: Risk Budget Contract v1 (Caps, Registries, and Binding Semantics)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Bundle G: Risk Budget Contract v1

## 1. Purpose

This contract defines the governed inputs used by Bundle G to cap allocation sizes.

The risk budget contract is:
- deterministic
- explicit
- hash-bound to governance
- enforceable by schema validation and fail-closed rules

## 2. Required Budget Dimensions

Bundle G must enforce caps in these dimensions:

### 2.1 Portfolio-level caps
- Total defined-risk exposure cap (absolute dollars)
- Max open positions
- Max expiry buckets
- Optional: max underlying concentration (as percent of NAV or dollars)

### 2.2 Per-trade caps
- Max defined-risk per trade (absolute dollars)
- Optional: max premium per trade

### 2.3 Per-engine caps
- Max defined-risk per engine (absolute dollars)
- Engine status gates: PAPER/SHADOW/LIVE gating is separate but referenced

### 2.4 Per-underlying caps
- Max defined-risk per underlying (absolute dollars)
- Optional: max positions per underlying

### 2.5 Per-expiry bucket caps
- Max defined-risk per expiry bucket (absolute dollars)
- Expiry bucket definition must be deterministic (e.g., 0-7d, 8-30d, 31-60d, 61-120d, 121+d)

## 3. Binding Inputs and Hashing

All risk budget inputs must be hash-bound pointers to governance or runtime truth artifacts.

Bundle G outputs must include:
- `risk_budget_registry_hash` (sha256 of canonical governed config)
- pointers to the exact config file(s) used (paths)
- a manifest of input hashes used for the decision

## 4. Deterministic Constraint Binding Semantics

### 4.1 Binding order
Binding order is contractual and must match `G_ALLOCATION_SPINE_V1`.

### 4.2 Binding set reporting
Bundle G must report:
- primary binding constraint
- secondary bindings (if multiple constraints are equal)
- final computed allowed contracts

The decision must include a stable list of:
- `binding_constraints[]` in deterministic order
- `reason_codes[]` in deterministic order

## 5. Fail-Closed Rules

Fail closed (exit non-zero and do not write latest pointer) when:
- risk budget config missing
- risk budget config schema invalid
- any computed cap yields NaN/Infinity or non-deterministic numeric results
- any attempt to overwrite immutable decision files occurs

## 6. Degraded Mode Policy

Risk budget contract itself is not optional. Missing it is a BLOCK (hard).

Degraded behavior applies only to optional modifiers (e.g., volatility scalar).

## 7. Acceptance Tests

- Load governed risk budget and validate schema
- Deterministic hash of risk budget config stable across runs
- Cap binding ordering stable under multiple constraints
- Block behavior on missing/invalid risk budget config

