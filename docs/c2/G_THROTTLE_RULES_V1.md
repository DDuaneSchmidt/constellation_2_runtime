---
doc_id: C2_G_THROTTLE_RULES_V1
title: "Bundle G: Throttle Rules v1 (Deterministic Caps + Multipliers)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Bundle G: Throttle Rules v1

## 1. Purpose

Throttle rules define how allocation sizing is reduced or blocked based on:
- Accounting health (Bundle F)
- Drawdown state (Bundle F)
- Volatility regime (Phase B)
- Engine execution mode (governance registry)
- Portfolio-level structural caps (risk budget contract)

These rules are deterministic and must yield identical outputs for identical inputs.

## 2. Hard Gates (BLOCK)

These gates produce `contracts_allowed = 0` and `status = BLOCK`.

### 2.1 Accounting gate (hard)
If `accounting_latest.status != OK`:
- BLOCK all new entries
- Reason code: `G_BLOCK_ACCOUNTING_NOT_OK`

This is non-negotiable and exists to prevent trading on degraded accounting truth.

### 2.2 Engine mode gate (hard)
If engine mode is not `LIVE`:
- BLOCK new entries for that engine
- Reason code: `G_BLOCK_ENGINE_NOT_LIVE`

### 2.3 Missing governed risk budget contract inputs (hard)
If the risk budget contract registry/config cannot be loaded or is schema invalid:
- BLOCK
- Reason code: `G_BLOCK_MISSING_RISK_BUDGET_CONTRACT`

## 3. Deterministic Multipliers (Sizing Reductions)

Multipliers never increase size above base caps. They only reduce or leave unchanged.

### 3.1 Drawdown multiplier

Inputs:
- `drawdown_pct` from Bundle F nav history (or equivalent governed field)

Piecewise multiplier function (v1):

- If `drawdown_pct` is null → multiplier = 0.0 and BLOCK (accounting not OK should already block)
- If `drawdown_pct <= 0.05` → `mult_drawdown = 1.00`
- If `0.05 < drawdown_pct <= 0.10` → `mult_drawdown = 0.50`
- If `0.10 < drawdown_pct <= 0.15` → `mult_drawdown = 0.25`
- If `drawdown_pct > 0.15` → `mult_drawdown = 0.00` and BLOCK

Reason codes:
- `G_DD_OK`
- `G_DD_REDUCE_50`
- `G_DD_REDUCE_25`
- `G_DD_BLOCK`

### 3.2 Volatility multiplier

Inputs:
- volatility regime scalar or category from Phase B (governed taxonomy)

If volatility input is missing:
- use conservative multiplier: `mult_vol = 0.50`
- record `G_DEGRADED_MISSING_VOLATILITY_INPUT`

If present, v1 multiplier table:

- `LOW` → `mult_vol = 1.00` reason `G_VOL_LOW`
- `MID` → `mult_vol = 0.75` reason `G_VOL_MID`
- `HIGH` → `mult_vol = 0.50` reason `G_VOL_HIGH`
- `EXTREME` → `mult_vol = 0.00` and BLOCK reason `G_VOL_BLOCK_EXTREME`

### 3.3 Final multiplier

`mult_final = mult_drawdown * mult_vol`

The multiplier is applied to the per-trade risk budget prior to computing contracts.

## 4. Caps and Constraint Binding

Even when multiplier allows, caps bind deterministically.

Mandatory caps include:
- Portfolio defined-risk cap
- Per-trade cap
- Per-engine cap
- Per-underlying cap
- Per-expiry bucket cap
- Max positions cap
- Max expiry buckets cap

Constraint ordering is defined in `G_ALLOCATION_SPINE_V1` and must not change.

## 5. Degraded vs Fail-Closed Policy

### 5.1 Degraded allowed
- Missing volatility input → apply conservative multiplier and proceed (if accounting OK and engine LIVE)

### 5.2 Fail-closed
- Any schema violation on required inputs or outputs
- Any attempted overwrite of immutable truth artifacts
- Any unknown fields in governed inputs (when enforced)

Fail-closed means:
- write failure artifact (if defined for the spine) or exit non-zero
- do not write `latest.json`

## 6. Acceptance Tests (Throttle Rules)

- Drawdown threshold tests: verify multiplier transitions and blocking
- Volatility regime tests: verify multiplier table and missing-vol degraded behavior
- Ordering test: ensure constraint binding ordering stable
- Determinism test: rerun yields byte-identical decision artifacts

