---
doc_id: C2_G_REASON_CODES_V1
title: "Bundle G: Reason Codes v1 (Allocation + Throttle)"
status: DRAFT
version: 1
created_utc: 2026-02-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Bundle G: Reason Codes v1

## 1. Purpose

Reason codes are the canonical explanation surface for why Bundle G:
- allowed a trade
- reduced sizing
- or blocked entry

Reason codes must be:
- deterministic
- stable strings (never reused with different meaning)
- emitted as an ordered list in outputs

## 2. Naming Convention

- Prefix: `G_`
- Uppercase snake case
- No free-form punctuation

Example: `G_BLOCK_ACCOUNTING_NOT_OK`

## 3. Core BLOCK reason codes

### Accounting / upstream truth
- `G_BLOCK_ACCOUNTING_NOT_OK`  
  Accounting latest exists but status != OK.

- `G_BLOCK_ACCOUNTING_MISSING`  
  Accounting latest missing (fail-closed in implementation; if recorded, it must be as failure artifact).

### Engine mode
- `G_BLOCK_ENGINE_NOT_LIVE`  
  Engine execution mode registry indicates engine is not LIVE.

### Risk budget contract
- `G_BLOCK_MISSING_RISK_BUDGET_CONTRACT`  
  Governed risk budget config missing or unreadable.

- `G_BLOCK_INVALID_RISK_BUDGET_CONTRACT`  
  Risk budget config fails schema validation.

### Volatility extreme
- `G_VOL_BLOCK_EXTREME`  
  Volatility regime indicates extreme conditions; throttle blocks.

### Drawdown hard stop
- `G_DD_BLOCK`  
  Drawdown threshold exceeded; throttle blocks.

## 4. Degraded / reduced sizing codes

### Missing volatility input
- `G_DEGRADED_MISSING_VOLATILITY_INPUT`  
  Volatility regime input missing; conservative multiplier applied.

### Drawdown reductions
- `G_DD_OK`
- `G_DD_REDUCE_50`
- `G_DD_REDUCE_25`

### Volatility reductions
- `G_VOL_LOW`
- `G_VOL_MID`
- `G_VOL_HIGH`

## 5. Cap binding reason codes

These indicate the binding cap that set final sizing to the minimum.

- `G_CAP_MAX_POSITIONS`
- `G_CAP_PORTFOLIO_DEFINED_RISK`
- `G_CAP_PER_TRADE`
- `G_CAP_PER_ENGINE`
- `G_CAP_PER_UNDERLYING`
- `G_CAP_PER_EXPIRY_BUCKET`
- `G_CAP_MAX_EXPIRY_BUCKETS`

## 6. Output semantics

### 6.1 Decision outputs must include
- `status` (ALLOW/BLOCK)
- `reason_codes[]` ordered, stable
- `binding_constraints[]` ordered, stable

### 6.2 Failure semantics
If the allocation spine fails closed (schema invalid, attempted overwrite, corrupt inputs), it should:
- write a failure artifact (if defined for the spine)
- exit non-zero
- not write `latest.json`

## 7. Acceptance tests for reason codes
- Every BLOCK output includes at least one `G_BLOCK_*` or `G_*_BLOCK_*` code
- Every reduced sizing output includes at least one reduction/degraded reason
- Binding caps always include one `G_CAP_*` code when contracts are reduced by cap

