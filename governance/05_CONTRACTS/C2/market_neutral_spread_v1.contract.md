---
id: C2_MARKET_NEUTRAL_SPREAD_CONTRACT_V1
title: "Market-Neutral Spread Contract v1 (Deterministic Spread Z-Score Signal; INACTIVE by Default)"
status: DRAFT
version: 1
created_utc: 2026-02-26
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - engine
  - market-neutral
  - spread
  - z-score
  - deterministic
  - fail-closed
  - registry-driven
  - inactive-by-default
---

# Market-Neutral Spread Contract v1

## 0. Purpose

Engine `C2_MARKET_NEUTRAL_SPREAD_V1` is a deterministic spread signal engine.

- Computes z-score of ETF pair spread (log price ratio).
- Emits **ExposureIntent v1** as a signal output surface (broker-agnostic, no sizing).
- Designed to be **INACTIVE by default** and not executed by schedulers until explicitly activated via registry.

## 1. Deterministic rule set (v1)

### 1.1 Pairs (required)

- SPY vs QQQ
- IWM vs SPY
- HYG vs LQD

No additional symbols beyond these are permitted in v1.

### 1.2 Spread definition

For pair `(A, B)` using aligned sessions (intersection of timestamps with bars present):

- `r_t = ln(close_A(t) / close_B(t))`

Lookback window:

- `LOOKBACK = 60` sessions ending at `DAY` inclusive.

### 1.3 Z-score

Compute over the lookback window:

- `mean = avg(r)`
- `std = sample_std(r)` (n-1); fail-closed if `std == 0`

- `z = (r_today - mean) / std`

### 1.4 Entry rule

- Enter when `|z| > Z_ENTER` where `Z_ENTER = 2.0`

Exit rule target:

- Exit when `|z| < Z_EXIT` where `Z_EXIT = 0.5`

### 1.5 Output selection (single intent per day)

If multiple pairs satisfy entry:

- Select the pair with maximum `|z|`
- Tie-breaker: lexicographic `(A, B)`.

If none satisfy entry: **NO_INTENT** and write nothing.

### 1.6 Signal exposure output (v1 limitation)

Target parameters:

- `target_notional_pct = 0.05`
- `constraints.max_risk_pct = 0.01`

**Important schema limitation (governed):**
- `ExposureIntent v1` can represent only one underlying symbol and its `exposure_type` enum does not represent a full two-leg dollar-neutral spread.
- Therefore v1 emits a deterministic **single long-leg** ExposureIntent as a signal surface.

Long-leg selection rule:

- If `z > 0` (ratio A/B high), emit long `B`.
- If `z < 0` (ratio A/B low), emit long `A`.

True dollar-neutral execution requires a governed multi-leg intent schema upgrade (out of scope for this v1 contract).

## 2. Inputs (fail-closed)

### 2.1 Required truth inputs

- Market data snapshot v1:
  - Root: `constellation_2/runtime/truth/market_data_snapshot_v1/`
  - Manifest: `constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json`
  - JSONL bars referenced by manifest entries `{file, sha256, symbol, year}`

### 2.2 Fail-closed conditions

The engine MUST fail (non-zero) and MUST write **no truth artifacts** if any occurs:

- market data manifest missing/unreadable
- required symbol missing from manifest (notably LQD is required)
- any referenced JSONL file missing
- sha256 mismatch vs manifest
- malformed JSONL rows
- insufficient aligned history (`< LOOKBACK`)
- `std == 0`
- any numeric parse error

## 3. Outputs (immutable truth surface behavior)

### 3.1 Output directory

If an intent is produced, it MUST be written under:

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`

### 3.2 Output file format

- Exactly one file for the day (if any), named:
  - `<sha256>.exposure_intent.v1.json`
- Canonical bytes:
  - `canonical_json_bytes_v1(intent_obj) + "\n"`
- `sha256` is computed over the canonical bytes.

### 3.3 Schema conformance

The output MUST conform to:

- `constellation_2/schemas/exposure_intent.v1.schema.json`

No extra fields are allowed beyond the schema.

## 4. Registry-driven execution + inactivity requirements

### 4.1 Registry presence

Engine MUST be present in:

- `governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json`

With:

- `engine_id = C2_MARKET_NEUTRAL_SPREAD_V1`
- `activation_status = INACTIVE` by default
- `runner_path` (python module path)
- `engine_runner_path` (file path)
- `engine_runner_sha256` (code lock)
- budget cap fields as governed

### 4.2 Orchestrator selection policy (governed behavior)

The orchestrator MUST execute **only** engines with:

- `activation_status == ACTIVE`

Therefore, when this engine is `INACTIVE`, it MUST NOT be selected, MUST NOT run in schedulers, and MUST NOT produce truth artifacts during normal runs.

## 5. Activation procedure (governed)

To activate (no scheduling yet):

1) Flip in registry:
   - `activation_status: INACTIVE -> ACTIVE` for `C2_MARKET_NEUTRAL_SPREAD_V1`
2) Run registry gate for a **new day key** and verify PASS.
3) Run orchestrator in PAPER for a historical day (single day) and verify:
   - engine intent file is produced under intents snapshot OR prints NO_INTENT
   - no submission artifacts are created by this engine module (it is broker-agnostic)
4) Only after the above passes may scheduling be considered.

## 6. Known dataset constraint (current repo)

If the current `market_data_snapshot_v1` does not include `LQD`, the engine will fail-closed if invoked. Activation MUST NOT proceed until the dataset is expanded to cover required symbols.
