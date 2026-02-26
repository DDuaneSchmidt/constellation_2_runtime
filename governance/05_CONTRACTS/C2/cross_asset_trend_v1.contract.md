---
id: C2_CROSS_ASSET_TREND_CONTRACT_V1
title: "Cross-Asset Trend Contract v1 (Deterministic ExposureIntent v1 Emitter; INACTIVE by Default)"
status: DRAFT
version: 1
created_utc: 2026-02-26
owner: Constellation
authority: governance+git+runtime_truth
tags:
  - engine
  - cross-asset
  - trend
  - exposure-intent
  - deterministic
  - fail-closed
  - registry-driven
  - inactive-by-default
---

# Cross-Asset Trend Contract v1

## 0. Purpose

Engine `C2_CROSS_ASSET_TREND_V1` is a deterministic, broker-agnostic exposure-intent emitter.

- Emits **ExposureIntent v1** only (no sizing, no broker connectivity).
- Evaluates a fixed ETF universe and selects at most **one** target per day (compatibility with downstream lifecycle that assumes one intent per engine/day).
- Designed to be **INACTIVE by default** and not executed by schedulers until explicitly activated via registry.

## 1. Deterministic rule set (v1)

### 1.1 Universe (required)

ETFs only (no futures, no leverage):

- SPY
- QQQ
- IWM
- TLT
- GLD
- HYG
- IEF
- LQD
- UUP
- DBC

### 1.2 Indicators

For each symbol independently:

- `SMA_FAST = 20`
- `SMA_SLOW = 100`

Trading sessions are defined as “days with bars present” (calendar-independent).

### 1.3 Entry qualification (per symbol)

A symbol qualifies on `DAY` iff:

- `SMA_FAST(DAY) > SMA_SLOW(DAY)` AND
- `CLOSE(DAY) > SMA_FAST(DAY)`

### 1.4 Scoring + selection (single intent per day)

If multiple symbols qualify, compute deterministic score:

- `score = (SMA_FAST - SMA_SLOW) / SMA_SLOW  +  (CLOSE - SMA_FAST) / SMA_FAST`

Select the single symbol with:
- maximum `score`
- tie-breaker: lexicographic symbol (ascending)

If zero symbols qualify: **NO_INTENT** and write nothing.

### 1.5 Exposure output (long-only)

- `exposure_type = LONG_EQUITY`
- `target_notional_pct = 0.10`
- `constraints.max_risk_pct = 0.02`

## 2. Inputs (fail-closed)

### 2.1 Required truth inputs

- Market data snapshot v1:
  - Root: `constellation_2/runtime/truth/market_data_snapshot_v1/`
  - Manifest: `constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json`
  - JSONL bars referenced by manifest entries `{file, sha256, symbol, year}`

### 2.2 Fail-closed conditions

The engine MUST fail (non-zero) and MUST write **no truth artifacts** if any occurs:

- market data manifest missing/unreadable
- required symbol missing from manifest
- any referenced JSONL file missing
- sha256 mismatch vs manifest
- malformed JSONL rows
- insufficient history to compute `SMA_SLOW`
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

- `engine_id = C2_CROSS_ASSET_TREND_V1`
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
   - `activation_status: INACTIVE -> ACTIVE` for `C2_CROSS_ASSET_TREND_V1`
2) Run registry gate for a **new day key** and verify PASS.
3) Run orchestrator in PAPER for a historical day (single day) and verify:
   - engine intent file is produced under intents snapshot OR prints NO_INTENT
   - no submission artifacts are created by this engine module (it is broker-agnostic)
4) Only after the above passes may scheduling be considered.

## 6. Known dataset constraint (current repo)

If the current `market_data_snapshot_v1` does not contain all required symbols (IEF/LQD/UUP/DBC), the engine will fail-closed if invoked. Activation MUST NOT proceed until the dataset is expanded to cover the required universe.
