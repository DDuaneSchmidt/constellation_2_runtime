id: C2_IB_HISTORICAL_MARKET_DATA_SNAPSHOT_DOWNLOADER_V1
title: "IB Historical Market Data Snapshot Downloader V1 (Market Data Spine, Instance-Aware, Fail-Closed)"
status: DRAFT
version: V1
created_utc: 2026-02-25
last_reviewed: 2026-02-25
owner: CONSTELLATION
authority: governance+git+runtime_truth
domain: MARKET_DATA
tags:
  - ib
  - historical-bars
  - market-data
  - truth-spine
  - determinism
  - fail-closed
  - instance-aware
  - audit-proof
---

# IB Historical Market Data Snapshot Downloader V1

## 1. Objective

Provide an **audit-grade**, **fail-closed**, **deterministic** tool that connects to Interactive Brokers (IB Gateway / TWS, paper) and produces immutable daily OHLCV truth under the existing spine:

`market_data_snapshot_v1`

This tool exists to ensure engines can generate intents for a controlled set of symbols by guaranteeing:

- each target symbol appears in `dataset_manifest.json`
- each symbol has sha256-verified yearly JSONL files
- the manifest is updated deterministically and validated before/after write
- instance-scoped truth is honored via `C2_TRUTH_ROOT` (absolute existing directory)

Target symbols (initial scope):

- `SPY`, `QQQ`, `IWM`, `TLT`, `GLD`, `HYG`

Non-goals:
- intraday bars, quotes, options chains
- price adjustment or dividend/split adjustment (no “adjusted_close” derivation)
- any UI changes

---

## 2. Authority & Truth Surface

### 2.1 Truth root selection (instance-aware)

The downloader MUST select the truth root as follows:

- If `C2_TRUTH_ROOT` is set:
  - it MUST be an absolute path
  - it MUST exist and be a directory
  - it becomes the truth root for all outputs
- Else:
  - default truth root is canonical:
    - `constellation_2/runtime/truth`

### 2.2 Output paths (immutable)

The tool MUST write:

`{TRUTH_ROOT}/market_data_snapshot_v1/<SYMBOL>/<YYYY>.jsonl`

and MUST update:

`{TRUTH_ROOT}/market_data_snapshot_v1/dataset_manifest.json`

The tool MUST NOT overwrite any existing truth JSONL file.

---

## 3. Record Schema & Determinism

### 3.1 Record shape

Each JSONL line MUST satisfy the governed schema:

`governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_data_snapshot.v1.schema.json`

Required fields per record:
- `dataset_version` (string)
- `symbol` (string)
- `timestamp_utc` (UTC midnight `YYYY-MM-DDT00:00:00Z`)
- `open`, `high`, `low`, `close` (number)
- `volume` (integer >= 0)
- `source_name` (string)
- `source_hash` (sha256 lowercase hex)
- `ingested_utc` (UTC Z string)

Optional:
- `adjusted_close` (not produced by this tool)

### 3.2 Determinism anchor: `--run_utc`

The tool MUST require an explicit `--run_utc` (UTC Z) which is written as `ingested_utc` in each record and is used as a deterministic anchor for any run-level truth metadata.

The tool MUST NOT embed wall-clock timestamps into truth bytes.

### 3.3 Canonical JSON and file hashing

- JSON lines MUST be written with stable canonical formatting:
  - `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False)`
- File sha256 MUST be computed over raw bytes of the JSONL file as written.
- The manifest MUST be written in canonical minified JSON with a trailing newline:
  - `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"`

---

## 4. Manifest Update Rules (append-only, fail-closed)

### 4.1 Manifest shape (existing spine)

`dataset_manifest.json` has:
- `created_utc` (string UTC Z)
- `dataset_version` (string)
- `date_range` (object with `start`, `end` as `YYYY-MM-DD`)
- `files` (list of `{symbol, year, file, sha256}`)
- `symbols` (sorted list of symbols)
- `global_hash` (sha256)

### 4.2 Global hash algorithm (MUST match existing spine)

Given `files` entries, compute:

- stable list: sort by `(symbol, year)`
- payload bytes:
  - for each entry in sorted order:
    - append: `"{symbol}|{year}|{sha256}\n"` (NOTE: real newline)
  - UTF-8 encode
- `global_hash = sha256(payload_bytes)`

### 4.3 Fail-closed validation before write

Before any manifest write, the tool MUST:
- verify every referenced file exists
- verify sha256 matches each referenced file
- recompute `global_hash` using the algorithm above and confirm it matches existing manifest

If any check fails: exit nonzero with a clear `FAIL:` reason.

### 4.4 Append-only semantics (no duplicates)

When adding new entries:
- the tool MUST refuse duplicates of `(symbol, year)` in the manifest
- the tool MUST never delete entries
- the tool MUST never rewrite existing JSONL year files

---

## 5. IB Data Acquisition Requirements

### 5.1 API library boundary

Implementation MUST use the same IB client stack already used elsewhere in C2:

- `ib_insync` for historical bar requests

The tool MUST fail closed if `ib_insync` cannot be imported.

### 5.2 Request parameters (daily bars)

The tool MUST request daily bars as:

- `barSizeSetting = "1 day"`
- `whatToShow = "TRADES"`
- `useRTH = 1` (default; configurable via CLI)
- one-year slices by year, producing `<YYYY>.jsonl`

### 5.3 Data normalization

Each bar MUST be normalized to:
- UTC midnight timestamp string: `YYYY-MM-DDT00:00:00Z`
- volume integer >= 0
- stable ordering by `timestamp_utc`

If IB returns no bars for a requested symbol/year: fail closed.

---

## 6. Operator Interface (CLI)

The tool MUST provide:

- `--run_utc` (required)
- `--dataset_version` (default `v1`)
- `--symbol` repeatable OR `--symbols` comma-separated
- `--start_year` and `--end_year` (required)
- IB connection:
  - `--host` (default `127.0.0.1`)
  - `--port` (required)
  - `--client_id` (default `7`)
- pacing:
  - `--sleep_sec` (default `1.0`)
- `--use_rth` (default `1`)

The tool MUST print:
- selected truth root
- per-file write status + sha256
- manifest path + updated `global_hash` + symbol list

---

## 7. Success Criteria (paper-trade readiness support)

A successful run MUST make it possible for downstream engines to avoid:
- `NO_MANIFEST_FILES_FOR_SYMBOL`
- `SYMBOL_NOT_PRESENT_IN_MARKET_DATA_MANIFEST`

by ensuring the manifest contains the target symbols and their yearly files.

This contract does not guarantee that an engine produces a non-empty intent; `NO_INTENT` is allowed if valid for strategy logic, but pipeline execution must not fail due to market data manifest absence.

---
