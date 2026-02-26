id: C2_DEFENSIVE_TAIL_REQUIRED_INPUTS_BRIDGE_V1
title: "Defensive Tail Required Inputs Bridge V1 (Deterministic Materialization, Fail-Closed)"
status: DRAFT
version: V1
created_utc: 2026-02-25
last_reviewed: 2026-02-25
owner: CONSTELLATION
authority: governance+git+runtime_truth
domain: OPS
tags:
  - defensive-tail
  - inputs-bridge
  - truth-surface
  - determinism
  - fail-closed
  - immutable
---

# Defensive Tail Required Inputs Bridge V1

## 1. Objective

Materialize the **exact required input files** for Engine 5 (Defensive Tail) under their required paths, using deterministic transformations from already-governed truth sources.

This is needed because Defensive Tail requires specific paths that may not be produced by the standard accounting/positions pipelines for historical/bootstrap days.

## 2. Required outputs (immutable)

For a given `DAY_UTC` (YYYY-MM-DD) and `SYMBOL` (default SPY), the bridge MUST ensure the following files exist under the selected truth root:

1) Market data snapshot surface:
- `market_data_snapshot_v1/snapshots/<DAY_UTC>/<SYMBOL>.market_data_snapshot.v1.json`

2) Accounting nav snapshot surface (v1):
- `accounting_v1/nav/<DAY_UTC>/nav_snapshot.v1.json`

3) Positions snapshot v2 surface (note: distinct from positions_v1):
- `positions_snapshot_v2/snapshots/<DAY_UTC>/positions_snapshot.v2.json`

All three outputs MUST be written immutably:
- if the output file already exists, the bridge MUST fail closed (no overwrite).

## 3. Truth root selection (instance-aware)

Truth root selection MUST follow:

- If `C2_TRUTH_ROOT` is set:
  - it MUST be an absolute path
  - it MUST exist and be a directory
  - it is used as the truth root
- Else:
  - canonical truth root: `constellation_2/runtime/truth`

## 4. Deterministic source-of-truth mapping (fail-closed)

### 4.1 Positions snapshot v2 bridge

Source:
- `positions_v1/snapshots/<DAY_UTC>/positions_snapshot.v2.json`

Target:
- `positions_snapshot_v2/snapshots/<DAY_UTC>/positions_snapshot.v2.json`

Rule:
- output bytes MUST be an exact byte-for-byte copy of the source file bytes.

### 4.2 Accounting nav snapshot v1 bridge

Target format MUST match the governed v1 snapshot surface:
- `{"day_utc": "...", "history": {"drawdown_pct": "<DECIMAL_STR>"}, "schema_id":"C2_NAV_SNAPSHOT_V1", "schema_version":"v1"}`

Source for `drawdown_pct`:
- Prefer `monitoring_v1/regime_snapshot_v2/<DAY_UTC>/regime_snapshot.v2.json` evidence field if present:
  - `evidence.drawdown_pct` (string)
- Else default:
  - `0.000000`

Fail-closed invariants:
- `DAY_UTC` in output MUST match the requested day.
- `drawdown_pct` MUST parse as a decimal string.

### 4.3 Market data snapshot surface (v1 wrapper)

Target format MUST match the existing snapshot surface contract observed in runtime truth:
- `{"bars":[], "day_utc":"<DAY_UTC>", "schema_id":"C2_MARKET_DATA_SNAPSHOT_V1", "schema_version":"v1", "symbol":"<SYMBOL>"}`

Notes:
- Defensive Tail does not read bar content in v1; it requires existence + sha256 lineage.

## 5. Output evidence

The bridge tool MUST print:
- selected truth root
- each output path written
- sha256 of each output file
- source paths and sha256s for bridged inputs (positions and regime snapshot)

Any missing required source MUST fail closed with a clear `FAIL:` reason.

---
