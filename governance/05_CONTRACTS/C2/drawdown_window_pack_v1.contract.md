---
id: C2_DRAWDOWN_WINDOW_PACK_V1
title: "Constellation 2.0 — Drawdown Window Pack v1 (30/60/90 From NAV Snapshot Truth)"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - drawdown
  - windows
  - monitoring
  - deterministic
  - fail_closed
---

# Drawdown Window Pack v1 — Contract

## 1. Objective
Compute day-scoped rolling drawdown metrics for trailing windows:

- 30
- 60
- 90

Computed ONLY from NAV Snapshot Truth.

## 2. Canonical output path
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/drawdown_window_pack/<DAY>/drawdown_window_pack.v1.json`

## 3. Input sources (required)
- NAV History Ledger for `<DAY>`:
  - `.../nav_history_ledger/<DAY>/nav_history_ledger.v1.json`

The window pack generator MUST use the ledger as the only history source.

## 4. Window definition (precise)
A window of size `N` is defined as the **last N available NAV snapshot observations** ending at `<DAY>`, using the `days[]` array in the ledger.

This is an observation-count window (trading-day aware) and does not assume calendar-day continuity.

## 5. Fail-closed rule for insufficient history (precise)
For each window size `N` in {30,60,90}:

- If the ledger contains fewer than `N` observations up to and including `<DAY>`, the writer MUST:
  - set bundle status to `FAIL_INSUFFICIENT_HISTORY`
  - include a reason code per missing window
  - and exit non-zero.

No partial windows are permitted in v1.

## 6. Computed fields (required)
For each window `N`, compute:

- `window_days` = N
- `window_start_day_utc` (the first observation day in the window)
- `window_end_day_utc` (= `<DAY>`)
- `max_drawdown_pct` (most negative drawdown_pct observed in the window)

Rules:
- Drawdown values are taken from NAV Snapshot Truth `drawdown_pct` (already quantized to 6dp).
- max_drawdown_pct must be represented as a decimal string with 6dp.

## 7. Required metadata (audit)
Top-level required:
- `schema_id` = `"C2_DRAWDOWN_WINDOW_PACK_V1"`
- `schema_version` = `1`
- `day_utc`
- `status` = `OK` or `FAIL_INSUFFICIENT_HISTORY`
- `windows` array (for 30/60/90)
- `input_manifest` including:
  - ledger path + sha256
  - output schema path + sha256
- `produced_utc`, `producer`, `canonical_json_hash`

## 8. Schema requirement
The produced artifact MUST validate against:
- `governance/04_DATA/SCHEMAS/C2/MONITORING/drawdown_window_pack.v1.schema.json`
