---
id: C2_ECONOMIC_NAV_DRAWDOWN_TRUTH_SPINE_BUNDLE_V1
title: "Constellation 2.0 — Bundled Economic NAV & Drawdown Truth Spine (Bundle) v1"
status: DRAFT
version: 1
created_utc: 2026-02-17
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - economic_nav
  - drawdown
  - monitoring
  - truth_spine
  - audit_grade
  - fail_closed
  - deterministic
  - single_writer
---

# Bundled Economic NAV & Drawdown Truth Spine — Bundle Contract v1

## 1. Objective (non-narrative)
Define a governed, audit-proof truth spine that produces:

1) NAV Snapshot Truth (core)
2) NAV History Ledger (index)
3) Drawdown Window Pack (v1, 30/60/90)
4) Economic Truth Availability Certificate (day-scoped)
5) Validator (one command) that validates the full bundle for a day

This bundle exists to provide a single authoritative source for **portfolio economic NAV and drawdown** for risk gates and hostile audit.

## 2. Canonical truth root (authority)
Canonical truth root is:

- `constellation_2/runtime/truth/`

All bundle artifacts MUST be written under the canonical truth root and MUST be day-scoped.

## 3. Canonical output directories (non-negotiable)
All paths below are relative to repo root and must resolve under canonical truth root:

### 3.1 NAV Snapshot Truth (core)
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_snapshot/<DAY>/nav_snapshot.v1.json`

### 3.2 NAV History Ledger (index)
- Day ledger:
  - `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_history_ledger/<DAY>/nav_history_ledger.v1.json`
- Latest pointer:
  - `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/nav_history_ledger/latest.json`

### 3.3 Drawdown Window Pack (v1)
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/drawdown_window_pack/<DAY>/drawdown_window_pack.v1.json`

### 3.4 Economic Truth Availability Certificate (day-scoped “gate file”)
- `constellation_2/runtime/truth/monitoring_v1/economic_nav_drawdown_v1/certificates/<DAY>/economic_truth_availability_certificate.v1.json`

## 4. Canonical upstream inputs (governed internal truth only)
This bundle MUST derive only from governed internal truth.

### 4.1 Required upstream input for NAV (day-scoped)
- Accounting NAV (Bundle F output):
  - `constellation_2/runtime/truth/accounting_v1/nav/<DAY>/nav.json`

This file is treated as evidence input only. The bundle MUST NOT scrape market data or external feeds.

## 5. Determinism requirements
All bundle writers MUST:
- Use canonical JSON serialization:
  - sorted keys
  - separators `(",", ":")`
  - UTF-8
  - no NaN/Infinity
- Forbid floats anywhere in produced artifacts.
- Include `producer.git_sha` and `produced_utc`.
- Include `input_manifest` with `sha256` for every input file used.
- Include a deterministic self-hash:
  - field name: `canonical_json_hash`
  - computed over canonical JSON with `canonical_json_hash` set to null

## 6. Fail-closed requirements (non-negotiable)
Any of the following MUST be a hard failure (exit non-zero):
- missing required input file
- missing required field
- schema mismatch
- overwrite attempt with different bytes
- non-deterministic serialization
- float encountered anywhere in produced artifacts
- insufficient history for required windows (see component contract)

## 7. Decimal-string risk boundary rule
Any value used for risk gating MUST be represented as **decimal strings** (no floats, no scientific notation):
- `end_nav` (decimal string)
- `peak_nav_to_date` (decimal string)
- `drawdown_pct` (decimal string, 6dp, negative underwater per C2_DRAWDOWN_CONVENTION_V1)

This bundle MUST compute drawdown per:
- `governance/05_CONTRACTS/C2/drawdown_convention_v1.contract.md`

## 8. Component contracts (must exist, must be governed)
This bundle is compliant only if the following governed contracts exist and are referenced by the availability certificate:

- `governance/05_CONTRACTS/C2/nav_snapshot_truth_v1.contract.md`
- `governance/05_CONTRACTS/C2/nav_history_ledger_v1.contract.md`
- `governance/05_CONTRACTS/C2/drawdown_window_pack_v1.contract.md`
- `governance/05_CONTRACTS/C2/economic_truth_availability_certificate_v1.contract.md`

## 9. Integration requirement
The validator for this bundle MUST be invocable by orchestrators.

The repo’s paper-day orchestrator MUST invoke the bundle validator for `--day_utc <DAY>` in the deterministic stage order, at a proven insertion point.

(Integration is governed separately by code + manifest registration; no invented filenames.)
