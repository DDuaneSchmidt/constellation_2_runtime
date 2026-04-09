---
id: C2_STARTUP_MATERIALIZATION_INPUTS_PREP_V1
title: "C2 Startup Materialization Inputs Prep Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_fact_plane
---

# C2 Startup Materialization Inputs Prep Contract (V1)

## Purpose

This contract defines the canonical source-authoritative fact surface that prepares the upstream inputs required for `startup_materialization_v1` before Phase C identity release is attempted.

This surface is domain fact only.

It is not session authority, readiness authority, admission authority, or submit authority.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_startup_materialization_inputs_prep_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/startup_materialization_inputs_prep_v1/<DAY>/startup_materialization_inputs_prep.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization_inputs_prep.v1.schema.json`

## Required inputs

- same-day paper intents under `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`
- same-day market-data spine under `constellation_2/runtime/truth/market_data_snapshot_v1/`
- liquidity gate report under `constellation_2/runtime/truth/reports/liquidity_slippage_gate_v1/<DAY>/` when same-day market close is unavailable

## Required meaning

The artifact must identify:

- `day_utc`
- `session_id`
- closed prep `status`
- `equity_entry_symbols[]`
- deterministic `default_equity_reference_price` when one can be source-authoritatively resolved
- `required_inputs_checked[]`
- stable `blocking_codes[]`
- producer identity
- `produced_at_utc`
- explicit `authority_scope = NON_AUTHORITY_FACT`

## Allowed status values

- `PASS`
- `BLOCKED_VALID`
- `BLOCKED_BY_DEFECT`

## Fail-closed rules

- Missing same-day intents must not yield `PASS`.
- Multi-symbol equity entry reference-price ambiguity must not yield `PASS`.
- Missing or malformed fallback liquidity-gate evidence must not yield `PASS`.
- The surface must never claim startup readiness or session authority.

## Consumers

- `ops/tools/run_startup_materialization_v1.py`

## Downstream prohibition

Consumers must not guess a default equity reference price when this governed surface is available.
