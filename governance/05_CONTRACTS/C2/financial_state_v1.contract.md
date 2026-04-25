---
id: C2_FINANCIAL_STATE_V1
title: "Financial State V1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git
scope: phaseL_financial_state
---

# Financial State V1

## Purpose

`financial_state_v1` is the canonical Phase L financial authority surface for
Command and Portfolio. The backend owns computation. The UI is a thin renderer.

## Required Inputs

All of the following inputs are mandatory for a canonical `financial_state_v1`
payload for day `<DAY>`:

- `GLOBAL_TRUTH_ROOT/accounting_v2/nav/<DAY>/nav.v2.json`
- `GLOBAL_TRUTH_ROOT/cash_ledger_v1/snapshots/<DAY>/cash_ledger_snapshot.v1.json`
- `SLEEVE_TRUTH_ROOT/positions_v1/snapshots/<DAY>/positions_snapshot.v5.json`
- `SLEEVE_TRUTH_ROOT/risk_v1/exposure_net_v1/<DAY>/exposure_net.v1.json`
- `SLEEVE_TRUTH_ROOT/risk_v1/portfolio_governance_snapshot_v1/<DAY>/portfolio_governance_snapshot.v1.json`
- `REPO_ROOT/governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json`

## Failure Model

- If any required input is missing, unreadable, malformed, or semantically
  unproven, the backend MUST fail closed.
- Fail closed means:
  - `truth_state="fail_closed"`
  - `data_condition="fail_closed"`
  - `financial_status="FAIL_CLOSED"`
  - no authoritative totals may be synthesized or backfilled in the UI

## Top-Level Shape

The payload MUST include:

- `view_name="financial_state"`
- `surface_kind="projection"`
- `entity_scope="financial_state"`
- `contract_id="financial_state_v1"`
- `contract_version="v1"`
- `truth_state`
- `data_condition`
- `as_of_utc`
- `freshness_state`
- `source_authority`
- `source_refs`
- `provenance_refs`
- `degradation_codes`
- `authority_inputs`
- `current_day`
- `financial_status`
- `financial_warnings`
- `runtime_context`
- `investable_summary`
- `account_rollups`
- `account_rollup_summary`
- `holdings_rollup`
- `liquidity_summary`
- `reserve_summary`
- `exposure_summary`
- `concentration_summary`
- `backing_status`

## Authority Semantics

- `investable_summary` is owned by `accounting_nav_v2`.
- `liquidity_summary` is owned by `cash_ledger_snapshot_v1`.
- `account_rollups` are owned by `positions_snapshot_v5` joined only with
  governed account registry and cash-ledger proof.
- `holdings_rollup` is owned by accounting NAV components with explicit
  positions-backed account mapping where provable.
- `reserve_summary` is owned by `portfolio_governance_snapshot_v1`.
- `exposure_summary` and `concentration_summary` are owned by `exposure_net_v1`.

## UI Boundary

- Command and Portfolio MUST consume this exact backend contract.
- The UI MUST NOT recompute competing stitched totals from positions, orders,
  or advisory surfaces once `financial_state_v1` is available.
