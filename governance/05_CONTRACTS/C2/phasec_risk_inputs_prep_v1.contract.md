---
id: C2_PHASEC_RISK_INPUTS_PREP_V1
title: "C2 Phase-C Risk Inputs Prep Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_fact_plane
---

# C2 Phase-C Risk Inputs Prep Contract (V1)

## Purpose

This contract defines the canonical source-authoritative fact surface that prepares the drawdown and risk inputs required by Phase-C entry transformation before `startup_materialization_v1` invokes Phase-C identity release.

This surface is domain fact only.

It is not session authority, readiness authority, admission authority, or submit authority.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_phasec_risk_inputs_prep_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/phasec_risk_inputs_prep_v1/<DAY>/phasec_risk_inputs_prep.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/phasec_risk_inputs_prep.v1.schema.json`

## Required inputs

- `constellation_2/runtime/truth/accounting_v2/nav/<DAY>/nav.v2.json`
- historical `accounting_v2/nav` days up to and including `<DAY>` when drawdown must be derived
- canonical compat output under `constellation_2/runtime/truth/accounting_compat_v1/nav/<DAY>/nav_snapshot.v1.json`

## Required meaning

The artifact must identify:

- `day_utc`
- `session_id`
- closed prep `status`
- canonical compat NAV path used by Phase-C risk transformation
- resolved `drawdown_pct`
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

- Missing source NAV must not yield `PASS`.
- Missing or malformed compat NAV output must not yield `PASS`.
- Missing `history.drawdown_pct` must not yield `PASS`.
- The surface must never claim startup readiness or session authority.

## Consumers

- `ops/tools/run_startup_materialization_v1.py`

## Downstream prohibition

Consumers must not guess drawdown or NAV-compatible risk inputs when this governed surface is available.
