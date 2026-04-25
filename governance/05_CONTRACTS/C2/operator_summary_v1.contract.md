---
id: C2_OPERATOR_SUMMARY_V1
title: "C2 Operator Summary Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_operator_summary
---

# C2 Operator Summary Contract (V1)

## Purpose

This contract defines the derived-only operator summary surface for the touched paper-session
startup path.

The current governed summary kind is:

- `preopen`

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_operator_summary_v1.py`
- Canonical helper: `constellation_2/common/operator_summary_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/operator_summary_v1/<DAY>/operator_summary.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/operator_summary.v1.schema.json`

## Authority classification

- This surface is explicitly non-authoritative.
- Under Subsystem Authority Architecture it is legacy diagnostic context only.
- Canonical writer output must set `binding_classification = LEGACY_DERIVED_ONLY`.
- It must not be promoted over `session_authority_status_v1/current.json` or `operator_summary_dossier_v1/<DAY>/operator_summary_dossier.v1.json`.
- It must derive from `trading_day_state_machine_v1`.
- It must not recompute or override control-plane or ledger authority.

## Required inputs

- `trading_day_state_machine_v1`
- `session_readiness_refresh_v1` when present for the requested day

## Fail-closed rules

- If `trading_day_state_machine_v1` is missing or invalid, the summary must fail closed and must not
  publish an artifact that implies independent authority.
- Missing optional context inputs may be reported, but they must not change control-plane truth.

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `blocking_codes[]` must be stable and sorted.
- Summary state, final start decision, and first true blocker must be copied from
  `trading_day_state_machine_v1`, not recomputed from raw fact inputs.

## Consumers

- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/run/c2_preopen_preflight_v1.sh`

## Migration note

This contract is a bounded derived-only helper for the current day-control startup plane.
`paper_session_ledger_v1` remains a supporting canonical input, but it is no longer the only
operator-facing daily startup answer.
