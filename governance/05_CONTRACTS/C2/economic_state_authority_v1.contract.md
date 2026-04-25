
---
contract_id: C2_ECONOMIC_STATE_AUTHORITY_V1
owner: governance
status: active
schema_id: c2_contract
schema_version: v1
---

# Economic State Authority v1

## Purpose

Economic State Authority is the canonical compiler/sealer for pre-submit economic closure for a governed sleeve/day/mode/account context.

It does not own raw business truth. It compiles raw economic truth, invokes owned producers in governed order, validates semantic consistency, emits `economic_state_build.v1`, and may emit `economic_state_package.v1` only when closure is complete.

## Upstream package and raw economic nodes it consumes

- `global_context_package_v1`
- `positions_snapshot_v2`
- `accounting_nav_v2`
- `allocation_summary_v1`
- `correlation_envelope_gate_v1`
- `exposure_net_v1`
- `capital_risk_envelope_v2`
- `capital_authority_allocation_v1`

## Owned materialization chain

`correlation_envelope_gate_v1` remains the raw economic truth owner for the gate artifact, but its governed producer lifecycle is:

- `engine_daily_returns_v1`
- `engine_correlation_matrix_v1`
- `correlation_envelope_gate_v1`

Economic State Authority may invoke that chain in canonical order when the gate is materializable and upstream blockers are clear.

`capital_authority_allocation_v1` remains the raw economic truth owner for deterministic intent-level capital permission. Its governed producer lifecycle within the economic layer is:

- `exposure_net_v1`
- `capital_risk_envelope_v2`
- `correlation_envelope_gate_v1`
- `capital_authority_allocation_v1`

Economic State Authority must continue materializing within the `CAPITAL_AUTHORITY` stage until newly unblocked producers for that stage have been evaluated.

`allocation_summary_v1` may remain materialized as a legacy advisory summary, but it is not the canonical capital-permission authority and it must not override `capital_authority_allocation_v1` or sleeve-edge qualification outputs in the control path.

## Seal rule

`economic_state_package.v1` may exist only when every required node in the governed manifest is PRESENT and semantically compatible for the same day/sleeve/mode/account context.

## Role boundary

- Raw truth owners remain authoritative for their own artifacts.
- Economic State Authority owns only compile/materialize/seal logic for economic closure.
- Execution Build Authority consumes the sealed economic package in its primary path.
- Submit Boundary must not reopen raw economic nodes in the normal primary path.
