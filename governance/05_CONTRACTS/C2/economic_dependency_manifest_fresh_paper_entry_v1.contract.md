
---
contract_id: C2_ECONOMIC_DEPENDENCY_MANIFEST_FRESH_PAPER_ENTRY_V1
owner: governance
status: active
schema_id: c2_contract
schema_version: v1
---

# Economic Dependency Manifest: Fresh Paper Entry v1

This manifest governs the economic closure graph for `fresh_paper_entry_v1`.

## Required nodes

- `canonical_authority_head_v1`
- `positions_snapshot_v2`
- `accounting_nav_v2`
- `allocation_summary_v1`
- `correlation_envelope_gate_v1`
- `exposure_net_v1`
- `capital_risk_envelope_v2`
- `capital_authority_allocation_v1`

## Excluded from economic closure

- `engine_activity_authorization_v1` remains downstream execution authorization
- `trade_submit_readiness_c2_v1` remains downstream broker/readiness authority

## Canonical stage order

1. `CONTROL_BASELINE`
2. `PORTFOLIO_BASELINE`
3. `CAPITAL_AUTHORITY`
4. `SEAL`
