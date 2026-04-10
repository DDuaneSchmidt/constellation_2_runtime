# capability_state_v1

`capability_state_v1` is the policy-neutral capability layer for Constellation readiness.

It derives a small stable set of operational capabilities from the shared canonical evidence DAG without duplicating any producer.

Canonical output:
- `<truth_root>/reports/capability_state_v1/<DAY>/capability_state.v1.json`

Required capabilities:
- `account_binding_valid`
- `startup_materialization_ready`
- `paper_trading_posture_ready`
- `broker_connectivity_available`
- `core_sleeve_gate_set_ready`
- `production_certification_gate_set_complete`
