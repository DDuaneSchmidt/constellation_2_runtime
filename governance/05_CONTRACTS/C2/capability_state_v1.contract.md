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
- `startup_authorization_gate_set_ready`
- `core_sleeve_gate_set_ready`
- `economic_health_gate_set_complete`
- `production_certification_gate_set_complete`

Rules:
- `overall_status` is a policy-neutral summary across all listed capabilities
- PAPER admission policy must bind only to capabilities whose `paper_role` is `BLOCKING` in `governance/02_REGISTRIES/CAPABILITY_POLICY_REGISTRY_V1.json`
- advisory/economic capability failures remain visible here but do not become PAPER startup blockers unless the governed capability policy says so
