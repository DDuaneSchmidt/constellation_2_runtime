# core2_core1_dependency_gate_v1.contract

owner: core2_core1_dependency_gate_v1
scope: Core 2 dependency gate on Core 1 broker evidence

Core 2 may consume only these canonical Core 1 inputs:
- observation_session_fact.v1.jsonl
- observed_order_fact.v1.jsonl
- observed_order_status_fact.v1.jsonl
- observed_fill_fact.v1.jsonl
- observed_position_fact.v1.jsonl
- broker_observation_health.v1.json
- broker_observation_trust_dependency.v1.json

Forbidden direct Core 2 inputs:
- broker_raw_evidence_envelope.v1.jsonl as canonical truth
- legacy broker-event mirrors
- any operator/audit surface as truth owner

Consumption rules:
- Core 2 must obey broker_observation_trust_dependency_v1 exactly.
- core2_consumption_mode=NORMAL_ONLY permits normal Core 2 consumption.
- core2_consumption_mode=DEGRADED_ALLOWED permits only explicit degraded Core 2 consumption.
- core2_consumption_mode=FAIL_CLOSED forbids Core 2 consumption.
- core1_pre_core2_readiness_v1 is the derived readiness proof surface for this gate.

Identity and legacy rules:
- Core 2 must fail closed if identity_stability_status != STABLE.
- Core 2 must fail closed if legacy_boundary_status != HARDENED_RUNTIME_DAY.
- Mixed hardened and legacy runtime rows are forbidden.
