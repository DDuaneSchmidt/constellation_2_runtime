# target_day_build_v1

`target_day_build_v1` is the canonical activation dependency-closure contract for Session Authority.

Canonical output:
- `/home/node/constellation_runtime_data/truth/target_day_build_v1/<DAY>.json`

Rules:
- inputs must be canonical runtime truth only
- every artifact that activation depends on must appear explicitly in `required_artifacts[]`
- PAPER startup build must include `pre_open_bundle_v1` as the canonical startup prerequisite materialization contract
- when `pre_open_bundle_v1` is present, the build may bind `ib_api_handshake_latest_pointer_v1`, `ib_api_handshake_v1`, `global_kill_switch_state_v1`, and `primary_scoped_canonical_authority_head_v1` from the bundle payload rather than re-deriving those prerequisite states independently
- each artifact result must declare:
  - `role_class`
  - canonical path
  - expected and observed target day
  - freshness rule and freshness result
  - provenance requirement and provenance summary
  - closure result and stable blocking reason code
- `role_class` is governed as one of:
  - `REQUIRED_BINDING_INPUT`
  - `REQUIRED_DERIVED_GATE`
  - `REQUIRED_EXECUTION_BOUNDARY`
- exact target-day binding is required for every required artifact
- policy-critical paths must resolve to canonical runtime truth authority only
- PAPER startup build must include `paper_startup_authorization_convergence_v1` and `primary_scoped_authorization_gate_verdict_v1` as required startup authorization artifacts
- PAPER startup build must include `bod_execution_environment_proof_v1` as the governed proof that the BOD-critical Python/import substrate is executable in the owning runtime context
- PAPER startup build must include `paper_startup_intent_input_convergence_v1` as the owned upstream convergence surface for primary-sleeve intent inputs
- PAPER startup build must include `phasec_risk_inputs_prep_v1` as an explicit upstream activation dependency before `startup_materialization_v1`
- PAPER startup build must include `startup_materialization_input_convergence_v1` as the owned upstream convergence surface for canonical startup-materialization inputs
- PAPER startup build must not require the legacy blended `primary_scoped_gate_stack_verdict_v1` for admission binding
- `market_calendar_day` may include `market_calendar_coverage_status_v1` as an upstream `source_ref` when the preventive coverage artifact is available
- freshness must be enforced per artifact class; missing freshness evidence is stale
- provenance is required unless explicitly waived by the build contract
- `blocker_chain[]` must be emitted in causal order from upstream materialization toward downstream execution block
- `hidden_dependency_check_result` must fail closed if downstream payloads reference undeclared activation dependencies
- `closure_status=CLOSED` is required before admission may bind
- `build_status=COMPLETE` alone is insufficient for admission; closure must also be proven
- stable blocker reason codes are governed by `governance/02_REGISTRIES/C2_SESSION_AUTHORITY_BLOCKER_REASON_REGISTRY_V1.json`
- downstream day-open trigger emission must consume `target_day_build_v1` only through binding `target_day_admission_v1`; build does not own timer or open-attempt execution
