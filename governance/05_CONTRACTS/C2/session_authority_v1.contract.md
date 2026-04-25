# session_authority_v1

`session_authority_v1` is the single owner of target-day construction, binding admission, and active-session rollover.

Rules:
- canonical runtime truth under `/home/node/constellation_runtime_data` is the only operational truth family
- repo-local `constellation_2/runtime/*` may not participate in paper or live admission decisions
- `target_day_build_v1` is the complete activation dependency contract for Session Authority
- `pre_open_bundle_v1` is the canonical startup prerequisite bundle for PAPER Session Authority day activation
- `session_promotion_decision_v1` is the explicit promotion boundary between startup prerequisite truth and `active_session_v1/current.json`
- for PAPER startup prerequisites owned by the pre-open layer, Session Authority must consume `pre_open_bundle_v1` rather than re-deriving scattered prerequisite state independently
- Session Authority must not advance `active_session_v1/current.json.active_day` until `session_promotion_decision_v1.promotion_state == PROMOTED`
- `target_day_build_v1` must prove closure, not merely artifact presence, before `target_day_admission_v1` may bind
- the governed BOD-critical execution substrate proof is:
  - `bod_execution_environment_proof_v1`
- PAPER startup admission must bind to the lifecycle-aware authorization surface:
  - `paper_startup_intent_input_convergence_v1`
  - `paper_startup_authorization_convergence_v1`
  - `primary_scoped_authorization_gate_verdict_v1`
- canonical startup materialization must bind its owned upstream convergence surface:
  - `phasec_risk_inputs_prep_v1`
  - `startup_materialization_input_convergence_v1`
  - `startup_materialization_v1`
- `operator_day_authority_summary_v1` is the canonical day-scoped binding summary for operators
- `day_failure_causality_v1` is the canonical first-failure chain for blocked or failed startup days
- `day_open_trigger_v1` is the canonical authority-to-open trigger emitted only after binding admission and granted session authority align
- for `environment = PAPER`, that trigger may emit whenever readiness and authority are currently granted, without same-day cap or clock-window suppression in this pass
- for non-paper environments, existing governed open-window behavior remains strict
- `day_open_attempt_v1` is the canonical record of whether the governed day-open action was consumed and executed
- Session Authority may emit `day_open_trigger_v1`, but it does not itself execute the open command
- non-paper late authority flips may produce one bounded re-entry trigger inside the governed open window; no unbounded retry loop is permitted
- legacy `primary_scoped_gate_stack_verdict_v1` may remain as diagnostic context, but it may not own PAPER startup admission
- `target_day_admission_v1` is the only binding day-entry decision
- `active_session_v1/current.json` is the only authority for which day is active
- `active_session_v1/current.json` is a promotion output of Session Authority; it must not advance from pre-open or producer-local truth directly
- `session_authority_status_v1/current.json` is the derived operator status surface for the control plane
- `session_authority_alert_v1/current.json` is the derived alert surface for the control plane
- wrappers and execution consumers must not derive the active day from wall-clock time
- rollover must fail closed when admission is `BLOCKED`
- rollover must also fail closed when promotion is `BLOCKED` or `FAILED_VALIDATION`
- withheld rollover must remain explicit and operator-readable
- stable blocker reason codes are governed by `governance/02_REGISTRIES/C2_SESSION_AUTHORITY_BLOCKER_REASON_REGISTRY_V1.json`
- `next_day_readiness_probe_v1` remains advisory unless separately promoted by governance
