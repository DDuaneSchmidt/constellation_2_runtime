# control_plane_semantic_surface_v1

Status: DRAFT
Contract ID: `control_plane_semantic_surface_v1`
Contract Class: `read_boundary_semantic_model`

## Purpose

This contract defines the governed semantic surfaces that may be exposed by
`constellation_2.common.control_plane_read_gateway_v1` during the pre-migration
 semantic-authority phase.

The semantic layer centralizes path resolution and narrow composed read
semantics for existing governed control-plane artifacts. It is not a new
authority surface and it does not create new canonical truth.

## Control-Plane Boundary

For this pre-cutover phase, a read is `control-plane` when its primary purpose is
to consume governed release, policy, session, execution, or operator-status
artifacts in order to:

- decide admission, authorization, or startup readiness
- validate governed control-plane consistency
- render a governed operator-facing control-plane state
- derive another governed control-plane artifact or gate input

A read is `non-control-plane` when it is limited to:

- diagnostic or proof reporting
- release/runtime inspection
- local workspace or execution-kernel discovery that does not itself define
  governed control-plane truth
- non-governed operator advisory or evidence browsing

The boundary classes are:

- `control_plane_semantic_candidate`
- `diagnostic_or_proof_tool`
- `orchestration_layer`
- `invalid_mixed_semantics`

`diagnostic_or_proof_tool` readers may bypass the gateway only when they are
explicitly declared and excluded from read-dominance enforcement.

`orchestration_layer` code may retain non-control-plane workflow logic, but its
governed control-plane reads must route through the gateway.

`invalid_mixed_semantics` code is blocked until the control-plane portion is
split from the non-control-plane portion. It must not be silently exempted.

## Allowed semantic surfaces

- `policy.configuration_activation_family`
- `session.next_day_readiness_probe_inputs`
- `session.fresh_day_admission_inputs`

The gateway may also expose additional simple governed surfaces needed to
support those semantic views and the rewritten validator callers, including:

- `release.release_manifest_for_release_root`
- `policy.paper_policy_verdict`
- `policy.production_policy_verdict`
- `session.market_calendar_day`
- `session.session_promotion_decision`
- `session.day_activation_build`
- `session.day_activation_package`
- `session.global_context_build`
- `session.global_context_package`
- `lifecycle.day_authority_decision`
- `lifecycle.pre_open_bundle`
- `lifecycle.paper_startup_intent_input_convergence`
- `lifecycle.paper_startup_authorization_convergence`
- `lifecycle.startup_materialization_input_convergence`
- `lifecycle.bod_execution_environment_proof`
- `lifecycle.phasec_risk_inputs_prep`
- `lifecycle.capability_state`
- `lifecycle.intents_day_completeness`
- `lifecycle.day_start_blocked`
- `lifecycle.trading_day_state`
- `lifecycle.sleeve_live_readiness`
- `execution.trade_submit_readiness`
- `execution.economic_state_build`
- `execution.economic_state_package`
- `execution.capital_risk_envelope`
- `execution.recurrence_kill_gate`
- `execution.execution_build`
- `execution.execution_package`
- `execution.global_kill_switch_state`
- `execution.authorization_gate_verdict`
- `execution.runtime_control_record`
- `execution.runtime_control_decision`
- `execution.runtime_control_run_envelope`
- `execution.gate_stack_verdict`
- `execution.execution_reconciliation`
- `operator.kill_switch`
- `operator.control_plane_operator_status`
- `operator.transition_timeline_projection`
- `operator.platform_readiness`
- `platform.deployment_state_machine`
- `platform.platform_bug_metrics`

## Invariants

1. The semantic gateway owns path resolution for approved semantic surfaces.
2. The semantic gateway owns the allowed multi-artifact joins for approved
   semantic surfaces.
3. Callers must not recompute those governed semantic views after the gateway
   returns them.
4. Unknown semantic names, unsupported context, and ambiguous requests must fail
   closed.
5. Missing required underlying artifacts must fail closed.
6. Optional underlying artifacts may only remain optional if current repo law
   already defines their absence as a governed `UNKNOWN` or degraded state.
7. The semantic layer must not become ATA, an ownership router, or a new
   canonical current layer.

## Caller prohibition

Runtime, UI, ops-tool, and validator callers that consume one of the approved
semantic surfaces must not:

- construct the governed control-plane paths themselves
- re-read the same governed artifacts directly
- recompute the same semantic join outside the gateway

## Non-authority rule

The semantic layer is a governed read boundary only.

It does not:

- write truth
- promote authority
- replace canonical authority families
- introduce `release_current`, `session_current`, `execution_current`, or
  `operator_readiness_current`

## Explicit Diagnostic Or Proof Bypass Set

The following paths are explicitly non-governed diagnostic or proof readers for
this phase and may read artifacts directly:

- `constellation_2/common/deployment_state_machine_v1.py`
- `ops/tools/refresh_market_data_manifest_and_rerun_gates_v1.sh`
- `ops/tools/run_constellation_root_cause_classifier_v1.py`
- `ops/tools/run_regime_snapshot_v2.py`
- `ops/tools/run_regime_snapshot_v3.py`
- `ops/tools/run_sleeve_live_readiness_v1.py`
- `constellation_2/common/visibility_foundation_v1.py`

All other remaining control-plane-adjacent readers must either consume gateway
surfaces, be classified as orchestration with gateway-routed control-plane
inputs, or be blocked as invalid mixed semantics.
