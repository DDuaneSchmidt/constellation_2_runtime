# control_plane_transition_engine_v1

This contract governs the Bundle 5 certified stage machine for the live control plane.

Core law:
- `constellation_2.common.control_plane_transition_engine_v1` is the only legal evaluation path for control-plane stage transitions.
- Orchestrators request transitions only.
- The transition engine evaluates.
- Admission, certification, and any current-surface projection are policy outcomes of the same evaluation core.

Deterministic evaluation object:
- Every stage transition request MUST first produce one deterministic evaluation result.
- Required fields:
  - `stage_id`
  - `policy`
  - `upstream_stage_ref`
  - `authoritative_inputs_used`
  - `frozen_inputs_used`
  - `invariants_checked`
  - `invariant_failures`
  - `validator_results`
  - `admissible_boolean`
  - `certifiable_boolean`
  - `blocked_reason_codes`
  - `supersession_determination`
  - `mutation_allowed_boolean`
  - `persistence_plan`

Policy model:
- supported policies:
  - `evaluate`
  - `admit_and_certify`
  - `certify_only`
  - `recompute_frozen`
  - `supersede_from_new_inputs`
  - `explain_blocked`
- policies may differ only in:
  - allowed input-resolution mode
  - whether mutation is legal
  - which governed outputs are emitted

Bans:
- validators, runners, or orchestrators MUST NOT implement parallel control semantics outside the transition engine
- orchestrators MUST NOT perform root resolution
- orchestrators MUST NOT duplicate invariant evaluation
- orchestrators MUST NOT perform stage admission or certification directly
- recovery and recompute logic MUST NOT exist outside the transition engine

