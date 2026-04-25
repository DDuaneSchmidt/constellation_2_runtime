# control_plane_stage_admission_v1

This contract governs the canonical stage-admission spine for the live startup/control path.

Canonical ordered stages:
1. `CONTROL_STAGE_DAY_ACTIVATION`
   - required upstream admitted stage: none
   - required local authoritative families:
     - `day_activation_build_v1`
     - `day_activation_package_v1`
     - `target_day_admission_v1`
   - stage invariant:
     - day activation closure is complete and the sealed package is context-matched
   - stage output artifact:
     - `control_stage_day_admitted_v1`
   - current surface:
     - none
2. `CONTROL_STAGE_GLOBAL_CONTEXT`
   - required upstream admitted stage:
     - `control_stage_day_admitted_v1`
   - required local authoritative families:
     - `global_context_build_v1`
     - `global_context_package_v1`
   - stage invariant:
     - global context closure is complete and binds the sealed day-activation package
   - stage output artifact:
     - `control_stage_context_admitted_v1`
   - current surface:
     - none
3. `CONTROL_STAGE_SESSION_AUTHORITY`
   - required upstream admitted stage:
     - `control_stage_context_admitted_v1`
   - required local authoritative families:
     - `target_day_build_v1`
     - `target_day_admission_v1`
     - `session_promotion_decision_v1`
     - `active_session_v1/current.json`
   - stage invariant:
     - binding day admission, explicit promotion, and current active session are coherent for the same target day
   - stage output artifact:
     - `control_stage_session_admitted_v1`
   - current surface:
     - `active_session_v1/current.json`
     - this is authoritative, not a projection
4. `CONTROL_STAGE_EXECUTION_BUILD`
   - required upstream admitted stage:
     - `control_stage_session_admitted_v1`
   - required local authoritative families:
     - `execution_build_v1`
     - `execution_package_v1`
   - stage invariant:
     - execution build closure is complete and the sealed execution package binds the same candidate submission
   - stage output artifact:
     - `control_stage_execution_build_admitted_v1`
   - current surface:
     - none

Stage-admission writer law:
- downstream stage admission MUST consume the prior admitted-stage artifact, not an ad hoc mixture of current surfaces and upstream package paths
- stage-admission artifacts MUST be emitted only when the governing family validator passes
- failed validation MUST NOT emit a stage-admitted artifact
- stage-admission artifacts MUST record:
  - stage id
  - admission status
  - governing validator ids
  - required upstream admitted-stage ref
  - required authoritative family refs
  - effective current-surface refs when applicable
  - immutable evidence refs
