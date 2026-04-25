# control_stage_transition_record_v1

This contract governs the durable immutable stage-transition record artifact.

Artifact role:
- `control_stage_transition_record_v1` is the audit center of gravity for Bundle 5 stage evaluation, admission, certification, and recovery actions.
- It is authoritative immutable audit truth under the canonical control truth root.

Required fields:
- `stage_id`
- `transition_request_type`
- `transition_status`
- `upstream_stage_ref`
- `authoritative_inputs_used`
- `frozen_inputs_used`
- `input_resolution_mode`
- `invariant_results`
- `validator_kernel_version`
- `boundary_validator_result`
- `family_validator_result`
- `evidence_refs`
- `admission_decision`
- `certification_decision`
- `supersedes_ref`
- `superseded_by_ref`
- `failure_taxonomy`
- `explanation_payload`

Behavior:
- blocked, admitted, certified, recomputed, and superseded transition actions MUST be inspectable through this artifact family
- `explain_blocked` and `evaluate` MAY remain read-only and therefore need not emit a record
- every mutating recovery or supersession action MUST emit a transition record

