# Aegis Hypothesis Workflow State Spec V1

## Artifact

`reports/aegis_hypothesis_workflow_state_v1/{day}/hypothesis_workflow_state.v1.json`

## States

Allowed `current_state` values are `DISCOVERED`, `TRIAGING`, `NEEDS_DATA`, `REJECTED_AUTOMATICALLY`, `READY_FOR_SHADOW_TRIAL`, `SHADOW_VALIDATION_RUNNING`, `SHADOW_VALIDATION_FAILED`, `SHADOW_VALIDATION_PASSED`, `PAPER_PROMOTION_RECOMMENDED`, `PAPER_PROMOTION_APPROVED`, `PAPER_SETUP_RUNNING`, `PAPER_TRACKING_READY`, `PAPER_TRACKING_BLOCKED`, `PAPER_OBSERVING`, `OUTCOMES_ACCUMULATING`, `STATISTICALLY_SUFFICIENT`, `RETIRE_RECOMMENDED`, and `CAPITAL_REVIEW_RECOMMENDED`.

Allowed `next_action` values are `NONE`, `APPROVE_PAPER_TEST`, `PROVIDE_DATA_SOURCE`, `REVIEW_RETIREMENT`, `REVIEW_CAPITAL`, and `WAIT_FOR_AUTOMATIC_PROCESSING`.

## Record Contract

Each hypothesis record includes:

- `hypothesis_id`
- `display_name`
- `source_type`
- `current_state`
- `prior_state`
- `next_action`
- `allowed_actions`
- `reason_codes`
- `blocker_codes`
- `source_artifact_paths`
- `source_artifact_hashes`
- `input_generated_at_utc`
- `computed_at_utc`
- `stale_input_rejected`
- `state_transition_history`

## Determinism

The payload includes `input_artifact_hashes`, `input_generated_at_utc`, `computed_at_utc`, `deterministic_rerun_id`, `source_artifact_paths`, `stale_input_rejected`, `state_counts`, and `content_hash`.
