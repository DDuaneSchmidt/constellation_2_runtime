# Aegis Generated Hypothesis Validation Proof Spec v1

## Artifact

`reports/aegis_generated_hypothesis_validation_proof_v1/<day_utc>/generated_hypothesis_validation_proof.v1.json`

## Row Fields

Each `hypothesis_rows[]` item includes:

- `hypothesis_id`
- `hypothesis_name`
- `proposal_state`
- `shadow_validation_state`
- `approval_state`
- `paper_tracking_setup_state`
- `candidate_flow_state`
- `paper_observation_flow_state`
- `outcome_flow_state`
- `validation_sample_flow_state`
- `current_stop_stage`
- `current_stop_reason`
- `blocker_owner`
- `david_action_required`
- `next_expected_step`
- `source_artifact_paths`
- `source_artifact_hashes`
- `computed_at_utc`

## Summary Fields

- `generated_hypotheses_total`
- `reached_candidate_flow_count`
- `reached_paper_observation_count`
- `reached_outcome_count`
- `reached_validation_sample_count`
- `blocked_count`
- `david_action_required_count`
- `furthest_stage_reached`
- `primary_generated_hypothesis_bottleneck`

## Source Artifacts

Primary inputs are `aegis_generated_hypothesis_throughput_v1`, `aegis_data_action_routing_v1`, `aegis_oil_shock_candidate_flow_v1`, candidate contracts, candidate lifecycle, outcome registry, and validation samples.


## Candidate-to-Paper Proof Integration

Generated Hypothesis Validation Proof v1 consumes `aegis_generated_hypothesis_candidate_to_paper_v1` when present for Oil Shock. It surfaces candidate-to-paper status, candidate-contract status, entry-reference certification status, paper-construction status, auto-promotion status, paper-observation-created, outcome-row-created, current stop stage, and current stop reason.

This consumption is read-only. The validation proof does not create candidates, paper observations, outcomes, validation samples, allocation changes, broker actions, trade advice, or safety-gate changes.
