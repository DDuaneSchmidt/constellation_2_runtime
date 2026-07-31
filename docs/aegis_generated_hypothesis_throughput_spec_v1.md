# Aegis Generated Hypothesis Throughput Spec v1

## Artifact

`reports/aegis_generated_hypothesis_throughput_v1/{day}/generated_hypothesis_throughput.v1.json`

## Schema

- `schema_id`: `aegis_generated_hypothesis_throughput_v1`
- `day_utc`
- `computed_at_utc`
- `generated_hypotheses`
- `summary`
- `input_artifact_hashes`
- `source_artifact_paths`
- `deterministic_rerun_id`
- `content_hash`

## Row Fields

- `hypothesis_id`
- `hypothesis_name`
- `proposal_state`
- `approval_state`
- `paper_setup_state`
- `candidate_count`
- `paper_observation_count`
- `open_observations`
- `closed_outcomes`
- `included_validation_samples`
- `excluded_validation_samples`
- `days_since_proposal`
- `days_since_approval`
- `throughput_status`
- `next_expected_step`
- `no_david_action_required_unless_blocked`

## UI Contract

The Research UI renders generated hypothesis throughput status from this artifact. It does not infer throughput from labels or local state.
