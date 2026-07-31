# Aegis Macro Calendar Data Readiness Spec v1

## Artifact

`aegis_macro_calendar_data_readiness_v1`

Path:

`reports/aegis_macro_calendar_data_readiness_v1/{day}/macro_calendar_data_readiness.v1.json`

## Governed Source Detection

The artifact checks for a governed macro calendar source at:

`reports/aegis_macro_calendar_source_v1/{day}/macro_calendar_source.v1.json`

The source may expose rows under `events`, `macro_events`, `rows`, or `data`.

## Output Fields

Top-level fields:

- `schema_id`
- `schema_version`
- `artifact_id`
- `day_utc`
- `computed_at_utc`
- `status`
- `macro_calendar_ready`
- `david_action_required`
- `buttons`
- `missing_fields`
- `required_fields`
- `supported_event_types`
- `event_count`
- `valid_event_count`
- `invalid_event_count`
- `next_step`
- `message`
- `source_status`
- `source_artifact_path`
- `source_artifact_hash`
- `row_validation`
- `downstream_effects`
- `safety`
- `content_hash`

## Status Rules

If no source artifact exists:

- `status: NEEDS_SOURCE`
- `macro_calendar_ready: false`
- `david_action_required: true`
- `buttons: ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"]`
- `message: Macro Calendar needs a governed macro event calendar source.`

If a source exists but any required field is absent, blank, or invalid:

- `status: SOURCE_INCOMPLETE`
- `macro_calendar_ready: false`
- `missing_fields` lists deterministic field names
- `david_action_required: true`

If a source exists and all rows satisfy the data contract:

- `status: READY`
- `macro_calendar_ready: true`
- `david_action_required: false`
- `next_step: rerun shadow validation using governed macro calendar source`

## Downstream Consumption

`aegis_hypothesis_workflow_state_v1`, `aegis_research_quality_engine_v1`, `aegis_research_follow_through_control_v1`, `aegis_generated_hypothesis_throughput_v1`, and `aegis_research_daily_scorecard_v1` may consume this artifact. They may remove the macro event calendar missing-data blocker only when `macro_calendar_ready` is true. They must not mark shadow validation passed solely because this artifact is ready.
