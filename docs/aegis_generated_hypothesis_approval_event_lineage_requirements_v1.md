# Aegis Generated Hypothesis Approval Event Lineage Requirements v1

## Intent

Repair the approval-event lineage gap for generated hypotheses without fabricating approval.

## Requirements

- Inspect paper promotion approval queue, approval event JSONL, operator action queue, promotion packet, workflow state, paper setup, and paper blueprint evidence.
- Emit `aegis_generated_hypothesis_approval_event_lineage_v1`.
- Return `APPROVAL_EVENT_FOUND` only when a real immutable approval event with target-day match, schema compatibility, and event hash exists.
- Return deterministic fail-closed statuses for missing event, state-only approval, missing hash, schema mismatch, stale/wrong-day event, or wrong path.
- Preserve source path and source hash for any event used.
- Do not create approval decisions, raw signals, candidates, observations, outcomes, trades, broker actions, allocations, or safety-gate changes.

## Oil Shock Required Fields

- `hypothesis_id`
- `hypothesis_name`
- `approval_lineage_status`
- `approval_event_found`
- `approval_event`
- `approval_event_hash`
- `approval_source_type`
- `approval_source_path`
- `approval_source_hash`
- `approval_actor`
- `approval_timestamp_utc`
- `approval_prior_state`
- `approval_new_state`
- `target_day`
- `source_target_day`
- `reason_codes`
- `david_action_required`
- `source_artifact_paths`
- `source_artifact_hashes`
- `computed_at_utc`

## Investigation Sources

The lineage builder must inspect all deterministic approval sources for Oil Shock:

- Current-day and historical `approval_events.v1.jsonl` rows under `aegis_paper_promotion_approval_queue_v1`.
- Current-day and historical `operator_action_events.v1.jsonl` rows under `aegis_operator_action_event_log_v1`.
- Paper promotion approval queue state.
- Paper promotion packet evidence.
- Workflow state and workflow replay evidence.
- Paper setup and paper sleeve blueprint evidence.
- UI approval endpoint output when represented by an immutable operator action event.

Historical immutable approval events may be reused when they are for the same hypothesis, are approved, schema-compatible, hashed, and not newer than the target day. Same-day event files containing a mismatched target day remain `TARGET_DAY_MISMATCH` and are not silently consumed.

## UI Fields

Generated Hypothesis and Oil Shock UI projections must expose approval lineage status, whether an approval event was found, whether an approval hash is present, remaining blocker, and whether David action is required.

## Historical Lineage Recovery

If the target-day approval event log is missing but an immutable prior-day approval event for the same Oil Shock hypothesis exists at or before the target day, the lineage artifact may report `APPROVAL_EVENT_FOUND` with `HISTORICAL_APPROVAL_EVENT_REUSED`. This recovers a real prior operator approval without creating a new approval decision. Future-dated approval events must not be consumed.

Operator action event logs may be normalized into canonical approval-event lineage only when they contain an explicit approved paper-test action for the same hypothesis. Normalization preserves source path/hash and keeps `approval_created_by_this_artifact` false.

