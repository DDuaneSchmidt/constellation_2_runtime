# Aegis Generated Hypothesis Approval Event Lineage Spec v1

## Artifact

`aegis_generated_hypothesis_approval_event_lineage_v1`

Path:

`reports/aegis_generated_hypothesis_approval_event_lineage_v1/{day}/generated_hypothesis_approval_event_lineage.v1.json`

## Status Codes

- `APPROVAL_EVENT_FOUND`
- `APPROVAL_EVENT_MISSING`
- `APPROVAL_EVENT_NOT_HASHED`
- `APPROVAL_EVENT_WRITTEN_TO_WRONG_PATH`
- `APPROVAL_EVENT_SCHEMA_MISMATCH`
- `APPROVAL_QUEUE_STATE_ONLY_NO_EVENT`
- `STALE_APPROVAL_ARTIFACT`
- `TARGET_DAY_MISMATCH`

## Authority Rules

The canonical source is the append-only approval event JSONL. Queue state may identify that an action is needed, but queue state alone is not an immutable approval event.

If an event exists but its `day_utc` or `target_day` does not match `TARGET_DAY`, lineage reports `TARGET_DAY_MISMATCH`.

If an event exists without `event_hash` or `approval_event_hash`, lineage reports `APPROVAL_EVENT_NOT_HASHED`.

If only approved queue state exists, lineage reports `APPROVAL_QUEUE_STATE_ONLY_NO_EVENT` and does not synthesize approval.

## Safety

The artifact is lineage-only. It cannot approve hypotheses, create candidates, create observations, allocate capital, submit orders, enable trade advice, or change live trading state.

## Source Precedence

1. Current-day canonical approval event JSONL.
2. Current-day operator action event log, normalized to a canonical approval event only when it records `APPROVE_PAPER_TEST` and `PAPER_PROMOTION_APPROVED` for Oil Shock.
3. Historical canonical approval event JSONL up to the target day.
4. Historical operator action event log up to the target day.
5. Queue latest approval event, paper setup, or blueprint hashes only when they already reference an approval hash.

Historical recovery reports `APPROVAL_EVENT_FOUND` with `HISTORICAL_APPROVAL_EVENT_REUSED` in `reason_codes`, preserves the source target day, source path, and source hash, and does not create a new approval decision.

## UI Projection

Downstream generated-hypothesis progress rows expose:

- `approval_lineage_status`
- `approval_event_found`
- `approval_event_hash_present`
- `approval_source_path`
- `remaining_blocker`
- `remaining_missing_fields`
- `david_action_required`
- `approval_lineage_message`

## Historical And Operator Event Sources

The producer searches the target-day approval JSONL first, then target-day operator action event logs, then historical approval/operator event logs dated on or before `target_day`. Historical recovery emits `APPROVAL_EVENT_FOUND` plus reason code `HISTORICAL_APPROVAL_EVENT_REUSED`; operator-event normalization emits `UI_OPERATOR_ACTION_EVENT_NORMALIZED`. Both paths require an existing immutable event and stable event hash, and neither path creates a new approval.

