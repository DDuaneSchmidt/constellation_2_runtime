# Aegis Research Follow-Through Control Spec v1

## Artifact Contract

Canonical path:

`reports/aegis_research_follow_through_control_v1/{day}/research_follow_through_control.v1.json`

## Top-Level Fields

- `schema_id`: `aegis_research_follow_through_control`
- `schema_version`: `v1`
- `artifact_id`: `aegis_research_follow_through_control_v1`
- `day_utc`
- `follow_through_policy_version`
- `scoring_version`
- `decision_policy_version`
- `allocation_policy_version`
- `input_artifact_hashes`
- `input_generated_at_utc`
- `computed_at_utc`
- `deterministic_rerun_id`
- `source_artifact_paths`
- `follow_ups`
- `summary`
- `content_hash`
- safety flags

## Status Sets

`DATA_SOURCE_RESOLUTION` statuses:

- `OPEN`
- `CONNECTED`
- `UPLOADED`
- `UNAVAILABLE`
- `DEFERRED`

`REPAIR_INVESTIGATION` statuses:

- `OPEN`
- `REPAIR_STARTED`
- `REPAIRED`
- `RETIRE_RECOMMENDED`
- `DEFERRED`

`CANDIDATE_FLOW_WATCH` statuses:

- `WATCHING`
- `FLOW_STARTED`
- `STALLED`
- `ESCALATE_TO_REDESIGN`

`PAPER_TRACKING_FLOW_WATCH` statuses:

- `WATCHING`
- `CANDIDATES_STARTED`
- `STALLED`

`SAMPLE_ACCUMULATION_WATCH` statuses:

- `WATCHING`
- `SAMPLE_FLOW_OK`
- `SAMPLE_FLOW_SLOW`
- `SUFFICIENCY_REACHED`

## Summary

The summary must include counts by follow-up type, status, David action requirement, automatic watches, repair investigations, sample accumulation watches, and stalled or overdue rows.

## Determinism

For the same target day and identical inputs, follow-up rows and content hashes must remain stable except generated timestamp fields excluded from the hash.
