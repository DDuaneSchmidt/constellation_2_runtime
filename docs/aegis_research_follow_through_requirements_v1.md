# Aegis Research Follow-Through Control Requirements v1

## Purpose

Research Follow-Through Control closes the loop after Aegis emits research quality, decision, and allocation recommendations. It tracks whether attention recommendations lead to data resolution, repair investigation, candidate flow, sample accumulation, redesign, retirement review, or continued monitoring.

## Scope

This is research workflow follow-through only. It must not implement broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, automatic repair, or automatic retirement.

## Required Flow

Quality Recommendation -> Follow-Up Requirement -> Follow-Up Action -> Follow-Up Status -> Follow-Up Result -> Re-score trigger

## Required Artifact

Artifact id: `aegis_research_follow_through_control_v1`

Each follow-up item must include:

- `follow_up_id`
- `hypothesis_id`
- `hypothesis_name`
- `source_recommendation`
- `source_quality_hash`
- `source_decision_hash`
- `source_allocation_recommendation_hash`
- `follow_up_type`
- `current_status`
- `prior_status`
- `reason_codes`
- `blocking_what`
- `expected_resolution`
- `due_by_utc` or `review_after_days`
- `state_age_days`
- `next_action`
- `requires_david_action`
- `source_artifact_paths`
- `source_artifact_hashes`
- `computed_at_utc`

## Follow-Up Types

- `DATA_SOURCE_RESOLUTION`
- `REPAIR_INVESTIGATION`
- `CANDIDATE_FLOW_WATCH`
- `PAPER_TRACKING_FLOW_WATCH`
- `SAMPLE_ACCUMULATION_WATCH`

## Next Actions

- `NONE`
- `PROVIDE_DATA_SOURCE`
- `INVESTIGATE_REPAIR`
- `MONITOR_AUTOMATICALLY`
- `REVIEW_RETIREMENT`
- `REVIEW_REDIRECT`

## Required Rules

- `NEEDS_DATA` creates `DATA_SOURCE_RESOLUTION`.
- `REDESIGN` or allocation `PAUSE` creates `REPAIR_INVESTIGATION` unless already retired.
- `PAPER_TRACKING_READY` with zero candidates creates `PAPER_TRACKING_FLOW_WATCH`.
- `CONTINUE` with candidates but no samples creates `CANDIDATE_FLOW_WATCH`.
- `CONTINUE` with included samples below threshold creates `SAMPLE_ACCUMULATION_WATCH`.
- Follow-through must not mutate allocation automatically.
- Follow-through must not repair code automatically.
- Follow-through may recommend repair, redesign, retirement review, or continued monitoring.

## Safety

Every output must preserve research-only safety flags and explicitly disallow broker execution, trade advice, live trading, real capital, autonomous execution, automatic repair, and allocation mutation.
