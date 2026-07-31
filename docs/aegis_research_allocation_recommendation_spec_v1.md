# Aegis Research Allocation Recommendation Spec v1

## Artifact Contract

Artifact id: `aegis_research_allocation_recommendation_v1`

Canonical path:

`reports/aegis_research_allocation_recommendation_v1/{day}/research_allocation_recommendation.v1.json`

## Top-Level Fields

- `schema_id`: `aegis_research_allocation_recommendation`
- `schema_version`: `v1`
- `artifact_id`: `aegis_research_allocation_recommendation_v1`
- `day_utc`
- `scoring_version`
- `decision_policy_version`
- `allocation_policy_version`
- `input_artifact_hashes`
- `input_generated_at_utc`
- `computed_at_utc`
- `deterministic_rerun_id`
- `source_artifact_paths`
- `recommendations`
- `summary`
- `content_hash`
- safety flags

## Recommendation Row

Each row must include:

- `hypothesis_id`
- `name`
- `decision_recommendation`
- `current_allocation_weight`
- `recommended_allocation_action`
- `recommended_weight_delta`
- `reason_codes`
- `confidence_level`
- `requires_david_review`
- `source_artifact_paths`

## Determinism

For identical inputs, recommendation rows and content hashes must remain stable except explicitly excluded generated timestamps.
## Outcome Performance Metrics Integration

Allocation recommendations consume outcome-performance-informed decisions. Each recommendation carries the decision's outcome metric snapshot and remains advisory.

Policy rules:

- `INCREASE` is not allowed solely from candidate count.
- `CAPITAL_REVIEW` is not allowed without statistical sufficiency and hard-gate clearance.
- Poor early underpowered outcomes may justify `HOLD` or `DECREASE` with `LOW` confidence.
- `PAUSE` and retirement review require an explicit decision-policy basis.
- The recommendation artifact must not mutate actual allocation weights or real-capital state.
