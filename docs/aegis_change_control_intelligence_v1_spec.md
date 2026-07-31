# Aegis Change Control Intelligence Layer V1 Specification

## Artifacts

### aegis_change_control_evidence_snapshot_v1

Path:

```text
truth/reports/aegis_change_control_evidence_snapshot_v1/<day>/change_control_evidence_snapshot.v1.json
```

Required fields:

* schema_id
* snapshot_id
* target_day
* generated_at
* register_source_path
* register_source_hash
* total_records
* open_p0_p1_records
* blocked_parent_records
* required_child_records
* incomplete_child_rollups
* failed_hostile_audits
* validation_gaps
* records_awaiting_decision
* records_awaiting_validation
* latest_audit_status
* latest_portal_smoke_status
* latest_runtime_truth
* source_evidence_refs

### aegis_change_control_advisor_score_v1

Path:

```text
truth/reports/aegis_change_control_advisor_score_v1/<day>/change_control_advisor_score.v1.json
```

Required fields:

* schema_id
* advisor_score_id
* target_day
* generated_at
* input_snapshot_id
* input_snapshot_hash
* register_source_hash
* stale_snapshot_flag
* scores
* top_recommended_record_id
* source_evidence_refs

Each score row includes:

* record_id
* title
* priority_score
* score_components
* blocked_by
* blocks_records
* risk_level
* decision_required
* validation_required
* stale_recommendation_flag
* recommended_next_action

### aegis_change_control_ai_review_v1

Path:

```text
truth/reports/aegis_change_control_ai_review_v1/<day>/change_control_ai_review.v1.json
```

Required fields:

* schema_id
* review_id
* target_day
* input_snapshot_id
* input_advisor_score_id
* generated_at
* model_or_agent_id
* plain_english_summary
* recommended_next_actions
* risk_explanations
* suggested_decision_notes
* suggested_codex_prompts
* possible_missing_work
* confidence
* human_decision_required
* expiration_or_staleness_rule
* forbidden_actions
* mutation_performed

## Recommendation Categories

* FIX_BLOCKING_CHILD
* VALIDATE_IMPLEMENTED_PARENT
* RECORD_DECISION
* COLLECT_VALIDATION_EVIDENCE
* REVIEW_STALE_OR_CONTRADICTORY_RECORD
* MONITOR

## Scoring Model

Components are additive and deterministic:

* P0 severity weight
* P1 severity weight
* blocked parent weight
* required child blocker weight
* failed hostile audit weight
* dependency impact weight
* safety impact weight
* strategic leverage weight
* stale evidence penalty
* already implemented penalty
* already validated penalty

## Expiration / Staleness Rules

A recommendation is stale when the snapshot register hash differs from the current register hash or when the snapshot target_day differs from the requested day.

## Human Decision Linkage

AI reviews may include suggested decision notes, but accepted/rejected human decisions must be recorded through Change Control decision records. The AI review is evidence, not authority.

## Validation Rules

Validation fails if:

* snapshot lacks register hash
* advisor lacks snapshot reference
* advisor top recommendation is not deterministic from scores
* AI review omits forbidden action boundary
* AI review reports mutation_performed=true
* AI review contains close/validate/implement/approve/reject/defer/prioritize authority claims
