# advisory_decision_explanation_mapping_v1

This contract governs the deterministic explanation mapping path for Bundle 8 advisory decisions.

Core law:
- explanation mapping is table-driven
- explanation mapping consumes only the canonical advisory decision object and its governing refs
- unsupported mappings MUST fail closed
- confidence scoring, causal invention, and prose-implied promotion eligibility are forbidden

Every explanation payload MUST preserve:
- `decision_state`
- `actionability_state`
- `freshness_state`
- `visibility_state`
- `invalidation_reason`
- governing refs
- evidence refs
- authority label

Required output fields:
- `explanation_id`
- `decision_state`
- `actionability_state`
- `freshness_state`
- `invalidation_reason`
- `short_message`
- `detail_fields`
- `evidence_refs`
- `authority_label`
- `action_class` only when it is one of the fixed governed enums

Allowed `action_class` enums:
- `none`
- `verify_required_artifact`
- `review_superseding_transition`
- `repair_invalid_artifact`
- `remove_forbidden_root_usage`
- `historical_only`

Ordering law:
- when multiple blocking factors exist, the primary explanation MUST reflect the highest-precedence rule from `advisory_decision_state_precedence_v1`
- secondary factors MAY appear only as ordered detail fields
