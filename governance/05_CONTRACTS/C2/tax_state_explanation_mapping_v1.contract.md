# tax_state_explanation_mapping_v1

This contract governs the deterministic explanation mapping path for Bundle 10 tax state.

Core law:
- explanation mapping is table-driven
- explanation mapping consumes only the canonical tax state object and its governing refs
- unsupported mappings MUST fail closed
- freeform tax inference, freeform confidence language, and unsupported action framing are forbidden

Every explanation payload MUST preserve:
- `completeness_state`
- `freshness_state`
- `blocker_states`
- `opportunity_states`
- `degraded_reason_id`
- governing refs
- evidence refs
- authority label

Required output fields:
- `explanation_id`
- `completeness_state`
- `freshness_state`
- `blocker_states`
- `opportunity_states`
- `degraded_reason_id`
- `short_message`
- `detail_fields`
- `evidence_refs`
- `authority_label`
- `action_class` only when it is one of the fixed governed enums

Allowed `action_class` enums:
- `none`
- `verify_tax_truth`
- `review_tax_blocker`
- `inspect_historical_tax_state`
- `wait_for_fresh_tax_state`

Ordering law:
- when multiple blocking or degraded factors exist, the primary explanation MUST reflect the highest-precedence rule from `tax_state_precedence_v1`
- secondary factors MAY appear only as ordered detail fields

