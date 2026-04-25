# control_plane_explanation_mapping_v1

This contract governs the deterministic explanation mapping kernel for the certified trust plane.

Core law:
- explanation mapping is table-driven and taxonomy-bound
- explanation may translate and summarize, but MUST NOT invent unsupported meaning
- unsupported taxonomy mappings MUST fail closed

Every explanation payload MUST preserve:
- `stage_id`
- `policy`
- `status`
- `taxonomy_code`
- evidence refs
- authority label
- freshness state

Required output fields:
- `explanation_id`
- `taxonomy_code`
- `stage_id`
- `policy`
- `status`
- `short_message`
- `details_fields`
- `evidence_refs`
- `authority_label`
- `freshness_state`
- `action_class` only when it is one of the fixed enums ratified below

Allowed `action_class` enums:
- `none`
- `provide_upstream_stage_ref`
- `verify_required_artifact`
- `repair_invalid_artifact`
- `remove_forbidden_root_usage`
- `replace_derived_input`
- `review_superseding_transition`
- `historical_only`

