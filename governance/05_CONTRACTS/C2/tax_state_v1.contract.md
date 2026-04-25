# tax_state_v1

This contract governs the durable Bundle 10 canonical tax state artifact family.

Artifact class:
- `outcome_record`

Authoritative writer:
- `constellation_2.common.tax_state_kernel_v1`

Authoritative path pattern:
- `{canonical_truth_root}/reports/tax_state_v1/{day_utc}/{scope_id}/{tax_state_id}/tax_state.v1.json`

Required identity and authority fields:
- `tax_state_id`
- `authority_label`
- `kernel_version`
- `explanation_mapping_version`
- `generated_at_utc`
- `target_day`
- `scope_id`
- `tax_scope_id`

Required state fields:
- `completeness_state`
- `freshness_state`
- `visibility_state`
- `blocker_states`
- `opportunity_states`
- `lot_basis_state`
- `holding_period_state`
- `wash_sale_state`
- `primary_rule_id`

Required governing refs:
- `governing_refs`
- `evidence_refs`
- `advisory_binding_state.tax_state_ref` when advisory binding is emitted

Required explanation fields:
- `primary_explanation`
- `primary_explanation.explanation_id`
- `primary_explanation.completeness_state`
- `primary_explanation.freshness_state`
- `primary_explanation.blocker_states`
- `primary_explanation.opportunity_states`
- `primary_explanation.degraded_reason_id`
- `primary_explanation.short_message`
- `primary_explanation.detail_fields`
- `primary_explanation.evidence_refs`
- `primary_explanation.authority_label`

Historical and lineage law:
- the artifact family is append-only
- a superseding tax state MUST preserve explicit lineage through `supersedes_ref`
- `superseded_by_ref` MAY be present only when it is directly provable at evaluation time
- `historical_visibility` MUST explicitly state whether the state is current, historical-only, or suppressed

Authority ceiling:
- the artifact MUST NOT claim authority beyond the deterministic tax and portfolio truth it cites
- the artifact MUST NOT become authority for policy or control-plane state outside the governed tax-binding path

