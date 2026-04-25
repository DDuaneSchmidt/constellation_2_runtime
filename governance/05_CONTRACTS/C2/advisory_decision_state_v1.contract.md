# advisory_decision_state_v1

This contract governs the durable Bundle 8 advisory decision artifact family.

Artifact class:
- `read_model`

Authoritative writer:
- `constellation_2.common.advisory_decision_state_kernel_v1`

Authoritative path pattern:
- `{canonical_truth_root}/reports/advisory_decision_state_v1/{day_utc}/{scope_id}/{decision_id}/advisory_decision_state.v1.json`

Required identity and authority fields:
- `advisory_item_id`
- `authority_label`
- `decision_state`
- `actionability_state`
- `freshness_state`
- `visibility_state`
- `promotion_eligibility_state`
- `invalidation_rule_id`
- `explanation_mapping_version`
- `projection_version`

Required governing refs:
- `governing_stage_refs`
- `governing_transition_refs`
- `governing_certification_refs`
- `governing_release_refs` when release/baseline actionability is evaluated
- `governing_tax_refs` when tax-aware advisory binding is evaluated
- `governing_opportunity_refs` when opportunity-aware advisory binding is evaluated
- `evidence_refs`

Required governed binding state fields:
- `tax_binding_state` when tax-aware advisory binding is evaluated
- `opportunity_binding_state` when opportunity-aware advisory binding is evaluated

Required explanation fields:
- `primary_explanation`
- `primary_explanation.explanation_id`
- `primary_explanation.decision_state`
- `primary_explanation.actionability_state`
- `primary_explanation.freshness_state`
- `primary_explanation.invalidation_reason`
- `primary_explanation.short_message`
- `primary_explanation.detail_fields`
- `primary_explanation.evidence_refs`
- `primary_explanation.authority_label`
- `primary_explanation.action_class` only when it is one of the fixed governed enums

Historical and lineage law:
- the artifact family is append-only
- a superseding decision MUST preserve explicit lineage through `supersedes_ref`
- `superseded_by_ref` MAY be present only when it is directly provable at evaluation time
- `historical_visibility` MUST explicitly state whether the decision is current, historical-only, or suppressed

Authority ceiling:
- the artifact MUST NOT claim authority beyond the certified control-plane and release truth it cites
- the artifact MUST NOT claim tax meaning beyond the bound `tax_state_v1` artifact when tax-aware binding is present
- the artifact MUST NOT claim opportunity meaning beyond the bound `opportunity_state_v1` artifact when opportunity-aware binding is present
- the artifact MUST NOT become upstream truth for Bundle 6 trust-plane projections
