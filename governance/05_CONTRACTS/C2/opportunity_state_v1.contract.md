---
id: C2_OPPORTUNITY_STATE_V1
title: "C2 Opportunity State Artifact Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_opportunity_state_artifact
---

# opportunity_state_v1

This contract governs the durable Bundle 11 opportunity artifact family.

Artifact class:
- `outcome_record`

Authoritative writer:
- `constellation_2.common.opportunity_state_kernel_v1`

Authoritative path pattern:
- `{canonical_truth_root}/reports/opportunity_state_v1/{day_utc}/{scope_id}/{opportunity_state_id}/opportunity_state.v1.json`

Required identity and authority fields:
- `opportunity_id`
- `opportunity_state_id`
- `authority_label`
- `opportunity_type`
- `opportunity_state`
- `actionability_state`
- `review_priority`
- `freshness_state`
- `visibility_state`
- `blocker_states`
- `delta_state`
- `scenario_significance_state`
- `advisory_binding_state`
- `kernel_version`
- `explanation_mapping_version`

Required governing refs:
- `governing_advisory_refs`
- `governing_tax_refs`
- `governing_trust_refs`
- `governing_readiness_refs`
- `governing_scenario_refs`
- `governing_portfolio_refs`
- `evidence_refs`

Required review provenance fields:
- `review_snapshot_ref` when current review snapshot was emitted in the same evaluation pass
- `prior_review_snapshot_ref` when a prior governed snapshot exists
- `supersedes_ref` when the opportunity supersedes a prior opportunity artifact
- `historical_visibility`

Authority ceiling:
- the artifact MUST NOT claim strategy or portfolio meaning beyond the cited advisory, tax, trust, readiness, portfolio, and scenario inputs
- the artifact MUST NOT become upstream truth for control-plane, trust-plane, tax, or advisory kernels beyond the explicit opportunity-aware binding path
