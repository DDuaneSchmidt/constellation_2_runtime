# Aegis Generated Hypothesis Paper Setup Bridge Spec v1

## Scope

This package defines the governed bridge from an approved generated hypothesis to paper candidate-construction eligibility. It is research-only and does not create raw signals, candidate contracts, paper observations, outcomes, trades, broker actions, or allocations.

## Authoritative Inputs

- `aegis_hypothesis_proposal_promotion_v1`
- `aegis_hypothesis_evidence_packet_v1`
- `aegis_hypothesis_shadow_trial_v1`
- `aegis_hypothesis_promotion_packet_v1`
- `aegis_paper_promotion_approval_queue_v1`
- `aegis_paper_sleeve_blueprint_v1`
- `aegis_paper_readiness_certification_v1`
- `aegis_approved_hypothesis_paper_tracking_setup_v1`

## Required Behavior

The bridge emits `PAPER_SETUP_BRIDGE_READY` only when an approval event is present, sleeve and policy mappings exist, paper readiness is certified, paper tracking setup is ready, and candidate construction eligibility is true. Missing approval emits `APPROVAL_EVENT_MISSING`; missing risk or exit mapping emits `POLICY_MAPPING_MISSING`; failed readiness emits `PAPER_READINESS_FAILED`.

## Oil Shock

For Oil Shock, the bridge must expose `generated_sleeve_id`, `sleeve_id`, `risk_policy_id`, `exit_policy_id`, blueprint status, readiness status, tracking setup status, candidate-construction eligibility, missing fields, reason codes, source paths, and hashes. If blocked, it lists exact missing fields and remains non-actionable for candidate construction.

## Safety

The bridge is an eligibility artifact only. Candidate contracts, entry reference price certification, paper lifecycle, outcome registry, and validation remain authoritative downstream gates. Safety gates are unchanged.


## Governance Bridge Integration

`aegis_generated_hypothesis_governance_bridge_v1` is the upstream authority for generated research sleeve and policy metadata. When it reports `GOVERNANCE_BRIDGE_READY`, the paper setup bridge can use its deterministic sleeve, risk policy, exit policy, candidate construction policy, validation plan, blueprint, readiness, and tracking setup IDs. When it is blocked, downstream candidate construction remains fail-closed and must not create raw signals or candidates.

## Approval Lineage Contract

Paper setup eligibility inherits approval truth from `aegis_generated_hypothesis_governance_bridge_v1`, which in turn consumes `aegis_generated_hypothesis_approval_event_lineage_v1`. A paper setup bridge cannot treat approved queue state as an approval event. If lineage reports missing, stale, wrong-day, schema-mismatched, or unhashed approval evidence, candidate construction remains ineligible and no paper setup status may be promoted to ready.

## Approval Lineage Consumption

The paper setup bridge consumes approval truth through `aegis_generated_hypothesis_governance_bridge_v1`. When governance is ready from a verified approval lineage event, the bridge may use governed generated IDs and report `PAPER_SETUP_BRIDGE_READY`; it still creates no raw signals, candidates, observations, trades, allocations, or broker actions.

## Historical Approval Lineage

Paper setup bridge readiness can inherit a governance bridge that was made ready from historical approval lineage. The historical event remains the approval authority; the paper setup bridge must preserve downstream source references and must not rewrite it as a new target-day approval.

