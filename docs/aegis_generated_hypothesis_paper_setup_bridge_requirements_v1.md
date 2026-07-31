# Aegis Generated Hypothesis Paper Setup Bridge Requirements v1

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
