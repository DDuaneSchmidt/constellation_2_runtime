---
id: C2_POLICY_EVOLUTION_STATE_V1
title: "C2 Policy Evolution State Artifact Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_policy_evolution_plane
---

# policy_evolution_state_v1

`policy_evolution_state_v1` is the durable canonical policy-evolution artifact family for Bundle 15.

Required fields:
- `evolution_id`
- `authority_label`
- `evolution_target`
- `target_policy_scope`
- `proposed_policy_change`
- `evidence_window_refs`
- `evidence_basis_refs`
- `evolution_strength`
- `threshold_result`
- `trust_override_state`
- `reversibility_state`
- `expiry_state`
- `before_policy_ref`
- `after_policy_ref` where applicable
- `preserved_visibility_flags`
- `lineage/history refs`
- `kernel / threshold / action model version metadata`

First-wave scope:
- command-home prominence policy
- refinement-target policy over top-level, compressed, secondary, and drill-down visibility
- operator-facing policy proof rows

Artifact law:
- `policy_evolution_state_v1` may propose or preserve policy, but may not directly mutate upstream truth and may not weaken blocked, degraded, claim-strength, or insufficient-evidence protections
