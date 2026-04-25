---
id: C2_POLICY_EVOLUTION_STATE_KERNEL_V1
title: "C2 Policy Evolution State Kernel Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_policy_evolution_plane
---

# policy_evolution_state_kernel_v1

`policy_evolution_state_kernel_v1` is the only legal semantic source for temporal policy changes in refinement and product emphasis rules.

Allowed inputs:
- `value_state_v1`
- `refinement_state_v1`
- `product_summary_v1`
- `product_snapshot_v1`
- governed review snapshot history where already bound through upstream artifacts
- prior `policy_evolution_state_v1` rows for lineage, expiry, and rollback only

Forbidden inputs:
- route-local adaptation
- shell-local temporal tuning
- UI-local prominence shifts
- AI-local adaptation choices
- raw-runtime inspection outside governed artifact/history paths

Kernel laws:
- one canonical policy-evolution object family: `policy_evolution_state_v1`
- one deterministic evidence-window threshold model
- one deterministic propose/preserve/withhold/expire/rollback action model
- one provenance path using governed historical refs only
- one before/after policy comparison path
- no direct mutation of product or refinement behavior; downstream consumers must read explicit policy outputs only

Required outputs per evolution object:
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
- `before_policy_ref` / `after_policy_ref` where applicable
- `preserved_visibility_flags`

Fail-closed law:
- if required governed history is missing, too recent, unstable, or forbidden, policy evolution MUST be withheld or fail closed explicitly
