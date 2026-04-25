---
id: C2_REFINEMENT_STATE_KERNEL_V1
title: "C2 Refinement State Kernel Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_refinement_plane
---

# refinement_state_kernel_v1

`refinement_state_kernel_v1` is the only legal semantic source for simplification, demotion, compression, and prominence decisions on the active C2 product path.

Allowed inputs:
- `product_summary_v1`
- `product_snapshot_v1`
- `value_state_v1`
- `opportunity_state_v1`
- governed tax/advisory refs only when already attached through governed summary or value artifacts

Forbidden inputs:
- page-local cleanup logic
- route-local prominence tuning
- AI-local simplification or suppression
- raw-runtime inspection outside governed artifact paths
- taste-based demotion without evidence basis

Kernel laws:
- one canonical refinement object family: `refinement_state_v1`
- one deterministic refinement-threshold model
- one deterministic action model
- one provenance path using governed before/after evidence only
- one reversible-or-withheld decision path
- no shell-local or read-model-local refinement semantics

Required outputs per refinement object:
- `refinement_target`
- `target_scope`
- `refinement_action`
- `refinement_strength`
- `refinement_reason_ids`
- `reversibility_state`
- `visibility_effect`
- `before_snapshot_ref` / `after_snapshot_ref` where applicable
- `preserved_drilldown_refs`

Fail-closed law:
- if required governed summary or value evidence is missing, refinement MUST be withheld or fail closed explicitly and MUST NOT guess a convenience-driven visibility change
