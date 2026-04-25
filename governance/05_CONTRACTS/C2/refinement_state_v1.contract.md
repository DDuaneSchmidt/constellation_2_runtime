---
id: C2_REFINEMENT_STATE_V1
title: "C2 Refinement State Artifact Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_refinement_plane
---

# refinement_state_v1

`refinement_state_v1` is the durable canonical refinement artifact family for Bundle 14.

Required fields:
- `refinement_id`
- `authority_label`
- `refinement_target`
- `target_scope`
- `evidence_basis_refs`
- `refinement_action`
- `refinement_strength`
- `refinement_reason_ids`
- `review_window`
- `reversibility_state`
- `visibility_effect`
- `before_snapshot_ref`
- `after_snapshot_ref` when applicable
- `preserved_drilldown_refs`
- `lineage/history refs`
- `kernel / threshold / action model version metadata`

First-wave scope:
- command-home refinement targets
- top-level vs secondary vs drill-down visibility decisions
- refinement proof rows for operator inspection

Artifact law:
- `refinement_state_v1` may preserve or reduce prominence, but it may not weaken upstream blocked, degraded, claim-strength, or insufficient-evidence distinctions
- before/after reconstruction MUST remain possible from artifact payload plus its governed refs
