---
id: C2_OPPORTUNITY_STATE_KERNEL_V1
title: "C2 Opportunity State Kernel Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_opportunity_plane
---

# opportunity_state_kernel_v1

Core law:
- `constellation_2.common.opportunity_state_kernel_v1` is the only legal semantic source for proactive opportunity discovery and review prioritization on the active C2 path.
- operator, advisory, and review surfaces MUST NOT recompute opportunity ranking, blocker handling, review priority, delta semantics, or scenario significance outside that kernel.
- the kernel MUST consume certified or otherwise explicitly governed upstream truth only and MUST fail closed on forbidden or unprovable inputs.

Allowed inputs:
- `advisory_decision_state_v1`
- `tax_state_v1`
- `control_plane_operator_status_v1`
- `transition_timeline_projection_v1`
- `certified_operational_readiness_v1` only through the governed Bundle 7 read path
- household portfolio compiler artifacts only when an explicit governed artifact path is provided:
  - `allocation_plan_v1`
  - `risk_envelope_v1`
  - `rebalance_candidates_v1`
  - `tax_adjudicated_rebalance_v1`
  - `portfolio_decision_record_v1`
- governed scenario input only when an explicit `stress_case_view_v1` artifact path is provided

Forbidden inputs:
- UI-local ranking logic
- advisory-local opportunity generation
- raw runtime file inspection outside certified paths
- AI-local opportunity inference
- freeform scenario interpretation

Canonical object law:
- one opportunity evaluation MUST yield one canonical opportunity object
- the same certified truth basis MUST yield the same opportunity object except for ratified volatile timestamps
- the object MUST carry:
  - `opportunity_type`
  - `opportunity_state`
  - `actionability_state`
  - `review_priority`
  - `freshness_state`
  - `visibility_state`
  - `blocker_states`
  - `delta_state`
  - `scenario_significance_state`
  - governing refs
  - deterministic explanation payload

Review model law:
- the kernel is the only legal source for current review snapshot and prior-snapshot delta semantics
- changed-since-last-review logic MUST be deterministic and snapshot-based
- the kernel MUST preserve historical reconstruction through explicit lineage

Scenario law:
- scenario significance is bounded and subordinate to the opportunity plane
- the kernel MUST NOT invent scenarios or scenario meaning beyond the governed scenario-significance contract
