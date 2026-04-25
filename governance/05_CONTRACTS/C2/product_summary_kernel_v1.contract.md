---
id: C2_PRODUCT_SUMMARY_KERNEL_V1
title: "C2 Product Summary Kernel Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_product_summary_plane
---

# product_summary_kernel_v1

Core law:
- `constellation_2.common.product_summary_kernel_v1` is the only legal semantic source for product-level top-of-screen selection on the active C2 path.
- shell pages, read models, and bounded assist surfaces MUST NOT rank or suppress top items outside this kernel.

Allowed inputs:
- `advisory_decision_state_v1`
- `tax_state_v1`
- `opportunity_state_v1`
- `opportunity_review_snapshot_v1`
- `certified_operational_readiness_v1` through the governed Bundle 7 read path
- bounded scenario-significance fields already embedded in `opportunity_state_v1`

Forbidden inputs:
- page-local ranking logic
- UI-local actionability or priority logic
- AI-local ranking or suppression
- raw runtime inspection outside governed artifact paths

Canonical object law:
- one evaluation yields one canonical `product_summary_v1` object
- the same governed inputs MUST yield the same summary selection except for ratified volatile timestamps
- the object MUST remain deliberately small and limited to:
  - `top_actionable_items`
  - `top_blocked_items`
  - `top_review_deltas`
  - `critical_degraded_states`
  - `readiness_summary`
  - selection and provenance refs

Bounded assist law:
- AI condensation MUST consume `product_summary_v1` only
- AI condensation MUST NOT invent new decisions, rankings, or opportunities
- deterministic fallback output is required when bounded assist is unavailable
