---
id: C2_PRODUCT_SUMMARY_V1
title: "C2 Product Summary Artifact Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_product_summary_plane
---

# product_summary_v1

Artifact law:
- `product_summary_v1` is the durable canonical product summary artifact family for Bundle 12.
- it is a governed summary projection over already-certified upstream artifacts.

Required fields:
- `summary_id`
- `authority_label`
- `top_actionable_items`
- `top_blocked_items`
- `top_review_deltas`
- `critical_degraded_states`
- `readiness_summary`
- governing advisory, tax, opportunity, and readiness refs
- `selection_reason_ids`
- lineage refs
- AI-assist eligibility metadata
- kernel and selection-model version metadata

Scope law:
- the artifact MUST stay small
- it MUST NOT become a second semantic dumping ground for lower-level domain state
- drill-down must occur through governed refs to lower-level artifacts
