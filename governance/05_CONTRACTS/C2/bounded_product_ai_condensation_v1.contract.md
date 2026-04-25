---
id: C2_BOUNDED_PRODUCT_AI_CONDENSATION_V1
title: "C2 Bounded Product AI Condensation Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_product_summary_plane
---

# bounded_product_ai_condensation_v1

Allowed AI inputs:
- canonical `product_summary_v1` only
- explanation fields and refs already embedded in the summary object

Allowed outputs:
- `daily_review_brief`
- `weekly_review_brief`
- `blocked_explanation_brief`
- `domain_condensed_summary`
- `scenario_comparison_brief`

Forbidden outputs:
- new decisions
- new rankings
- new opportunities
- tax calculations
- suppression of degraded or blocked states
- direct interpretation of raw upstream artifacts

Fallback law:
- when AI assist is unavailable, the system MUST emit a deterministic fallback condensation
- fallback output MUST preserve blocked and degraded states explicitly
- provenance MUST include source summary and source explanation refs
