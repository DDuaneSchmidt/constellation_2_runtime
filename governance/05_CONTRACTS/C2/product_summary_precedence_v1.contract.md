---
id: C2_PRODUCT_SUMMARY_PRECEDENCE_V1
title: "C2 Product Summary Precedence Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_product_summary_plane
---

# product_summary_precedence_v1

Selection order:
1. `critical_degraded_state`
2. `blocked_but_important`
3. `review_delta_elevated`
4. `actionable_now`
5. `review_soon`
6. `monitor_only`
7. `historical_or_suppressed`

Rules:
- critical degraded states MAY outrank ordinary opportunities
- blocked-but-important items MUST outrank monitor-only items
- stale, superseded, historical-only, or suppressed items MUST NOT remain top-of-screen
- review deltas MAY outrank stable background items when their governing opportunity state remains current
- scenario-significance MAY elevate an already-governed opportunity but MUST NOT create a new ranking family

The implementation MUST be table-driven and deterministic.
