---
id: C2_OPPORTUNITY_SCENARIO_SIGNIFICANCE_V1
title: "C2 Opportunity Scenario Significance Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_opportunity_scenario_significance
---

# opportunity_scenario_significance_v1

Allowed scenario input:
- `stress_case_view_v1` only when a governed artifact path is explicitly provided

Forbidden behavior:
- inventing new scenarios
- freeform scenario meaning outside the opportunity kernel
- using AI-generated scenario comparison logic

Deterministic first-wave significance categories:
- `not_evaluated`
- `basis_unavailable`
- `monitor_only`
- `review_now`

First-wave binding law:
- no scenario artifact -> `not_evaluated`
- scenario artifact with stub or unsupported basis -> `basis_unavailable`
- one governed scenario basis without material comparison breadth -> `monitor_only`
- multiple governed scenario ids with non-stub outcomes -> `review_now`

Scenario significance MAY influence opportunity review priority but MUST remain subordinate to blocker precedence.
