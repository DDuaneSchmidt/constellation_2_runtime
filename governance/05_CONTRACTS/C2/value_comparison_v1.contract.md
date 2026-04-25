---
id: C2_VALUE_COMPARISON_V1
title: "C2 Value Comparison Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_value_plane
---

# value_comparison_v1

Legal comparison set:
- `blocked_vs_allowed`

Unsupported first-wave comparison types:
- `acted_vs_not_acted`
- `tax_aware_vs_naive`
- `current_window_vs_prior_window`
- `sleeve_vs_portfolio_contribution`

`blocked_vs_allowed` law:
- required basis:
  - governed blocked opportunity state
  - governed blocker basis
  - governed realized no-activity observation window
- observability window:
  - current target day only unless an explicit later governed snapshot is bound
- forbidden assumptions:
  - no inferred alternative path
  - no loss avoided amount estimate
  - no generalized “would have happened” claim
- allowed claim strength:
  - at most `bounded_association`

Failure law:
- unsupported comparison types MUST be rejected explicitly and MUST NOT silently degrade into broad storytelling
