---
id: C2_OPPORTUNITY_STATE_PRECEDENCE_V1
title: "C2 Opportunity State Precedence Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_opportunity_state_precedence
---

# opportunity_state_precedence_v1

Opportunity precedence is table-driven and deterministic.

Ordered precedence:

| Rule family | Dominates | Resulting opportunity state | Actionability | Review priority | Visibility |
| --- | --- | --- | --- | --- | --- |
| `SUPERSEDED_OPPORTUNITY` | all lower states | `historical_only` | `inspect_only` | `historical_only` | `historical_only` |
| `STALE_OPPORTUNITY` | lower visibility states | `monitor_only` | `inspect_only` | `review_soon` | `historical_only` or `current_downgraded` per kernel input |
| `DEGRADED_UPSTREAM_TRUTH` | all positive opportunities | `blocked` | `blocked` | `review_now` | `current_downgraded` |
| `READINESS_BLOCKED` | actionable or monitor-only opportunities | `blocked` | `blocked` | `review_now` | `current_downgraded` |
| `TAX_BLOCKED` | tax-dependent opportunities | `blocked` | `blocked` | `review_now` | `current_downgraded` |
| `ADVISORY_BLOCKED_IMPORTANT` | lower review states | `blocked` | `blocked` | `review_now` | `current_downgraded` |
| `BLOCKED_BUT_IMPORTANT` | monitor-only and lower | `blocked` | `inspect_only` | `review_now` | `current_downgraded` |
| `SCENARIO_REVIEW_NOW` | monitor-only | `actionable` | `inspect_only` | `review_now` | `current` |
| `ACTIONABLE_REVIEW_NOW` | monitor-only | `actionable` | `actionable` | `review_now` | `current` |
| `MONITOR_ONLY` | historical-only fallback only | `monitor_only` | `monitor_only` | `monitor_only` | `current` |
| `NO_VISIBLE_OPPORTUNITY` | fallback | `monitor_only` | `inspect_only` | `historical_only` | `suppressed` |

Delta influence law:
- `new`, `changed`, and `blocked_but_still_important` MAY elevate `review_priority` to `review_now`
- `resolved` MUST move to review snapshot history only and MUST NOT remain current

Multiple-opportunity ordering law:
- deterministic ordering is by `review_priority`, then blocker severity, then `opportunity_type`, then `opportunity_id`
