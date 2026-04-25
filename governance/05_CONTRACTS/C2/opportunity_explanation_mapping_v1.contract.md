---
id: C2_OPPORTUNITY_EXPLANATION_MAPPING_V1
title: "C2 Opportunity Explanation Mapping Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_opportunity_explanation_mapping
---

# opportunity_explanation_mapping_v1

Explanation mapping law:
- explanation mapping is table-driven
- no unsupported inference is allowed
- explanation consumes only the canonical opportunity object plus its governing refs

Required preserved fields:
- `opportunity_type`
- `opportunity_state`
- `actionability_state`
- `review_priority`
- `blocker_states`
- `delta_state`
- `scenario_significance_state`
- `freshness_state`
- `visibility_state`
- `authority_label`
- governing refs

Fixed first-wave action classes:
- `none`
- `review_now`
- `monitor`
- `inspect_blocker`
- `inspect_history`
