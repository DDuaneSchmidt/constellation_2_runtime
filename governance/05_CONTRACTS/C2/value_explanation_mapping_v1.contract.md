---
id: C2_VALUE_EXPLANATION_MAPPING_V1
title: "C2 Value Explanation Mapping Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_value_plane
---

# value_explanation_mapping_v1

Explanation law:
- explanation mapping MUST be table-driven
- unsupported performance, edge, or benefit language is forbidden
- the explanation layer MUST preserve:
  - `realized_state`
  - `observability_state`
  - `effectiveness_state`
  - `attribution_state`
  - `claim_strength`
  - `sleeve_refs`
  - governing refs
  - comparison refs where applicable

Output law:
- explanations MUST distinguish observed fact from attribution and comparison
- explanations MUST distinguish execution-scope sleeve linkage from stronger sleeve attribution
- explanations MUST refuse unsupported mappings rather than inventing narrative meaning
