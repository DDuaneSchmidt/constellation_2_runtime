---
id: C2_VALUE_STATE_V1
title: "C2 Value State Artifact Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_value_plane
---

# value_state_v1

Artifact law:
- `value_state_v1` is the durable canonical value-proof artifact family for Bundle 13.
- one artifact corresponds to one governed value basis and MUST declare its claim strength explicitly.

Required semantic fields:
- `realized_state`
- `observability_state`
- `effectiveness_state`
- `attribution_state`
- `claim_strength`
- `basis_state`
- `primary_rule_id`
- `primary_explanation`
- `sleeve_refs`

Required provenance fields:
- governing advisory refs where present through governed upstream artifacts
- governing tax refs where present
- governing opportunity refs
- governing realized execution refs
- governing product summary and snapshot refs where relevant
- comparison refs where applicable
- lineage refs for supersession/history

Conservative law:
- the artifact MUST NOT claim stronger evidence than the claim-strength ladder allows
- missing or incomplete realized basis MUST be represented explicitly, not inferred away
- sleeve linkage ambiguity MUST be represented explicitly, not collapsed into a stronger sleeve claim
