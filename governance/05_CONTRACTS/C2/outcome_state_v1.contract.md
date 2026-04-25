---
id: C2_OUTCOME_STATE_V1
title: "C2 Outcome State Artifact Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_outcome_plane
---

# outcome_state_v1

Artifact law:
- `outcome_state_v1` is the durable canonical outcome-proof artifact family for Bundle 13.
- one artifact corresponds to one governed outcome basis and MUST declare its claim strength explicitly.

Required semantic fields:
- `realized_state`
- `observability_state`
- `effectiveness_state`
- `attribution_state`
- `claim_strength`
- `basis_state`
- `primary_rule_id`
- `primary_explanation`

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

