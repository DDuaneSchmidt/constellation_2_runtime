---
id: C2_POLICY_EVOLUTION_PROVENANCE_V1
title: "C2 Policy Evolution Provenance Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_policy_evolution_plane
---

# policy_evolution_provenance_v1

Every policy-evolution decision must preserve:
- evidence window refs
- evidence basis refs
- threshold result
- trust override state
- before policy ref
- after policy ref where applicable
- preserved visibility flags
- reversibility state
- expiry state

Historical reconstruction law:
- operators must be able to reconstruct what policy target changed, what evidence windows supported it, what blocked or weakened it, what policy existed before and after, and when it expires or must be re-reviewed
