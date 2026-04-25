---
id: C2_VALUE_CLAIM_STRENGTH_V1
title: "C2 Value Claim Strength Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_value_plane
---

# value_claim_strength_v1

Allowed claim-strength ladder:
1. `not_yet_observable`
2. `insufficient_evidence`
3. `observed_fact`
4. `bounded_association`
5. `partial_attribution`
6. `direct_attribution`

Evidence law:
- `not_yet_observable` applies when the governed realized observation window is not yet available.
- `insufficient_evidence` applies when realized basis, sleeve linkage, or value linkage is incomplete.
- `observed_fact` applies when realized truth is observed without attribution.
- `bounded_association` applies only when a bounded governed basis supports association but not stronger attribution.
- `partial_attribution` applies only when explicit but incomplete governed linkage exists.
- `direct_attribution` applies only when explicit governed evidence directly binds prior governed state to realized value result.

Conservative precedence:
- weaker-claim states MUST dominate whenever evidence is incomplete
- direct attribution MUST remain rare and explicit
- unsupported edge, alpha, or broad benefit language is forbidden below `direct_attribution`
