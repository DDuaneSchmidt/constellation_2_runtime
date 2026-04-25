---
id: C2_VALUE_SLEEVE_LINKAGE_V1
title: "C2 Value Sleeve Linkage Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_value_plane
---

# value_sleeve_linkage_v1

Sleeve linkage law:
- value claims MAY reference sleeves only through governed sleeve identity inputs already proven in repo
- execution scope alone is not equivalent to a canonical economic sleeve identity

Allowed first-wave linkage states:
- `direct_governed_sleeve`
- `execution_scope_only`
- `multi_sleeve_overlap`
- `insufficient_sleeve_attribution`

Rules:
- `direct_governed_sleeve` requires explicit governed economic sleeve identity basis
- `execution_scope_only` applies when only the governed execution scope sleeve is proven
- `multi_sleeve_overlap` applies when multiple sleeves are implicated and isolation is not proven
- `insufficient_sleeve_attribution` applies when sleeve linkage is missing or ambiguous

Conservative law:
- sleeve claims MUST weaken when only `execution_scope_only` is available
- direct sleeve attribution MUST NOT be claimed from `PRIMARY` alone without engine lineage
- ambiguous or overlapping sleeve bases MUST not be collapsed into one stronger sleeve claim
