---
id: C2_REFINEMENT_PROVENANCE_V1
title: "C2 Refinement Provenance Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_refinement_plane
---

# refinement_provenance_v1

Every refinement decision must preserve:
- evidence basis refs
- threshold rule applied
- action selected
- before snapshot ref
- after snapshot ref where applicable
- preserved drill-down refs
- reversibility state

Historical reconstruction law:
- operators must be able to reconstruct what target changed, why it changed, what remained visible, and whether the change was reversible from the refinement artifact plus its governed refs
