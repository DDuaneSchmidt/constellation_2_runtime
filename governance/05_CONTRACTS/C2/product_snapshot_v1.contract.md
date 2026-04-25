---
id: C2_PRODUCT_SNAPSHOT_V1
title: "C2 Product Snapshot Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_product_summary_plane
---

# product_snapshot_v1

Snapshot law:
- `product_snapshot_v1` captures what the user saw from the product summary plane.
- the snapshot MUST preserve:
  - current `product_summary_v1` ref
  - prior snapshot ref where available
  - selected item refs
  - selected blocked and degraded refs
  - bounded AI condensation outputs and provenance

Reconstruction law:
- historical reconstruction MUST be possible from durable refs and version metadata alone
- product surfaces MUST NOT infer prior top-level presentation from raw runtime artifacts when snapshot refs exist
