---
id: C2_LIFECYCLE_STATE_AUTHORITY_V1
title: "Lifecycle State Authority V1"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Lifecycle State Authority V1

This artifact declares the governed lifecycle state used to assemble action-specific gate verdicts.

V1 is intentionally narrow:
- `PRE_TRADE`
- `POST_TRADE_ECONOMIC`

State selection is driven only by real on-disk lifecycle/economic truth, not by inferred intent.
