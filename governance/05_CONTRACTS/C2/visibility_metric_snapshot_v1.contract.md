---
id: C2_VISIBILITY_METRIC_SNAPSHOT_V1_CONTRACT
title: "C2 Visibility Metric Snapshot v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the canonical Batch 2 normalized metric extraction snapshot for a run/day.

# Canonical path
- `constellation_2/runtime/truth/reports/visibility_metric_snapshot_v1/<DAY>/visibility_metric_snapshot.v1.json`

# Metric governance
- metrics must be defined by `C2_VISIBILITY_METRIC_REGISTRY_V1`
- zero, unavailable, blocked downstream, and integrity-failed states must remain distinct
