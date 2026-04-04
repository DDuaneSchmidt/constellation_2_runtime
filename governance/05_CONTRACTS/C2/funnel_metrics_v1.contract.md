---
id: C2_FUNNEL_METRICS_V1_CONTRACT
title: "C2 Funnel Metrics v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the canonical Batch 2 rejection-funnel visibility artifact derived only from:

- `activity_flow_diagnostics_v1`
- `visibility_metric_snapshot_v1`
- `visibility_decision_ledger_v1`

# Canonical path
- `constellation_2/runtime/truth/reports/funnel_metrics_v1/<DAY>/funnel_metrics.v1.json`

# Required properties
- run-scoped stage metrics
- deterministic dominant-drop classification
- explicit integrity status
- replayable evidence refs

# Non-goals
- no trade decisions
- no retries
- no readiness overrides
