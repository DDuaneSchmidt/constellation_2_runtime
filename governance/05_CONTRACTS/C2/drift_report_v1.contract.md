---
id: C2_DRIFT_REPORT_V1_CONTRACT
title: "C2 Drift Report v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the canonical Batch 2 PRIOR_DAY drift artifact derived only from:

- `runtime_regression_analytics_v1`
- `visibility_metric_snapshot_v1`
- `visibility_decision_ledger_v1`

# Canonical path
- `constellation_2/runtime/truth/reports/drift_report_v1/<DAY>/drift_report.v1.json`

# Comparison governance
- comparison mode is fixed to `PRIOR_DAY`
- illegitimate comparisons must be surfaced explicitly
- no ad hoc windows
