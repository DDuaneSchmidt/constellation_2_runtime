---
id: C2_DAILY_SUMMARY_V1_CONTRACT
title: "C2 Daily Summary v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the canonical Batch 2 daily executive readiness summary using this fixed precedence:

1. `gate_stack_verdict_v1`
2. integrity failure truth
3. Batch 1 diagnostics
4. reconciled current-day visibility metrics
5. PRIOR_DAY drift observations

# Canonical path
- `constellation_2/runtime/truth/reports/daily_summary_v1/<DAY>/daily_summary.v1.json`

# Constraints
- factual only
- no advice
- no readiness semantic changes
