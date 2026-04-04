---
id: C2_ECONOMIC_FINALIZATION_STATUS_V1_CONTRACT
title: "C2 Economic Finalization Status v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the canonical gate for when positions, cash, marks, accounting, and exit reconciliation may be treated as economically finalized.

# Canonical path
- `constellation_2/runtime/truth/reports/economic_finalization_status_v1/<DAY>/economic_finalization_status.v1.json`

# Audit rule
Economic truth must remain blocked until the required execution truth and external/operator prerequisites are actually present and governed.
