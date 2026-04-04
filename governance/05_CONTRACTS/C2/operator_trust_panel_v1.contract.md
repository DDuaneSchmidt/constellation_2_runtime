---
id: C2_OPERATOR_TRUST_PANEL_V1_CONTRACT
title: "C2 Operator Trust Panel v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the governed trust/evidence state for a Batch 3 view or query response.

# Canonical path
- `constellation_2/runtime/truth/reports/operator_trust_panel_v1/<DAY>/<ID>/operator_trust_panel.v1.json`

# Exactness classes
- `EXACT`
- `BOUNDED_PARTIAL`
- `UNAVAILABLE`
- `INTEGRITY_CONSTRAINED`

# Rules
- trust state actively governs presentation
- suppression and downgrade actions must be explicit
