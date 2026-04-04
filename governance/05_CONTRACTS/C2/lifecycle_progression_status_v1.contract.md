---
id: C2_LIFECYCLE_PROGRESSION_STATUS_V1_CONTRACT
title: "C2 Lifecycle Progression Status v1 Contract"
status: DRAFT
version: 1
created_utc: 2026-04-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Purpose
Provide the canonical staged lifecycle status for intent, authorization, submission, execution stream, and fill completion.

# Canonical path
- `constellation_2/runtime/truth/reports/lifecycle_progression_status_v1/<DAY>/lifecycle_progression_status.v1.json`

# Audit rule
Later lifecycle stages must never be presented as complete when earlier stages are blocked, missing, or sparse.
