---
id: C2_DAY_FAILURE_CAUSALITY_V1
title: "Day Failure Causality V1"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Day Failure Causality V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/day_failure_causality_v1/<DAY>/day_failure_causality.v1.json`

Rules:
- this artifact is the canonical first-failure chain for blocked or failed day startup outcomes
- canonical writer: `ops/tools/run_day_failure_causality_v1.py`
- canonical helper: `constellation_2/common/day_failure_causality_v1.py`
- it must cover canonical blocked, failed, open-missed, and open-failed day outcomes
- it must identify:
  - first failing artifact id
  - first failing artifact path
  - first failing field
  - first failing code
  - owning tool
  - owning contract
  - direct return code and stderr when the failure is a runtime or subprocess defect
  - downstream propagation chain through governed startup/admission artifacts
- it must derive from canonical runtime truth only
- when applicable it must include `day_open_trigger_v1` and `day_open_attempt_v1` in the auditable propagation chain
- for `environment = PAPER`, clock-window expiration alone is not a first-failure cause in this pass; causality must point to actual readiness, authority, trigger, attempt, or downstream action failure
- it must remain auditable and machine-readable; vague prose-only summaries are not sufficient
