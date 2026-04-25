---
id: C2_DAY_OPEN_ATTEMPT_V1
title: "Day Open Attempt V1"
status: DRAFT
version: 1
created_utc: 2026-04-14
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Day Open Attempt V1

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/day_open_attempt_v1/<DAY>/day_open_attempt.v1.json`

Rules:
- this artifact is the single canonical record of whether the governed day-open action was attempted
- canonical writer: `ops/tools/run_day_open_attempt_v1.py`
- canonical helper: `constellation_2/common/day_open_attempt_v1.py`
- policy helper: `constellation_2/common/day_open_policy_v1.py`
- runtime submit-stage wrapper: `ops/tools/run_c2_multi_sleeve_orchestrator_v1.py`
- canonical sequencing owner for sleeve-edge publication before allocation: `ops/tools/run_c2_paper_day_orchestrator_v2.py`
- it must consume exactly one valid `day_open_trigger_v1` emission at most once
- in paper mode it must allow repeated same-day governed attempts whenever current readiness and authority remain granted
- in paper mode it must not suppress a new attempt solely because:
  - a prior same-day paper attempt succeeded
  - a prior same-day paper attempt failed
  - the clock is outside the prior BOD window
- in non-paper environments it must preserve the existing strict same-day and open-window constraints
- it must record:
  - the trigger ref and dedupe key it consumed
  - the consumed `trigger_kind`
  - the actor that consumed it
  - whether the open command executed
  - whether the submit stage was entered
  - the canonical sequencing owner in `submit_stage_owner`
  - direct evidence refs to `sleeve_rollup_v1`, `orchestrator_run_verdict_v2`, and `run_pointer_v1` when present
  - retained `attempt_history` for prior same-day attempts so repeated paper attempts remain auditable
  - a machine-readable open-policy snapshot
- it must classify the outcome as exactly one of:
  - `NOT_EXECUTED`
  - `OPEN_ATTEMPTED`
  - `OPEN_SUCCEEDED`
  - `OPEN_FAILED`
  - `OPEN_MISSED`
- it must not fabricate success when downstream orchestrator evidence is absent
- it must not bypass `run_c2_paper_day_orchestrator_v2.py` to reach allocation directly
- it must fail closed and record `OPEN_MISSED` explicitly when a strict non-paper open window expires after the day became open-authorized but no successful open occurred
