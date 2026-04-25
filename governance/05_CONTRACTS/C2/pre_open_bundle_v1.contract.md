---
id: C2_PRE_OPEN_BUNDLE_V1
title: "C2 Pre-Open Bundle V1"
status: DRAFT
version: 1
created_utc: 2026-04-16
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_startup
---

# C2 Pre-Open Bundle V1

Canonical schema:
- `governance/04_DATA/SCHEMAS/C2/REPORTS/pre_open_bundle.v1.schema.json`

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/pre_open_bundle_v1/<DAY>/pre_open_bundle.v1.json`

Required meaning:
- `target_day`
- `active_day_observed`
- `active_day_alignment_status`
- `owner_tool`
- `materialization_state`
- `completion_state`
- `blocking_reason_codes[]`
- `prerequisite_checks[]`
- `producer_results[]`
- `market_calendar_status`
- `built_at_utc`
- producer identity
- `producer_results[]` must carry explicit producer classification fields:
  - `result_state`
  - `reason_codes[]`
  - `freshness_status`
  - `target_day_expected`
  - `target_day_observed`
  - `artifact_refs[]`

Materialization state:
- `COMPLETE`
- `INCOMPLETE`
- `BLOCKED`

Rules:
- `prerequisite_checks[]` is the single governed prerequisite summary that Session Authority may consume for day-bound startup prerequisites in this pass
- `pre_open_bundle_v1` is canonical startup prerequisite truth, but it is not itself a current-state promotion output
- `session_promotion_decision_v1` must consume this bundle before `active_session_v1/current.json` may advance
- canonical morning operator surfaces must stop before `run_day_open_attempt_v1.py` when this bundle is not `materialization_state=COMPLETE`
- prerequisite rows must remain machine-readable and audit-stable
- `materialization_state=COMPLETE` only when every required prerequisite check passes for the target day
- `completion_state=COMPLETE` only when every required prerequisite check passes
- producer failures must surface explicitly in `producer_results[]` and `blocking_reason_codes[]`
- `producer_results[].result_state` must distinguish at minimum:
  - producer unavailable / missing artifact
  - stale producer evidence
  - target-day mismatch
  - intentional fail-closed execution-boundary block
- a producer may still classify `UNAVAILABLE` when a same-day artifact exists but its written reason codes prove broker/session or prerequisite absence rather than an intentional control-path block
- `artifact_refs[]` should bind the exact authoritative producer outputs that were revalidated for the producer classification, without duplicate path rows
- stale `active_day_observed` must remain explicit, but active-day mismatch alone must not fabricate admission PASS or FAIL outside Session Authority
- this bundle is canonical startup prerequisite truth for Session Admission; advisory readiness or legacy startup surfaces must not override it
