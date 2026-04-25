---
id: C2_BUG_METRICS_CONTRACT_V1
title: "C2 Bug Metrics Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Bug Metrics Contract v1

## Purpose

Define a governed, artifact-backed bug metrics surface derived from runtime diagnostics evidence.

## Artifact surface

- `/home/node/constellation_runtime_data/truth/readiness_v1/constellation_bug_metrics_v1/<DAY_UTC>/constellation_bug_metrics.v1.json`

This artifact is platform-level (global truth) and must not be derived from UI state.

## Required evidence inputs

Bug metrics MUST use artifact-backed diagnostics evidence only:

- `/home/node/constellation_runtime_data/truth/system_snapshot/constellation_runtime_state.v1.json`
- `/home/node/constellation_runtime_data/truth/system_snapshot/constellation_root_cause_report.v1.json`
- `/home/node/constellation_runtime_data/truth/system_snapshot/constellation_repair_plan.v1.json`
- `/home/node/constellation_runtime_data/truth/monitoring_v1/lifecycle_monitor/<DAY>/lifecycle_monitor_report.v1.json`
- `/home/node/constellation_runtime_data/truth/monitoring_v1/paper_readiness/<DAY>/paper_readiness_report.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/orchestrator_run_verdict_v2/<DAY>/*/orchestrator_run_verdict.v2.json`

## Required outputs

- `new_bug_events_today`
- `bug_velocity_7d_avg`
- `bug_velocity_14d_avg` (or `UNKNOWN` path via `unknown_fields` when history is insufficient)
- `recurring_bug_events`
- `recurrence_rate`
- `mttr_hours` and `median_ttr_hours` only when provable from artifact timestamps
- `bug_velocity_trend` and `bug_half_life_estimate_days` only when provable
- `diagnostic_stability_rate`
- `evidence_paths`
- `unknown_fields`

Numeric rates are deterministic integer-scaled fields:

- `bug_velocity_*` are events/day in `x100` units
- `recurrence_rate` and `diagnostic_stability_rate` are basis points (`0..10000`)
- artifacts must include `metric_views.<metric> = {raw_value, unit, display_value}` for operator-safe consumption
- artifacts must include `calculation_summary` with deterministic formula/basis and window days used

## Deterministic recurrence key

Recurrence MUST be computed from structured keys:

- `root_cause_class`
- `affected_surface`
- `reason_code`

Unstructured text matching is disallowed where structured fields exist.

## Fail-closed unknown handling

If required history or timestamps are unavailable, the writer MUST:

- emit `null` for unsupported numeric metrics
- record each unavailable metric in `unknown_fields`
- avoid fabricated estimates

## Governed schema

- `governance/04_DATA/SCHEMAS/C2/READINESS/bug_metrics.v1.schema.json`
