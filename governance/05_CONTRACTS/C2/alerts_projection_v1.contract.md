---
id: C2_ALERTS_PROJECTION_V1
title: "C2 Alerts Projection Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_alerts_projection
---

# C2 Alerts Projection Contract (V1)

## Purpose

This contract defines the derived operator alert mapping for the touched Constellation
operational path.

It translates the current system projection into a bounded actionable alert set.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_alerts_projection_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/alerts_projection_v1/<DAY>/alerts_projection.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json`

## Authority classification

- This surface is derived-only and non-authoritative.
- It must not create alternate blocker ordering.
- The first true blocker from current authoritative state remains primary.

## Alert model

Allowed severities:

- `INFO`
- `ACTION_REQUIRED`
- `DEGRADED`
- `CRITICAL_BLOCK`

Allowed root-cause families:

- `UPSTREAM_MISSING_INPUT`
- `POLICY_BLOCK`
- `SYSTEM_DEFECT`
- `DEPLOYMENT_BLOCK`

## Fail-closed rules

- Missing or invalid `current_system_projection_v1` input must fail closed.
- Malformed or identity-invalid current-system projection inputs must fail closed.
- Duplicate alert storms are forbidden; alerts must deduplicate by root cause.
- This surface must never claim readiness truth.
