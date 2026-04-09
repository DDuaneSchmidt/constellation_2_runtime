---
id: C2_PERFORMANCE_PROJECTION_V1
title: "C2 Performance Projection Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_performance_projection
---

# C2 Performance Projection Contract (V1)

## Purpose

This contract defines the informational stage-timing view for the touched Constellation
operational path.

It derives timing and degradation information from the execution journal only.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_performance_projection_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/performance_projection_v1/<DAY>/performance_projection.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/performance_projection.v1.schema.json`

## Authority classification

- This surface is informational-only and non-authoritative.
- It must not influence readiness, blocker precedence, or control-plane truth.

## Required inputs

- `execution_journal_v1`

## Fail-closed rules

- Missing execution journal input must fail closed.
- Missing or invalid journal identity fields must fail closed.
- Invalid or contradictory stage-duration event shapes must fail closed.
- Performance degradation by itself must never become a readiness blocker through this surface.
