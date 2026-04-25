---
id: C2_CRITICAL_VALIDATOR_COVERAGE_V1
title: "C2 Critical Validator Coverage Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_critical_validator_coverage
---

# C2 Critical Validator Coverage Contract v1

## Purpose

Define the minimum independent validator coverage required for Bundle 7 release safety.

## Required independent validator coverage

Bundle 7 release gating MUST have independent validation for:

- configuration activation family
- control-plane boundary
- day activation family
- global context family
- session authority family
- execution build family
- certified trust-plane operator/timeline surfaces used for operator go/no-go
- runtime-state and readiness surfaces still used operationally for diagnostics or release posture

## Critical operational surfaces

The minimum critical operational surfaces in scope are:

- `configuration_state_v1/current.json` and its configuration activation family
- `control_plane_operator_status_v1`
- `transition_timeline_projection_v1`
- `deployment_state_machine_v1`
- `constellation_runtime_state.v1.json`
- `constellation_root_cause_report.v1.json`
- `constellation_repair_plan.v1.json`
- `constellation_bug_metrics_v1`
- `constellation_platform_readiness_v1`

## Validator rules

Every Bundle 7 validator MUST:

- be independent of the writer it validates
- read canonical roots only
- fail closed
- emit deterministic JSON
- exit `0` only on success
- never mutate truth
- report forbidden-root hits explicitly
- report governing refs explicitly
