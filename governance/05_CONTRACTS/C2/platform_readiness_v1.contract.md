---
id: C2_PLATFORM_READINESS_CONTRACT_V1
title: "C2 Platform Readiness Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Platform Readiness Contract v1

## Purpose

Define a governed platform-readiness authority for live-capital promotion of the Constellation platform itself.

Platform readiness is separate from:

- sleeve execution health
- system monitoring health
- sleeve live readiness

## Artifact surface

- `/home/node/constellation_runtime_data/truth/readiness_v1/constellation_platform_readiness_v1/<DAY_UTC>/constellation_platform_readiness.v1.json`

## Required policy binding

Writer logic MUST be governed by:

- `governance/02_REGISTRIES/C2_PLATFORM_READINESS_POLICY_V1.json`

Numeric encodings used for bug-linked metrics MUST follow policy-declared units (for example `events_per_day_x100`, `basis_points`) and platform artifacts must expose operator-safe display values.

## Required inputs

- runtime diagnostics scope health
- root-cause and repair outputs
- bug metrics artifact (`constellation_bug_metrics.v1.json`)

## Required outputs

- `platform_readiness_state`
- `platform_readiness_score`
- `platform_readiness_grade`
- `score_threshold_ready`
- `platform_promotion_candidate`
- `root_blockers`
- `derived_blockers`
- `aggregate_blocker_summary`
- `readiness_summary`
- `promotion_decision_basis`
- `top_blockers_ordered`
- `minimum_conditions_summary`
- `current_vs_required`
- `promotion_checklist`
- `smallest_clearance_set`
- `blocker_dependency_order`
- `score_contribution`
- `metric_views` (`raw_value`, `unit`, `display_value` for platform and bug-linked metrics)
- `policy_values`
- `evidence_paths`
- `bug_stability_summary`
- `calibration_support`

## Hard-blocker semantics

When any platform hard blocker is present:

- `platform_readiness_state` must be `BLOCKED`
- `platform_promotion_candidate` must be `false`

## Fail-closed semantics

If required artifacts or governed policy are missing/invalid, writer must fail closed.

## Governed schema

- `governance/04_DATA/SCHEMAS/C2/READINESS/platform_readiness.v1.schema.json`
