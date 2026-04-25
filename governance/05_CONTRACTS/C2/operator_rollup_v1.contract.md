---
id: C2_OPERATOR_ROLLUP_CONTRACT_V1
title: "C2 Operator Rollup Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_operator_rollup
---

# C2 Operator Rollup Contract V1

## Purpose

The structured rollup engine is the deterministic Core 5 center.

It consumes only a sealed operator snapshot binding and emits only governed structured fields.

## Governed rollup vocabulary

### Overall operator state

- `HEALTHY`
- `ACTION_REQUIRED`
- `REVIEW_REQUIRED`
- `BLOCKED`
- `DEGRADED_INCOMPLETE`

### Highest severity

- `INFO`
- `WARNING`
- `CRITICAL`

### Required operator action

- `NONE`
- `REMEDIATE_BLOCKED_BOUNDARY`
- `FOLLOW_REQUIRED_ACTION`
- `PERFORM_OPERATOR_REVIEW`
- `REFRESH_STALE_TRUTH`
- `INVESTIGATE_PROVENANCE_GAP`
- `REBUILD_CROSS_CORE_SNAPSHOT`

### Review reason class

- `NONE`
- `FOREIGN_MANUAL_REVIEW_REQUIRED`
- `DRIFT_EXCEPTION_REVIEW_REQUIRED`
- `ACTION_AUTHORITY_REVIEW_REQUIRED`
- `SNAPSHOT_COHERENCE_REVIEW_REQUIRED`

### Blocked reason class

- `NONE`
- `BLOCKED_BY_BOUNDARY`
- `BLOCKED_BY_ACTION_AUTHORITY`
- `BLOCKED_BY_RECONCILIATION`
- `BLOCKED_BY_SNAPSHOT`

### Stale or degraded class

- `NONE`
- `STALE_TRUTH_WARNING`
- `STALE_TRUTH_CRITICAL`
- `DEGRADED_SUMMARY_INCOMPLETE_LOWER_CORE_CHAIN`
- `LOWER_CORE_PROVENANCE_INCOMPLETE`

### Last material change class

- `INITIAL_MATERIALIZATION`
- `NO_MATERIAL_CHANGE`
- `OPERATOR_STATE_CHANGED`
- `SEVERITY_CHANGED`
- `REQUIRED_ACTION_CHANGED`
- `BOUNDARY_STATUS_CHANGED`
- `ACTION_POSTURE_CHANGED`
- `SNAPSHOT_STATUS_CHANGED`

## Hard rules

- no freeform interpretation is allowed in the structured rollup layer
- blocked lower-core states must remain blocked
- review-required lower-core states must remain review-required
- stale or degraded lower-core states must remain explicit
- snapshot incoherence must remain explicit
- hidden heuristics are forbidden
