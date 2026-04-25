---
id: C2_DAY_READINESS_AUTOMATION_V1
title: "C2 Day Readiness Automation Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-16
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_admission
---

# C2 Day Readiness Automation Contract (V1)

## Purpose

This contract defines one narrow operator and automation projection for preparing a target day through canonical owners before the morning path is invoked.

It is subordinate to bootstrap.

It may prepare, refresh, rerun, classify, and report.

It is not a second control plane and it is not a substitute authority surface for bootstrap, `pre_open_bundle_v1`, `session_promotion_decision_v1`, or Session Authority.

## Canonical owner

- canonical writer: `ops/tools/run_tomorrow_paper_startup_prep_v1.py`
- canonical artifact path: `constellation_2/runtime/truth/reports/day_readiness_automation_v1/<DAY>/day_readiness_automation.v1.json`
- canonical schema: `governance/04_DATA/SCHEMAS/C2/REPORTS/day_readiness_automation.v1.schema.json`

## Runtime position

The intended runtime layering is:

`Day Authority -> Day Readiness Automation -> Bootstrap -> Pre-Open Bundle -> Promotion Gate -> Session Authority -> Day Activation -> Orchestrator -> Trading`

`day_authority_decision_v1` remains a validation artifact.

`day_readiness_automation_v1` remains an operator and automation projection.

`paper_session_bootstrap_v1`, `pre_open_bundle_v1`, and `session_promotion_decision_v1` remain canonical startup truth.

## Allowed behavior only

Day Readiness Automation may only:

- resolve the target day through repo-owned day resolution
- attempt safe refresh of known refreshable prerequisites through their canonical owners
- rerun canonical bootstrap and post-bootstrap readiness checks
- classify the resulting readiness state for operator use
- stop on real external or time-bound blockers

It may not:

- decide readiness independently of bootstrap
- override `pre_open_bundle_v1`, `session_promotion_decision_v1`, or Session Authority truth
- fabricate broker evidence, market data, or other runtime truth artifacts
- mutate control truth directly outside canonical writers
- run deeper trading stages manually
- add retries, polling loops, or hidden background recovery

## Readiness vocabulary

The operator projection must use the following readiness states:

- `READY_FOR_DAY`
- `WAITING_FOR_MARKET_DATA`
- `BLOCKED_EXTERNAL`
- `BLOCKED_REPO_FIXABLE`

The projection classification bucket must be one of:

- `refreshable`
- `verifiable_only`
- `external_time_bound`

## Minimum prerequisite classification map

Refreshable through canonical owners:

- `paper_capital_seed_v1` via `ops/tools/ensure_paper_capital_seed_v1.py`
- canonical kill switch state materialization via `ops/tools/run_global_kill_switch_v1.py`
- broker reconciliation via `ops/tools/run_broker_reconciliation_day_v2.py`
- handshake and pointer-head refresh via `ops/tools/run_ib_api_handshake_spine_v1.py` and `ops/tools/run_pointer_heads_materialize_v1.py`
- capital-authority allocation chain via `ops/tools/run_exposure_net_day_v1.py`, `ops/tools/run_reconciled_trade_state_v1.py`, `ops/tools/run_sleeve_edge_measurement_v1.py`, and `ops/tools/run_capital_authority_allocation_day_v1.py`
- startup authorization convergence via `ops/tools/run_paper_startup_authorization_convergence_v1.py`

Verifiable-only current-state outputs:

- `target_day_build_v1`
- `target_day_admission_v1`
- `submit_boundary_status_v1`

External or time-bound blockers:

- broker-event flow that depends on real governed broker runtime evidence
- target-day reference price and startup-materialization inputs that depend on real governed target-day market data

For the target-day default equity reference price:

- before `09:30 America/New_York`, absence of a governed same-day positive core-session price must project as `WAITING_FOR_MARKET_DATA`
- at or after `09:30 America/New_York`, a governed same-day positive `market_data_snapshot_v1` row with `ingested_utc` at or after the open boundary may satisfy the blocker
- if that same-day governed price still does not exist, the projection must remain fail-closed and classify the blocker as `external_time_bound`
- prior-day closes, zero values, negative values, and stale liquidity-gate fallbacks must not be treated as ready

## Canonical next-step rule

After each safe refresh attempt, the automation owner must rerun canonical bootstrap or the subordinate post-bootstrap readiness check and then recalculate the earliest failing prerequisite from canonical truth.

The report must surface:

- `target_day`
- `automation_run_at`
- `readiness_state`
- `bootstrap_status`
- `earliest_failing_prerequisite`
- `owner_tool`
- `artifact_path`
- `blocker_class`
- `reason_codes`
- `classification`
- `canonical_next_step`

## Bootstrap monopoly rule

Bootstrap remains the canonical operator-facing judge of startup readiness.

Pre-open and promotion remain canonical startup truth.

Day Readiness Automation may help operators prepare the day, but it must never replace the canonical bootstrap, pre-open, promotion, or Session Authority judgments.
