---
id: C2_STARTUP_MATERIALIZATION_V1
title: "C2 Startup Materialization Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_fact_plane
---

# C2 Startup Materialization Contract (V1)

## Purpose

This contract defines the canonical source-authoritative fact surface that records whether the required startup materialization for the paper-session scope completed successfully and produced the required governed outputs.

This surface is domain fact only.

It is not session authority, readiness authority, admission authority, or submit authority.
It is also not the canonical startup prerequisite closure surface for Session Authority; that role is owned by `pre_open_bundle_v1`.
It is not a startup current-state promotion surface; current-state advancement is owned by `session_authority_v1` through `session_promotion_decision_v1`.
Producer-level single-shot read-back verification for handshake, pointer-head, and kill-switch startup prerequisites belongs to the pre-open producer layer and must not be inferred from this fact surface.

## Truth owner

- Truth owner: Constellation governance
- Canonical writers:
  - `ops/tools/run_paper_session_bootstrap_v1.py`
  - `ops/tools/run_startup_materialization_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/startup_materialization_v1/<DAY>/startup_materialization.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json`

## Required inputs

- bootstrap-session prerequisites:
  - paper capital seed
  - operator statement
  - same-day cash ledger snapshot
  - same-day accounting NAV
  - same-day capital-risk envelope
  - same-day capital-authority allocation
  - same-day `paper_startup_authorization_convergence_v1`
  - same-day authorization gate artifact
- legacy phase-C startup materialization prerequisites:
  - same-day `startup_materialization_input_convergence_v1` artifact under `constellation_2/runtime/truth/reports/startup_materialization_input_convergence_v1/<DAY>/`
  - same-day paper intents under `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`
  - same-day phase-C preflight root under `truth_sleeves/<sleeve_id>/<mode>/phaseC_preflight_v1/<DAY>/`
  - same-day latest active attempt pointer produced by phase-C identity materialization
  - supported same-day identity output set produced by phase-C identity materialization

## Required meaning

The artifact must identify:

- `day_utc`
- `session_id`
- closed startup materialization `status`
- `required_inputs_checked[]`
- `materialized_outputs[]`
- stable `blocking_codes[]`
- producer identity
- `produced_at_utc`
- explicit `freshness_verdict`
- explicit `linkage_verdict`
- explicit `authority_scope = NON_AUTHORITY_FACT`
- optional `materialization_scope` when the writer is distinguishing bootstrap-session prerequisites from phase-C identity materialization
- optional `phase_summary` when the writer is making same-day phase separation explicit for operator-facing bootstrap flows

## Allowed status values

- `SUCCESS`
- `FAIL`
- `MISSING_DEPENDENCY`
- `MALFORMED`
- `STALE`
- `UNKNOWN`

## Fail-closed rules

- Missing same-day intents must not yield `SUCCESS`.
- A missing or blocked `startup_materialization_input_convergence_v1` artifact must not yield `SUCCESS`.
- Missing or malformed latest active attempt evidence must not yield `SUCCESS`.
- A zero materializer process exit without a supported same-day identity output set must not yield `SUCCESS`.
- Missing, stale, malformed, unknown, or contradictory prerequisite evidence must deny `SUCCESS`.
- Missing same-day bootstrap prerequisites or missing same-day authorization convergence must not yield `SUCCESS`.
- This surface must not evaluate kill switch state directly; it exists to prove materialization before evaluation.
- The surface must never publish `READY`, `ADMITTED`, or `AUTHORIZED`.

## Freshness and linkage

- Freshness is current only when the artifact binds the requested `day_utc` and its required evidence resolves within the same day-scoped truth family.
- Linkage is valid only when all required artifacts bind the same paper-session day identity convention:
  - `paper_session:<DAY>:PAPER`

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `blocking_codes[]` must be stable and sorted.
- `required_inputs_checked[]` and `materialized_outputs[]` must be stable and sorted.
- One evaluation instant must be captured once per write and reused in the document.

## Day-readiness automation note

When this surface or its governed input-prep dependency is blocked only because a real target-day reference price or equivalent governed target-day market data is not yet available, `ops/tools/run_tomorrow_paper_startup_prep_v1.py` may classify the operator state as `WAITING_FOR_MARKET_DATA`.

For this purpose, a target-day default equity reference price becomes acceptable as soon as a governed positive same-day core-session price exists in `market_data_snapshot_v1` with `ingested_utc` at or after `09:30 America/New_York`.

Prior-day closes, zero values, negative values, and unguided fabricated values remain invalid for startup readiness.

That classification is a subordinate operator projection only.

It must not be treated as startup authority and it must not replace bootstrap, `pre_open_bundle_v1`, or Session Authority truth.

## Consumers

- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/run/c2_preopen_preflight_v1.sh`
- `paper_session_ledger_v1`

## Downstream prohibition

Consumers must not infer startup completion from raw phase-C directories or subprocess return codes when this governed surface is available.

## Migration note

This contract now covers two practical writers that share one governed meaning:

- `run_paper_session_bootstrap_v1.py` owns the startup materialization plane for same-day bootstrap prerequisites in the touched runtime path.
- `run_startup_materialization_v1.py` continues to own the legacy phase-C identity-and-input materialization fact surface used by broader build and readiness flows.

Both writers must preserve fail-closed semantics and must not silently bypass missing prerequisites.

Canonical morning operations note:

- `ops/tools/run_paper_session_bootstrap_v1.py` is the canonical operator-facing morning startup tool under `ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh`.
- bootstrap must emit one ordered runtime prerequisite verification projection that binds owned startup prerequisites, pre-open, promotion, and admission into operator-readable earliest-failure guidance.
- It may continue into Session Authority only after `pre_open_bundle_v1` is `COMPLETE`.
- It may continue into day activation or deeper startup progression only after `session_promotion_decision_v1` is `PROMOTED`.
- It must not continue into day-activation or deeper morning progression when pre-open or Session Authority blocks the day.
