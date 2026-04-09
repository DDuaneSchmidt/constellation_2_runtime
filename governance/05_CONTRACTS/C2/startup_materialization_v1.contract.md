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

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_startup_materialization_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/startup_materialization_v1/<DAY>/startup_materialization.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json`

## Required inputs

- same-day paper intents under `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`
- same-day phase-C preflight root under `constellation_2/runtime/truth/phaseC_preflight_v1/<DAY>/`
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

## Allowed status values

- `SUCCESS`
- `FAIL`
- `MISSING_DEPENDENCY`
- `MALFORMED`
- `STALE`
- `UNKNOWN`

## Fail-closed rules

- Missing same-day intents must not yield `SUCCESS`.
- Missing or malformed latest active attempt evidence must not yield `SUCCESS`.
- A zero materializer process exit without a supported same-day identity output set must not yield `SUCCESS`.
- Missing, stale, malformed, unknown, or contradictory prerequisite evidence must deny `SUCCESS`.
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

## Consumers

- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/run/c2_preopen_preflight_v1.sh`
- `paper_session_ledger_v1`

## Downstream prohibition

Consumers must not infer startup completion from raw phase-C directories or subprocess return codes when this governed surface is available.

## Migration note

This contract formalizes a previously referenced but ungoverned startup materialization surface in source authority. It replaces implicit reliance on a missing canonical writer.
