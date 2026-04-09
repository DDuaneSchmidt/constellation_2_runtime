---
id: C2_INTENTS_DAY_COMPLETENESS_V1
title: "C2 Intents Day Completeness Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_intents_day_completeness
---

# C2 Intents Day Completeness Contract (V1)

## Purpose

This contract defines the explicit upstream day-completeness prerequisite fact for paper-day
startup.

It answers only whether the governed day-level intent snapshot inputs are complete enough for
startup to begin.

It is not the final day-start authority surface.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_intents_day_completeness_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/intents_day_completeness_v1/<DAY>/intents_day_completeness.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/intents_day_completeness.v1.schema.json`

## Authority classification

- This surface is non-authoritative.
- It is a narrow prerequisite fact only.
- `trading_day_state_machine_v1` owns the final day-start decision.

## Required inputs

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/`
- governed exposure-intent payloads under that day root when present
- governed `no_intents_day.v1.json` marker when present

## Allowed status values

- `COMPLETE`
- `NO_INTENTS_DECLARED`
- `INCOMPLETE`
- `MALFORMED`
- `UNKNOWN`

## Fail-closed rules

- Missing day roots, empty day roots, malformed intent files, malformed marker files, or
  contradictory marker-plus-intent combinations must not pass startup prerequisites.
- This surface must not infer completeness from partial day inputs.
- This surface must not claim readiness, admission, or authority semantics.

## Freshness and linkage

- Shared paper-session linkage convention:
  - `paper_session:<DAY>:PAPER`
- The writer must bind the requested `day_utc` to the governed intents snapshot day root.
- `no_intents_day.v1.json` may satisfy path linkage only when its own `day_utc` matches the
  requested day.

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `required_inputs_checked[]`, `missing_inputs[]`, and `blocking_codes[]` must be stable and
  sorted.

## Consumers

- `ops/tools/run_paper_day_control_plane_v1.py`
- `ops/tools/run_trading_day_control_plane_v1.py`
- `ops/tools/run_trading_day_execution_control_plane_v1.py`
- `ops/tools/run_trading_day_state_machine_v1.py`

## Migration note

This contract exists to move missing-day startup prerequisites earlier than
`startup_materialization_v1`, so the canonical day-start control plane can report the first true
blocker without relying on downstream legacy summaries.
