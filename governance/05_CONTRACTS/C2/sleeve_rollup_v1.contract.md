---
id: C2_SLEEVE_ROLLUP_V1
title: "C2 Sleeve Rollup Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_control_stack
---

# C2 Sleeve Rollup Contract (V1)

## Purpose

This contract formalizes the canonical execution-completion fact surface for the paper-session control stack.

It records the global per-day multi-sleeve execution completion outcome after the per-sleeve orchestrator runs.

This surface is execution fact only.

It is not readiness authority, admission authority, or session authority.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_c2_multi_sleeve_orchestrator_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<DAY>/sleeve_rollup.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_rollup.v1.schema.json`

## Required meaning

The artifact must identify:

- `day_utc`
- `input_day_utc`
- `produced_utc`
- closed rollup `status`
- paper-session `session_id`
- bound `paper_session_ledger_path`
- bound `ledger_id`
- bound ledger `authority_status`
- registry path
- per-sleeve execution facts
- producer identity

## Allowed status values

- `PASS`
- `DEGRADED`
- `FAIL`
- `ABORTED`

## Fail-closed rules

- Rollup publication must not proceed unless the bound paper-session ledger grants authority.
- Missing per-sleeve verdict evidence must not yield `PASS`.
- `ABORTED` must be published if any per-sleeve orchestrator verdict aborts.
- The surface must not infer or publish session authority on its own.

## Consumers

- `paper_session_ledger_v1`
- `ops/run/c2_preopen_preflight_v1.sh`
- `ops/run/c2_verify_multi_sleeve_rollup_v1.sh`

## Migration note

This contract formalizes the previously unversioned global rollup payload so the paper-session ledger can bind governed execution-completion evidence and post-submit lifecycle references when they exist.
