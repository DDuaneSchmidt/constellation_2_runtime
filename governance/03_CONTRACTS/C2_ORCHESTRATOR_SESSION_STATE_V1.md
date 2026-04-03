---
id: C2_ORCHESTRATOR_SESSION_STATE_V1
title: "Constellation 2.0 — Orchestrator Session State Contract V1"
status: DRAFT
version: 1
created_utc: 2026-03-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
tags:
  - orchestrator
  - session_state
  - market_calendar
  - fail_closed
  - lifecycle
---

# C2 Orchestrator Session State Contract V1

## Objective

Define governed session-state behavior before activity-gated stage execution.

## Session State Enum

`session_state ∈ { TRADING_SESSION, NON_TRADING_SESSION, UNKNOWN_SESSION }`

## Resolution Rules (authoritative order)

1. Read market calendar truth from:
   - `constellation_2/runtime/truth/market_calendar_v1/dataset_manifest.json`
   - referenced year file(s), e.g. `constellation_2/runtime/truth/market_calendar_v1/<EXCHANGE>/<YEAR>.jsonl`
2. If an explicit `(exchange, day_utc)` record exists:
   - `is_trading_session=true` -> `TRADING_SESSION`
   - `is_trading_session=false` -> `NON_TRADING_SESSION`
3. If no explicit calendar record exists:
   - weekend fallback (`Saturday` or `Sunday`) -> `NON_TRADING_SESSION`
   - otherwise -> `UNKNOWN_SESSION` (fail-closed)

## Lifecycle Rules

### TRADING_SESSION

- Run normal orchestrator pipeline.

### NON_TRADING_SESSION

- Orchestrator lifecycle class is `DEGRADED_NO_SESSION`.
- Encoding uses existing verdict schema:
  - `status = "DEGRADED"`
  - `reason_codes` includes `NON_TRADING_SESSION`.
- Required behavior:
  - MUST NOT execute `A7A_GOVERNED_SUBMIT_V5`.
  - MUST NOT convert no same-day trading activity into a blocking failure.
  - MUST preserve immutable artifact semantics (no rewrite).

### UNKNOWN_SESSION

- Fail closed:
  - `status = "FAIL"`
  - `reason_codes` includes `UNKNOWN_SESSION`.
- MUST NOT execute `A7A_GOVERNED_SUBMIT_V5`.

## Evidence Requirements

Attempt manifest MUST include:
- `session_state`
- `session_source.path`
- `session_source.sha256` when file-backed
- `session_source.resolution_reason`

## Non-Claims

- Does not change ABORT safety-breach policy.
- Does not alter immutable artifact policies.
- Does not redefine strategy logic.
