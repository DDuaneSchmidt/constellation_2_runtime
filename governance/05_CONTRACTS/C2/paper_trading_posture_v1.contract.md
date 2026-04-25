---
id: C2_PAPER_TRADING_POSTURE_V1
title: "C2 Paper Trading Posture Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_fact_plane
---

# C2 Paper Trading Posture Contract (V1)

## Purpose

This contract defines the canonical source-authoritative fact surface that records paper-trading posture for a paper-session scope.

This surface answers only whether paper trading is enabled or disabled from the narrow posture or policy standpoint it owns.

It is not session authority, readiness authority, admission authority, or submit authority.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_paper_trading_posture_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/paper_trading_posture_v1/<DAY>/paper_trading_posture.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_posture.v1.schema.json`

## Required inputs

- governed `market_calendar_v1` truth spine for the requested day

## Required meaning

The artifact must identify:

- `day_utc`
- `session_id`
- `system_ready`
- closed `posture_status`
- `policy_reasons[]`
- stable `blocking_codes[]`
- `source_dependencies[]`
- producer identity
- `produced_at_utc`
- explicit `freshness_verdict`
- explicit `linkage_verdict`
- explicit `authority_scope = NON_AUTHORITY_FACT`
- explicit `binding_classification = NON_BINDING_DIAGNOSTIC`

Compatibility fields may be retained for existing consumers, but they remain derived-only posture facts:

- `posture_class`
- `blocking_family`
- `expected_no_op_today`
- `blocking_reason_codes[]`

## Allowed status values

- `ENABLED`
- `DISABLED`
- `BLOCKED`
- `UNKNOWN`
- `STALE`
- `MALFORMED`

## Fail-closed rules

- Missing or malformed market-calendar evidence must not yield `ENABLED`.
- `system_ready=true` is allowed only when the governed market-calendar truth explicitly marks the day as a trading session.
- Non-trading sessions may publish a valid disabled posture with expected no-op semantics.
- This surface must never publish `READY`, `ADMITTED`, or operational session `AUTHORIZED`.

## Freshness and linkage

- Freshness is current only when the artifact resolves an explicit market-calendar record for the requested `day_utc`.
- Linkage is valid only when the posture binds the same day-scoped paper-session identity convention:
  - `paper_session:<DAY>:PAPER`

## Determinism requirements

- JSON emission must use deterministic canonical serialization.
- `blocking_codes[]`, `blocking_reason_codes[]`, `policy_reasons[]`, and `source_dependencies[]` must be stable and sorted.

## Consumers

- `ops/tools/run_session_readiness_refresh_v1.py`
- `ops/tools/run_c2_market_data_preopen_prepare_v1.py`
- `paper_session_ledger_v1`

## Downstream prohibition

Consumers must not reconstruct paper-trading posture directly from raw market-calendar files when this governed surface is available.

## Migration note

This contract formalizes the posture artifact shape already consumed in source code and removes the prior gap where consumers referenced a surface with no canonical contract, schema, or writer in source authority.
