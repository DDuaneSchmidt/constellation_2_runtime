---
id: C2_INTENT_SIMULATOR_V1_CONTRACT
title: "C2 Intent Simulator v1 Contract (Deterministic Daily Structural Intent Wave)"
status: DRAFT
version: 1
created_utc: 2026-03-01
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

## Purpose

**C2_INTENT_SIMULATOR_V1** is a **structural validation sleeve**. It exists to deterministically exercise the Constellation pipeline with a fixed, daily intent wave.

This sleeve is **not** an alpha engine. It is a regression guard and spine exerciser.

## Scheduling contract (FAIL-CLOSED)

The sleeve **MUST** accept:

- `--produced_utc YYYY-MM-DDTHH:MM:SSZ` (UTC, exact `Z` form)

The sleeve **MUST** convert `produced_utc` to `America/New_York` and require:

- `hour == 10`
- `minute == 00`
- `second == 00`

No tolerance window. No rounding. No `>=` or `<=`. Any deviation **FAIL_CLOSED**.

## Run frequency contract (FAIL-CLOSED)

Exactly **one run per calendar day (day_utc)**.

If any intents already exist at:

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/`

then the sleeve **MUST FAIL_CLOSED** (refuse overwrite; no partial writes).

## Output contract

Output location (canonical truth only):

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/`

One file per scenario:

- `<INTENT_HASH>.exposure_intent.v1.json`

Where:

- `INTENT_HASH = sha256(file_bytes)`
- `file_bytes = canonical_json_bytes_v1(intent_obj) + b"\n"`

Files **MUST** be newline-terminated and written via an atomic, refuse-overwrite mechanism.

## Deterministic identity

For each scenario:

- `intent_id = sha256(engine_id + "|" + scenario_name + "|" + day_utc)` (hex)

No randomness. No UUIDs. No timestamps inside IDs.

## Scenario matrix (exactly 7)

All scenarios:

- `underlying.symbol = "SPY"`
- `underlying.currency = "USD"`
- `engine.mode = "PAPER"`
- Engine IDs referenced by scenarios **MUST** be `activation_status == ACTIVE` in `ENGINE_MODEL_REGISTRY_V1.json` (else FAIL_CLOSED).

The 7 required scenarios:

1. **TREND_EQUITY_OPEN** (C2_TREND_EQ_PRIMARY_V1) — `LONG_EQUITY`, `target_notional_pct="0.01"`
2. **TREND_EQUITY_CLOSE** (C2_TREND_EQ_PRIMARY_V1) — `LONG_EQUITY`, `target_notional_pct="0"`
3. **MEAN_REVERSION_OPEN** (C2_MEAN_REVERSION_EQ_V1) — `LONG_EQUITY`, `target_notional_pct="0.01"`
4. **VOL_INCOME_SHORT_PUT** (C2_VOL_INCOME_DEFINED_RISK_V1) — `SHORT_VOL_DEFINED`, option metadata `{structure:"PUT", direction:"SELL"}`
5. **DEFENSIVE_TAIL_LONG_PUT** (C2_DEFENSIVE_TAIL_V1) — `SHORT_VOL_DEFINED`, option metadata `{structure:"PUT", direction:"BUY"}`
6. **EVENT_DISLOCATION_CALL** (C2_EVENT_DISLOCATION_V1) — `SHORT_VOL_DEFINED`, option metadata `{structure:"CALL", direction:"BUY"}`
7. **EXIT_OBLIGATION_EQUITY_CLOSE** (C2_MEAN_REVERSION_EQ_V1) — `LONG_EQUITY`, `target_notional_pct="0"`

## Fail-closed semantics

The sleeve MUST fail (no partial writes) if any of the following are true:

- produced_utc not exactly 10:00:00 America/New_York
- intents already exist for the day
- any referenced scenario engine_id is not ACTIVE in engine registry
- schema validation fails
- canonicalization fails
- any output file would overwrite an existing file

## Replay safety

Re-running the same day is **not permitted** (idempotency is enforced by refusing pre-existing output).

Identical inputs (same `produced_utc`, same registry SHA argument, same code, same governed schemas) must result in byte-identical outputs.
