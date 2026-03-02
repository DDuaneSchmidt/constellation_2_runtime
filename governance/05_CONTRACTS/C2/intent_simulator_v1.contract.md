---
id: C2_INTENT_SIMULATOR_V1_CONTRACT
title: "C2 Intent Simulator v1 Contract (Deterministic Daily Structural Intent Wave)"
status: DRAFT
version: 2
created_utc: 2026-03-01
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

## Purpose

**C2_INTENT_SIMULATOR_V1** is a **structural validation sleeve**. It deterministically emits a daily intent wave to exercise Constellation’s allocation + authorization pipeline.

This sleeve is **not** an alpha engine and must not be treated as a production risk-taking sleeve.

## Activation control

The simulator is controlled exclusively by:

- `governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json`
  - `engine_id = C2_INTENT_SIMULATOR_V1`
  - `activation_status ∈ {ACTIVE, INACTIVE}`

### Required semantics

- If `activation_status == INACTIVE`:
  - The orchestrator MUST NOT run the simulator.
  - The simulator MUST NOT emit intents.
  - Preflight MUST NOT fail due to simulator requirements.

- If `activation_status == ACTIVE`:
  - The simulator MUST run only under the scheduling + idempotency contracts below.
  - Preflight MUST enforce simulator invariants.

No other switches or bypasses are permitted.

## Scheduling contract (FAIL-CLOSED)

The simulator MUST accept:

- `--produced_utc YYYY-MM-DDTHH:MM:SSZ` (UTC, exact `Z` form)

It MUST convert `produced_utc` to `America/New_York` and require:

- hour == 10
- minute == 00
- second == 00

No tolerance window. No rounding. Any deviation FAIL_CLOSED.

## Run frequency contract (FAIL-CLOSED)

Exactly **one run per calendar day (day_utc)**.

If any intents already exist at:

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/`

then the simulator MUST FAIL_CLOSED (refuse overwrite; no partial writes).

## Output contract

Output location (canonical truth only):

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY_UTC>/`

One file per scenario:

- `<INTENT_HASH>.exposure_intent.v1.json`

Where:

- `INTENT_HASH = sha256(file_bytes)`
- `file_bytes = canonical_json_bytes_v1(intent_obj) + "\n"`

Files MUST be newline-terminated and written via an atomic, refuse-overwrite mechanism.

## Deterministic identity

For each scenario:

- `intent_id = sha256(engine_id + "|" + scenario_name + "|" + day_utc)` (hex)

No randomness. No UUIDs. No timestamps inside IDs.

## Scenario matrix (exactly 7)

All scenarios:

- `underlying.symbol = "SPY"`
- `underlying.currency = "USD"`
- `engine.mode = "PAPER"`

Engine IDs referenced by scenarios MUST be `activation_status == ACTIVE` in engine registry (else FAIL_CLOSED).

The 7 required scenarios:

1. TREND_EQUITY_OPEN (C2_TREND_EQ_PRIMARY_V1) — LONG_EQUITY, target_notional_pct="0.01"
2. TREND_EQUITY_CLOSE (C2_TREND_EQ_PRIMARY_V1) — LONG_EQUITY, target_notional_pct="0"
3. MEAN_REVERSION_OPEN (C2_MEAN_REVERSION_EQ_V1) — LONG_EQUITY, target_notional_pct="0.01"
4. VOL_INCOME_SHORT_PUT (C2_VOL_INCOME_DEFINED_RISK_V1) — SHORT_VOL_DEFINED, option metadata {structure:"PUT", direction:"SELL"}
5. DEFENSIVE_TAIL_LONG_PUT (C2_DEFENSIVE_TAIL_V1) — SHORT_VOL_DEFINED, option metadata {structure:"PUT", direction:"BUY"}
6. EVENT_DISLOCATION_CALL (C2_EVENT_DISLOCATION_V1) — SHORT_VOL_DEFINED, option metadata {structure:"CALL", direction:"BUY"}
7. EXIT_OBLIGATION_EQUITY_CLOSE (C2_MEAN_REVERSION_EQ_V1) — LONG_EQUITY, target_notional_pct="0"

## Structural chain (registry-gated)

When `C2_INTENT_SIMULATOR_V1` is ACTIVE and orchestrator mode is PAPER, the orchestrator MUST execute the structural chain:

- H_INTENT_SIMULATOR_V1
- A1_CAPAUTH_ALLOCATION_V1
- A1_AUTHORIZATION_ARTIFACTS_V1
- PHASED_DRY_SUBMIT_PROOF_V1 (proof-only)

This structural chain is **registry-gated** and is not required to satisfy systemic risk gates. It must not claim production authority.

## Submission prohibition

The simulator structural chain MUST NOT cross any live submission boundary. Production authority remains the only path that may approach submission.

## Fail-closed semantics

The simulator MUST fail (no partial writes) if any of the following are true:

- produced_utc not exactly 10:00:00 America/New_York
- intents already exist for the day
- any referenced scenario engine_id is not ACTIVE in engine registry
- schema validation fails
- canonicalization fails
- any output file would overwrite an existing file

## Replay safety

Re-running the same day is not permitted (idempotency enforced by refusing pre-existing output).

Identical inputs must result in byte-identical outputs.
