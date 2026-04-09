---
id: C2_TRADING_DAY_INTENT_GENERATION_V1
title: "C2 Trading Day Intent Generation Contract (V1)"
status: DRAFT
version: 1
created_utc: 2026-04-08
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_trading_day_intent_generation
---

# C2 Trading Day Intent Generation Contract (V1)

## Purpose

This contract defines the single canonical upstream pre-start generator for touched daily intent
production.

It exists to produce one governed daily outcome before startup/day-start control evaluates the
day:

- canonical intent snapshots under `intents_v1/snapshots/<DAY>/`
- or a governed `no_intents_day.v1.json` valid-zero marker
- or an explicit generation defect/block report

It is not a day-open authority surface.

## Truth owner

- Truth owner: Constellation governance
- Canonical writer: `ops/tools/run_trading_day_intent_generation_v1.py`
- Canonical artifact path:
  - `constellation_2/runtime/truth/reports/trading_day_intent_generation_v1/<DAY>/trading_day_intent_generation.v1.json`
- Canonical schema:
  - `governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_intent_generation.v1.schema.json`

## Authority classification

- This surface is non-authoritative.
- It is an upstream prerequisite fact only.
- `trading_day_state_machine_v1` remains the sole top-level daily startup/day-start authority.

## Governed producer topology

- Producer topology MUST be resolved from the ACTIVE engine registry:
  - `governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json`
- Required pre-start producers are ACTIVE engine entries with governed
  `engine_runner_path` values ending in `run_*_intents_day_v1.py`.
- `C2_INTENT_SIMULATOR_V1` is not a required production pre-start producer for this contract:
  - it remains a structural validation sleeve and may be recorded as skipped.
- Producer execution order MUST be deterministic and fixed.

## Required semantics

1. The writer MUST resolve one canonical truth root.
2. The writer MUST verify governed direct-writer code lock before invoking a producer:
   - actual file sha256 MUST match registry `engine_runner_sha256`
3. The writer MUST run each required producer before startup/day-start completeness is evaluated.
4. Producers MUST write only to the canonical truth root.
5. Plain `NO_INTENT` stdout is not sufficient final truth.
6. Valid-zero is established only by governed `no_intents_day.v1.json`.
7. Contradictory outputs (intent snapshots plus valid-zero marker) MUST fail closed.
8. Existing canonical outputs for the day MAY be reused idempotently.

## Allowed final status values

- `INTENTS_PRESENT`
- `VALID_ZERO`
- `BLOCKED_VALID`
- `BLOCKED_BY_DEFECT`

## Fail-closed rules

- Missing registry rows, missing writer paths, writer hash mismatches, non-zero producer exits,
  contradictory outputs, malformed valid-zero markers, or zero-return producers without canonical
  output MUST yield `BLOCKED_BY_DEFECT`.
- This surface must not open the day.
- This surface must not bypass `intents_day_completeness_v1`.
- This surface must not claim readiness/admission/authority semantics.

## Consumers

- `ops/tools/run_trading_day_state_machine_v1.py`
- `ops/run/c2_preopen_preflight_v1.sh`

## Integration order

The touched startup/day-start chain MUST remain:

1. `trading_day_intent_generation_v1`
2. `intents_day_completeness_v1`
3. `trading_day_execution_control_plane_v1`
4. `trading_day_state_machine_v1` final authority decision
