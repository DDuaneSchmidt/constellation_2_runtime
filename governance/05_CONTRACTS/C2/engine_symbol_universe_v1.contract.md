---
id: C2_ENGINE_SYMBOL_UNIVERSE_CONTRACT_V1
title: "C2 Engine Symbol Universe Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-03
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Engine Symbol Universe Contract v1

## Objective

Enforce a deterministic, audited symbol universe per engine.

This reduces accidental cross-symbol activation, prevents unintended exposure, and makes per-sleeve refinement more controlled.

## Definitions

- **Engine registry**: `governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json`
- **allowed_symbols**: a JSON field on each engine object:
  - `null` => engine may run on any symbol the orchestrator is invoked with
  - `["SPY","QQQ", ...]` => engine may run only on listed symbols
- **Run symbol**: the `--symbol` argument passed to the orchestrator for that run/day.

## Requirements

1) Engine registry MUST include `allowed_symbols` for every engine entry.
2) Orchestrator MUST enforce the symbol universe:
   - If `allowed_symbols` is `null`, the engine is eligible (subject to activation_status).
   - If `allowed_symbols` is a list of strings, the engine is eligible only if run_symbol is present.
   - Any other type is invalid and MUST fail-closed.
3) `activation_status` remains authoritative:
   - Only `ACTIVE` engines are eligible for selection.
4) Enforcement MUST occur before invoking the engine runner.
5) Enforcement MUST be deterministic (no wall-clock, no external IO).

## Audit evidence

- The orchestrator MUST emit a deterministic log line when skipping an engine due to symbol universe mismatch:
  - `SKIP_ENGINE_SYMBOL_UNIVERSE engine_id=<...> symbol=<...> allowed_symbols=<...>`

## Non-goals

- This contract does not require engines to independently enforce universes when invoked directly.
  (Optional future hardening may add engine-side checks.)

## Future extension (non-binding)

A future multi-account design may bind per-engine symbol universes together with per-engine `ib_account`.
That is explicitly out of scope for v1.
