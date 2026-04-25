---
id: C2_TRADING_SYMBOL_POLICY_AUTHORITY_CONTRACT_V1
title: "C2 Trading Symbol Policy Authority Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-10
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Trading Symbol Policy Authority Contract v1

## Purpose

Define one authoritative source of truth for the tradable symbol policy used to produce and submit PAPER order plans.

## Canonical authority

Trading symbol policy is authoritative only from:

- `governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json`

Specifically:

- `engines[].allowed_symbols`

## Required enforcement chain

1. Intent-generation and orchestrator stages MUST enforce engine `allowed_symbols` before producer execution.
2. Phase C and Phase D consumers MUST preserve `engine_id` and plan symbol lineage.
3. Phase D submit boundary MUST re-validate the final plan symbol against `ENGINE_MODEL_REGISTRY_V1.allowed_symbols` for `lineage.engine_id`.

## Non-authoritative symbol surfaces

The following are not the trading-symbol authority:

- `governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json`
  - account eligibility only
- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
  - sleeve coverage/topology only

If those surfaces contain symbol lists, they are compatibility or operational-coverage data only and MUST NOT override the engine registry at submit boundary.

## Fail-closed behavior

- Missing engine row
- missing `allowed_symbols`
- malformed `allowed_symbols`
- missing plan symbol when restriction is present
- symbol not present in `allowed_symbols`

All MUST fail closed before any broker call.

## Allowed semantics

- `allowed_symbols = null` means no symbol restriction from engine policy.
- `allowed_symbols = ["SPY", "QQQ", ...]` means only those symbols are tradable for that engine.

## Non-goals

- This contract does not define account eligibility.
- This contract does not define gateway host/port/client-id routing.
- This contract does not define sizing or budget policy.
