---
id: C2_MULTI_ACCOUNT_TOPOLOGY_CONTRACT_V1
title: "C2 Multi-Account / Multi-Mode Sleeve Topology Contract"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Multi-Account / Multi-Mode Sleeve Topology Contract

## Purpose

This contract introduces a governed topology where **sleeves are first-class** and each sleeve is explicitly bound to:

- `mode` (PAPER or LIVE)
- `ib_account`
- `symbols` (explicit list)
- a dedicated **truth partition**

This change is strictly **topology + routing + truth partitioning + orchestration**. It MUST NOT change engine strategy logic (signals, sizing, allocation math).

## Definitions

- **Sleeve**: an independently operated sub-topology identified by `sleeve_id`, configured via the governed registry `C2_SLEEVE_REGISTRY_V1`.
- **Mode**: `PAPER` or `LIVE`. A sleeve is always exactly one mode at a time (per registry).
- **Truth partition**: an isolated write-root for all sleeve outputs:
  - `constellation_2/runtime/truth_sleeves/<sleeve_id>/<mode>/...`

## Invariants

### C2_SLEEVE_TOPOLOGY_AUTHORITY_V1

1. **Registry authority**
   - Sleeve identity, mode, account, symbols, gateway profile, and partition are authoritative only from:
     - `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`.

2. **No ambiguity**
   - Runtime code MUST NOT infer sleeve mode/account/symbols from environment variables, ad-hoc flags, or truth artifacts.
   - If a required field is missing or invalid in the registry, the system MUST fail closed.

3. **No mixing across sleeves**
   - No sleeve may write to another sleeve’s truth partition.
   - No sleeve may write to the global canonical truth root, except explicitly governed global rollups.

4. **Compatibility with single-account mode**
   - If (and only if) the sleeve registry resolves to exactly one active sleeve, then the system may be considered compatible with the single-sleeve operational model.
   - This contract does not automatically deactivate the existing single-account contract; rather:
     - single-account invariants remain satisfied when all active sleeves resolve to one unique account id per mode/day.

## Required evidence surfaces (minimum)

For each sleeve-run day, the following MUST be produced under the sleeve truth partition:

- a sleeve-scoped readiness verdict + pointer head
- a sleeve-scoped orchestrator verdict + pointer head
- each major artifact must embed the sleeve’s `ib_account` and `mode`

## Fail-closed conditions

The system MUST ABORT (hard fail) on any of the following:

- registry cannot be parsed or validated
- sleeve_id not found
- mode not one of `PAPER|LIVE`
- `truth_partition` path does not match the canonical required shape
- any artifact account_id/mode conflicts with the registry binding
- evidence of cross-partition writes

## Non-claims

- This contract does not claim profitability, fill quality, or broker uptime.
- This contract defines topology, routing, and truth partition authority only.
