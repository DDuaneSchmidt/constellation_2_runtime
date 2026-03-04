---
id: C2_SLEEVE_REGISTRY_CONTRACT_V1
title: "C2 Sleeve Registry Contract (Authoritative Sleeve Configuration)"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Sleeve Registry Contract (Authoritative Sleeve Configuration)

## Purpose

This contract defines the canonical sleeve registry and the required fields that make a sleeve fully specified, auditable, and deterministic.

The sleeve registry is the authoritative source for:

- sleeve identity and mode
- IB account binding
- symbol universe binding (explicit list)
- truth partition binding
- IB gateway connection profile binding (host/port/client ids)

## Canonical registry file

- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`

## Required sleeve fields

Each sleeve entry MUST include:

- `sleeve_id` (string; stable identifier; uppercase recommended)
- `enabled` (boolean)
- `mode` (`PAPER` or `LIVE`)
- `ib_account` (string; e.g. DUO... or U...)
- `symbols` (array of strings; MUST be explicit; empty list is allowed only if the sleeve is explicitly non-trading and the orchestrator treats it as NO_ACTIVITY)
- `truth_partition` (string; MUST equal `truth_sleeves/<sleeve_id>/<mode>`)
- `ib_gateway_profile` (object):
  - `host` (string)
  - `port` (integer)
  - `client_id_market_data` (integer)
  - `client_id_orders` (integer)
  - `client_id_observer` (integer)

## Invariants

### C2_SLEEVE_REGISTRY_DETERMINISM_V1

1. **No freeform configuration**
   - Registry schema must be explicit and validated by runtime tooling.
   - Unknown fields MUST be rejected (fail closed) or ignored only if explicitly governed (this contract does not permit freeform fields).

2. **Mode authority**
   - A sleeve’s `mode` is authoritative from the registry only.
   - Runtime code MUST NOT allow a sleeve to flip mode without a registry change.

3. **Account authority**
   - A sleeve’s `ib_account` is authoritative from the registry only.
   - Runtime artifacts MUST embed the account id and match the registry.

4. **Client id isolation**
   - If multiple sleeves are enabled simultaneously, client ids MUST NOT collide across sleeves for the same host/port.
   - Collision MUST cause fail-closed ABORT.

## Required evidence surfaces (minimum)

For each sleeve day:

- readiness pointer head resolves to a readiness artifact embedding:
  - `sleeve_id`, `mode`, `ib_account`
- orchestrator verdict artifact embedding:
  - `sleeve_id`, `mode`, `ib_account`
- sha256 proof lines for pointer head targets

## Non-claims

- This contract does not define strategy logic.
- This contract defines configuration authority and validation requirements only.
