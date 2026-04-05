---
schema_id: c2_contract
schema_version: v1
contract_id: C2_AUTHORIZATION_ARTIFACTS_GATE_V1
owner: ops/governance
status: active
created_utc: 2026-03-01T00:00:00Z
---

# C2 Authorization Artifacts Gate v1

## Purpose

When Constellation’s authority head is **PASS** and **authoritative=true**, the system must be able to prove that every exposure intent for that day has a corresponding authorization artifact under canonical truth.

This prevents “submit boundary” checks from failing due to missing authorization files and removes ambiguity about whether capital authority actually executed.

## Scope

- Applies to **PAPER** and **LIVE** days whenever the authority head is present and indicates PASS+authoritative.
- Applies to governed intent files under:
  - `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/*.json`
- Allocation/authorization remain fail-closed for unsupported intent schemas or intents that lack a deterministic sizing basis.

## Inputs

For day `<DAY>`:

1) Authority head (required to trigger enforcement)

- `constellation_2/runtime/truth/run_pointer_v2/canonical_authority_head.v1.json`
- Must satisfy:
  - `status == "PASS"`
  - `authoritative == true`
  - `day_utc == <DAY>`

2) Allocation artifact (authorization depends on it)

- `constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json`

3) Intent files

- `constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/*.json`

## Required Outputs

For each governed intent file `F` under intents day `<DAY>`:

- Let `H = sha256(bytes(F))`
- Then the following file **must exist**:

`constellation_2/runtime/truth/engine_activity_v1/authorization_v1/<DAY>/<H>.authorization.v1.json`

and it must satisfy minimal schema checks:

- `schema_id == "C2_AUTHORIZATION_V1"`
- `schema_version == 1`
- `day_utc == <DAY>`
- `intent_hash == <H>`
- `authorization.decision ∈ {"AUTHORIZED","REJECTED"}`
- `authorization.authorized_quantity >= 0`

## Allocation Semantics

`run_capital_authority_allocation_day_v1.py` is the governed allocation writer for Bundle A v1.

It MUST:
- consume the policy manifest, capital risk envelope, correlation envelope gate, exposure net, and day intent set
- remain deterministic and fail-closed on missing or invalid inputs
- emit `decision = "AUTHORIZED"` with `authorized_quantity > 0` only when the intent has a deterministic sizing basis and all upstream gating inputs are passing
- emit `decision = "REJECTED"` with `authorized_quantity = 0` for unsupported intents, under-specified intents, or days with non-positive headroom

This contract does not require every governed intent schema to be sizeable under v1. It requires that any nonzero authorization be traceable to governed inputs and deterministic bounds already present on the Bundle A path.

## Orchestrator Ordering Requirement

For a given day `<DAY>`:

1) `run_capital_authority_allocation_day_v1.py` must run after intents exist for `<DAY>` and produce the allocation artifact.

2) `run_authorization_artifacts_day_v1.py --day_utc <DAY>` must run after allocation and intents exist, producing authorization files.

3) Submit boundary / dry submit proof may run only after (1) and (2) are satisfied (or it must fail closed).

## Enforcement

- Governance preflight script MUST fail closed when authority head is PASS+authoritative but required authorization artifacts are missing:
  - `ops/governance/preflight_require_authorization_for_authority_day_v1.sh`

## Failure Semantics

- Missing any required artifact is a **FAIL** (hard stop).
- This contract is deterministic and day-scoped; no “latest pointer” is used.
