---
id: C2_ORCHESTRATION_TRIGGER_CONTRACT_V1
title: "C2 Orchestration Trigger Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_upper_orchestration_plane_trigger
---

# C2 Orchestration Trigger Contract V1

## Canonical purpose

`orchestration_trigger.v1` is the governed trigger artifact emitted by the Orchestration Plane.

It answers only:

- what trigger class fired
- why it fired
- what governed downstream target must evaluate next

## Canonical owner

The one canonical orchestration trigger artifact is:

- `orchestration_trigger.v1`

## Required trigger classes

`orchestration_trigger.v1` must support at least:

- `PERIODIC_EVALUATION`
- `RETRY`
- `ALERT`
- `REVIEW`
- `SESSION_OPEN`

## Required semantics

Every trigger must carry:

- stable trigger identity
- trigger reason class and code
- scope or trade reference
- emitted timestamp
- downstream target type
- required lower-core and upper-plane references

## Non-scope

Triggers must not:

- mutate trade truth
- decide final action eligibility
- authorize transmission
- directly send broker instructions
- perform direct executable side effects

Any downstream execution remains required to re-enter through Core 3 and Core 4.

## Canonical runtime family

The Orchestration Plane writes trigger artifacts under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/orchestration_trigger_v1/materializations/<DAY>/<trigger_id>/trades/<trade_identity_id>/orchestration_trigger.v1.json`

## Proof basis

- `governance/05_CONTRACTS/C2/orchestration_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/upper_layer_provenance_spine_v1.contract.md`
