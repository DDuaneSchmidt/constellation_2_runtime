---
id: C2_ORCHESTRATION_STATE_CONTRACT_V1
title: "C2 Orchestration State Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_upper_orchestration_plane_state
---

# C2 Orchestration State Contract V1

## Canonical purpose

The Orchestration Plane owns artifact-driven cadence state above the lower-core backbone.

This plane answers only:

- when should a governed evaluation, alert, review, or retry request be emitted

## Canonical owner

The one canonical orchestration state artifact is:

- `orchestration_state.v1`

## Scope

This plane owns only:

- periodic evaluation cadence
- trigger emission cadence
- retry cadence
- alert cadence
- review wakeup cadence
- session-aware cadence handling
- explicit degradation and blockage posture for orchestration timing

The Orchestration Plane emits artifacts only.

## Non-scope

This plane does not own:

- trade truth
- action eligibility
- transmit authorization
- broker instruction transport
- hidden workflow side effects

## Required cadence semantics

`orchestration_state.v1` must make explicit:

- cadence window start/end
- next evaluation time
- pending retries
- pending alerts
- emitted trigger references
- session phase
- degradation or blockage posture

## Hard invariants

1. Orchestration is artifact-driven only.
2. Orchestration must never directly execute broker-facing code.
3. Orchestration must never mutate Core 2 truth.
4. Orchestration must never replace Core 3 or Core 4 authority.
5. Any executable path initiated by orchestration must re-enter through Core 3 and Core 4.
6. Session-aware scheduling must remain explicit and fail closed when session posture is blocked.

## Canonical runtime family

The Orchestration Plane writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/orchestration_state_v1/materializations/<DAY>/<orchestration_state_id>/trades/<trade_identity_id>/orchestration_state.v1.json`

## Proof basis

- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/strategy_policy_projection_v1.contract.md`
- `governance/05_CONTRACTS/C2/upper_layer_provenance_spine_v1.contract.md`
