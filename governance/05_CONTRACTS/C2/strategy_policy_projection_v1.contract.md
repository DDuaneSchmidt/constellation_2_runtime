---
id: C2_STRATEGY_POLICY_PROJECTION_CONTRACT_V1
title: "C2 Strategy Policy Projection Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_upper_policy_projection_plane
---

# C2 Strategy Policy Projection Contract V1

## Canonical purpose

The Policy Projection Plane is the governed upper-layer policy surface above Core 2, Core 3, Core 4, and Core 5.

This plane answers only:

- given canonical Core 2 truth, what governed strategy policy projection applies now

## Canonical owner

The one canonical Policy Projection Plane artifact is:

- `strategy_policy_projection.v1`

There is one canonical policy projection artifact per governed `trade_identity_id` per evaluated upper-layer materialization boundary.

## Scope

This plane owns only:

- stop logic projection
- trailing logic projection
- scaling rule projection
- time-exit projection
- risk-rule projection
- policy parameter and version selection
- explicit constraint attachment from exception/intervention artifacts when present

Outputs from this plane are projections only.

## Non-scope

This plane does not own:

- current trade truth
- Core 1 evidence ingestion
- Core 2 truth mutation
- final action eligibility
- broker transmission
- scheduler wakeups
- retry ownership
- operator aggregation truth

This plane must not re-read Core 1 raw evidence directly.

## Required downstream discipline

Policy projection outputs may constrain later flows, but they must never directly execute.

Any downstream executable effect arising from policy projection must re-enter through:

1. Core 3 `lifecycle_action_authority.v1` for action eligibility
2. Core 4 `post_entry_submit_boundary.v1` for exact transmit authorization

## Required policy families

`strategy_policy_projection.v1` must support governed projection for:

- stop policy
- trailing policy
- scaling policy
- time-exit policy
- risk rules

## Hard invariants

1. Core 2 remains the only current-truth owner.
2. Core 3 remains the only action-eligibility owner.
3. Core 4 remains the only exact transmit-authorization owner.
4. Core 5 remains the only derived operator aggregation plane.
5. Policy projection artifacts are artifact-driven only.
6. Policy projection artifacts must never directly authorize or transmit broker instructions.
7. Policy projection artifacts must carry explicit Core 2 references and any exception/intervention refs used as constraints.
8. Any policy-driven executable path must prove re-entry through Core 3 and Core 4.

## Canonical runtime family

The Policy Projection Plane writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/strategy_policy_projection_v1/materializations/<DAY>/<policy_projection_id>/trades/<trade_identity_id>/strategy_policy_projection.v1.json`

## Proof basis

- `governance/05_CONTRACTS/C2/reconciled_trade_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/upper_layer_provenance_spine_v1.contract.md`
