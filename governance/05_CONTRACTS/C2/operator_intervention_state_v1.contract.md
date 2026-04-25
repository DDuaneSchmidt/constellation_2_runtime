---
id: C2_OPERATOR_INTERVENTION_STATE_CONTRACT_V1
title: "C2 Operator Intervention State Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_upper_operator_intervention_plane
---

# C2 Operator Intervention State Contract V1

## Canonical purpose

`operator_intervention_state.v1` is the governed human intervention artifact for explicit review, override, and acknowledgement state.

This plane answers only:

- what human-governed intervention state currently constrains later flows

## Canonical owner

The one canonical operator intervention artifact is:

- `operator_intervention_state.v1`

## Scope

This plane owns only:

- review state
- narrow override state
- acknowledgement state
- explicit human decision classification
- expiry and one-shot versus durable semantics for supported override scopes

## Required override discipline

Overrides must be:

- narrow in scope
- explicit
- expiring
- auditable

Supported override semantics must never become a broad direct-execution lane.

## Non-scope

This plane must not:

- rewrite Core 2 truth
- replace Core 3 action authority
- replace Core 4 boundary verdicts
- directly authorize or transmit broker instructions

Any executable effect after intervention remains required to re-enter through Core 3 and Core 4.

## Canonical runtime family

The Intervention / Exception Plane writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/operator_intervention_state_v1/materializations/<DAY>/<intervention_id>/trades/<trade_identity_id>/operator_intervention_state.v1.json`

## Proof basis

- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/exception_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/upper_layer_provenance_spine_v1.contract.md`
