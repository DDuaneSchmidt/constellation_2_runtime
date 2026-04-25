---
id: C2_EXCEPTION_STATE_CONTRACT_V1
title: "C2 Exception State Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_upper_exception_plane
---

# C2 Exception State Contract V1

## Canonical purpose

The Intervention / Exception Plane classifies non-normal conditions that constrain later flows.

This plane answers only:

- what abnormal or exception condition currently constrains the system

## Canonical owner

The one canonical exception artifact is:

- `exception_state.v1`

## Scope

This plane owns only:

- abnormal-state classification
- partial fill classification
- reconnect or replay recovery classification
- manual IB change classification
- orphan-order classification
- constrained repair recommendation classification

## Non-scope

This plane does not own:

- Core 2 truth
- Core 3 action authority
- Core 4 boundary authority
- direct execution
- silent normalization of abnormal conditions into normal flow

Exception outputs may constrain or recommend later flows, but they must never replace Core 2, Core 3, or Core 4.

## Hard invariants

1. Exception handling is classification-first.
2. Exception outputs are artifact-driven only.
3. Exception artifacts may constrain later flow but may not directly execute.
4. Any executable path arising from exception handling must re-enter through Core 3 and Core 4.
5. Abnormal conditions must remain explicit and auditable.

## Canonical runtime family

The Intervention / Exception Plane writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/exception_state_v1/materializations/<DAY>/<exception_id>/trades/<trade_identity_id>/exception_state.v1.json`

## Proof basis

- `governance/05_CONTRACTS/C2/reconciled_trade_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/upper_layer_provenance_spine_v1.contract.md`
