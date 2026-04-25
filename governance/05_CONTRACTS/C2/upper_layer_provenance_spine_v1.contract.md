---
id: C2_UPPER_LAYER_PROVENANCE_SPINE_CONTRACT_V1
title: "C2 Upper Layer Provenance Spine Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_upper_layer_cross_plane_provenance
---

# C2 Upper Layer Provenance Spine Contract V1

## Canonical purpose

The Cross-layer Provenance Spine governs traceability across the upper planes and back into the lower-core chain.

This spine answers only:

- what lower-core artifacts an upper-layer artifact depended on
- what cross-upper-plane artifacts constrained it
- what executable re-entry path is mandatory before any action may occur

## Canonical owner

The one canonical provenance artifact is:

- `upper_layer_provenance_spine.v1`

There is one canonical provenance spine artifact per upper-layer artifact materialization.

## Required lower-core references

Every upper-layer provenance artifact must carry:

- Core 2 truth and health references
- Core 3 authority references where relevant
- Core 4 boundary references where relevant
- Core 5 operator-health references where surfaced to humans

## Required cross-upper-plane references

The provenance spine must record:

- policy projection refs when orchestration, exception, or intervention depends on them
- exception refs when policy, orchestration, or intervention depends on them
- intervention refs when policy or orchestration depends on them
- orchestration refs when a trigger was emitted from orchestration state

## Required executable re-entry path

Any executable path from any upper-layer artifact must prove governed re-entry through:

1. Core 3 `lifecycle_action_authority.v1`
2. Core 4 `post_entry_submit_boundary.v1`

Upper-layer actions without provenance or without explicit Core 3 / Core 4 re-entry refs are forbidden.

## Hard invariants

1. Provenance is mandatory for every upper-layer artifact.
2. Provenance-free upper-layer actions are forbidden.
3. Missing lower-core refs must fail closed.
4. Cross-plane ref inconsistency must remain explicit.
5. Executable re-entry path completeness is non-negotiable.

## Canonical runtime family

The Cross-layer Provenance Spine writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/upper_layer_provenance_spine_v1/materializations/<DAY>/<artifact_family>/<artifact_id>/upper_layer_provenance_spine.v1.json`

## Proof basis

- `governance/05_CONTRACTS/C2/reconciled_trade_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md`
- `governance/05_CONTRACTS/C2/operator_trade_health_v1.contract.md`
