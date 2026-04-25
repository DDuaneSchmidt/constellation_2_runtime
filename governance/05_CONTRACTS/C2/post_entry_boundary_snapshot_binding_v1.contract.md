---
id: C2_POST_ENTRY_BOUNDARY_SNAPSHOT_BINDING_CONTRACT_V1
title: "C2 Post-Entry Boundary Snapshot Binding Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_snapshot_binding
---

# C2 Post-Entry Boundary Snapshot Binding Contract V1

## Purpose

`post_entry_boundary_snapshot_binding.v1` seals one request evaluation to exact upstream references.

It answers:

- what exact upstream truth, action-authority, identity, and rule snapshots apply to this request evaluation

## Required bindings

Every binding must include:

- one sealed request reference
- one exact Core 2 truth snapshot reference
- one exact Core 3 action-authority snapshot reference
- one exact execution identity snapshot/reference
- one exact rule-version set

## Binding immutability

Once created, the binding is immutable for that evaluation.

Core 4 must not swap in newer snapshots under the same binding.

## Invalid and stale bindings

The binding must be invalidated if:

- a required snapshot is missing
- Core 2 is stale for transmit use
- Core 3 is stale for transmit use
- identity snapshot/reference is stale
- any bound snapshot is superseded before evaluation
- rule-version binding is missing
- trade identity, environment, sleeve, account, or routing continuity do not align across the bound inputs

## Evaluation consistency rules

Core 4 must evaluate only against the exact sealed binding artifact.

It must not:

- fetch floating latest state during evaluation
- silently replace a stale input with a newer one
- merge multiple candidate snapshots into one implied state

## Rule-version binding

The binding must record the exact rule pack and contract/schema versions used by Core 4 for:

- request sealing
- snapshot binding
- boundary evaluation
- authorized payload freeze
- provenance emission
