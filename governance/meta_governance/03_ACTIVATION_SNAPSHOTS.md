---
title: Activation Snapshots
status: active
owner: constellation
authority: canonical
doc_type: contract
---

# Activation Snapshots

## Purpose

`ActivationSnapshot` is the active runtime authority object for governed runtime behavior.

## Required Schema

Every activation snapshot must include:

- `snapshot_id`
- `graph_id`
- `graph_hash`
- `interpreter_version`
- `active_at`
- `activated_by`
- `activation_scope`
- `approval_refs`
- `evaluation_refs`
- `rollback_snapshot_ref`
- `prior_snapshot_id`

## Snapshot Completeness Requirements

Every snapshot must pin:

- compiled graph identity
- graph hash
- interpreter version
- approval references
- evaluation references
- predecessor snapshot reference
- rollback-ready predecessor state

## Runtime Rule

All governed runtime actions must attach `snapshot_id`.

Runtime consumers must resolve policy, bounds, and dependencies from the active snapshot and compiled graph, not from mutable source files.

## Activation Preconditions

Activation must fail if:

- any required predicate fails
- required audit evidence is missing
- rollback-ready predecessor is missing
- interpreter version is missing or inconsistent
- graph hash is missing
- graph dependencies are unresolved

## Immutability

An activation snapshot is immutable once written.

Changing the active snapshot means activating a different immutable snapshot, not mutating an existing one.
