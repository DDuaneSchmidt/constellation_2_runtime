# Multi-Delta Execution Decision Contract

## Purpose

Freeze the pure set-level gate that determines whether an approved change set may enter durable set-level execution authority.

## Allowed outcomes

Allowed outcomes are:

- `promote`
- `no_action`
- `blocked`
- `duplicate`

## Pure-gate rule

`MultiDeltaExecutionDecision` may evaluate only:

- approved change-set validity
- set completeness
- deterministic ordering
- current policy trade-count limits already proven in upstream authority
- duplicate or idempotent set detection
- governed value-basis presence
- existing execution-contract compatibility

It must not:

- recompute advisory drift
- alter target allocation
- optimize trade choice
- reshape quantities
- reopen broker strategy

## Duplicate rule

Duplicate detection must be durable and deterministic.

If a set-level execution authority artifact already exists for the same governed input set, the decision must return `duplicate`.

## Fail-closed rule

Missing lineage, ambiguous membership, unsupported compatibility, or invalid ordering must yield `blocked`.

## Identity rule

The same frozen approved change set and duplicate state must yield the same decision identity and outcome.
