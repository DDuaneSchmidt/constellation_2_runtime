# Multi-Delta Execution Record Contract

## Purpose

Freeze the sole durable set-level execution authority for one coherent multi-trade rebalance attempt.

## Sole-authority rule

`MultiDeltaExecutionRecord` is the only artifact allowed to authorize creation of `ExecutionSetIntent` for a multi-delta rebalance.

No raw list, `PromotionDecisionV1`, or `PromotionRecordV2` may directly feed multi-trade handoff.

## Required contents

The record must freeze:

- exact ordered tradable member set
- upstream advisory and promotion lineage
- governed value-basis linkage
- deterministic sizing inputs
- deterministic ordering inputs
- stable idempotent set identity
- explicit set-level status

## Mutation rule

The record is immutable.

Any change to ordered membership, sizing, or governed value-basis lineage requires a new record identity.

## Set-level truth rule

The record must support answering:

- what trades belong to this rebalance set
- what ordered handoff members were derived from it
- what per-trade execution identities belong to the set

It may rely on linked per-trade submission and lifecycle truth for downstream progress, but it remains the authoritative membership root.

## Blocked-state rule

Blocked or duplicate decisions may not create an authorized execution record.

## Out of scope

This record does not replace:

- per-trade submission truth
- per-trade lifecycle truth
- execution evidence truth
