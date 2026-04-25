# Execution-Set Intent Contract

## Purpose

Freeze the governed handoff artifact that deterministically maps one authorized multi-delta execution record into an ordered sequence of per-trade execution intents compatible with the existing execution submission kernel.

## Required relationship

`ExecutionSetIntent` may be created only from one valid `MultiDeltaExecutionRecord`.

It must preserve lineage back to:

- `ApprovedChangeSet`
- `PromotionRecordV2`
- governed value basis

## Member-intent rule

`ExecutionSetIntent` must contain or deterministically derive the full ordered member `ExecutionIntentV1` payload sequence.

Each member must preserve:

- member order
- side
- quantity
- routing scope
- lineage to the set-level record
- deterministic idempotency
- deterministic predicted submission identity

## Adapter rule

If the existing execution submission kernel remains one-intent-at-a-time, the only allowed adapter is a governed deterministic sequencer that consumes `ExecutionSetIntent` and invokes the existing per-trade submission path in set order.

That adapter must not become alternate authority.

## No-bypass rule

Raw ad hoc multi-trade lists may not feed submission directly.

The only allowed multi-trade handoff path is:

`MultiDeltaExecutionRecord -> ExecutionSetIntent -> ordered member ExecutionIntentV1 -> existing submission kernel`

## Set-level traceability rule

`ExecutionSetIntent` must make it possible to trace:

- which member intents belong to the set
- which predicted submissions belong to the set
- how per-trade submission and lifecycle truth link back to the set

## Out of scope

This contract does not authorize:

- new broker pathways
- parallel unordered submission
- hidden trade reordering
- strategy re-interpretation
