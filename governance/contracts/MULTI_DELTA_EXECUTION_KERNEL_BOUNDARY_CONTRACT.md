# Multi-Delta Execution Kernel Boundary Contract

## Purpose

Define the narrow set-level execution kernel required for coherent multi-trade rebalance handoff.

This kernel exists only to turn one approved multi-change rebalance result into one deterministic, auditable, execution-safe ordered trade set without redesigning advisory authority, submission authority, or lifecycle authority.

## Canonical set-level chain

The only valid multi-delta execution chain under this contract is:

`PromotionRecordV2 -> ApprovedChangeSet -> MultiDeltaExecutionDecision -> MultiDeltaExecutionRecord -> ExecutionSetIntent -> ordered ExecutionIntentV1 members -> existing execution submission kernel`

No raw multi-trade list may bypass this chain.

## Boundary ownership

- `PromotionRecordV2` remains the sole upstream promotion authority.
- `ApprovedChangeSet` is the sole authoritative multi-change input to this kernel.
- `MultiDeltaExecutionDecision` is a pure set-level gate only.
- `MultiDeltaExecutionRecord` is the sole durable set-level execution authority.
- `ExecutionSetIntent` is the only governed handoff artifact allowed to emit ordered per-trade `ExecutionIntentV1` members.
- Existing `ExecutionSubmissionDecisionV1`, `ExecutionSubmissionRecordV1`, and `ExecutionStateRecordV1` remain authoritative for per-trade submission and lifecycle truth.

## Determinism rule

This kernel must preserve deterministic:

- membership
- ordering
- sizing
- lineage
- duplicate detection
- per-trade handoff sequence

The same frozen upstream inputs must yield the same set-level identities and the same member ordering.

## Fail-closed rule

If the kernel cannot prove a coherent set because of:

- missing or incomplete approved changes
- missing governed value basis lineage
- ambiguous ordering
- unsupported instrument or routing scope
- unsupported execution contract compatibility
- duplicate or superseded set state

then:

- no `MultiDeltaExecutionRecord` may authorize handoff
- no `ExecutionSetIntent` may be emitted
- no per-trade `ExecutionIntentV1` member may be submitted

## No-partial-adhoc rule

Partial ad hoc execution of a governed approved change set is forbidden.

If fewer than all ordered members can be derived into governed handoff members, the set must fail closed before handoff.

## Relationship to existing kernels

This kernel adds only the missing set-level execution truth.

It must not redesign:

- snapshot/value basis authority
- allocation advisory
- actionability
- execution submission kernel
- execution lifecycle kernel
- broker behavior

## Out of scope

This contract does not permit:

- generalized orchestration platforms
- retry engines
- optimization
- tax logic
- stochastic behavior
- broker-specific strategy logic
