# Execution Submission Contract

## Purpose

Freeze the minimal execution-kernel submission contract for the existing paper-trading path.

This contract governs:

- pure submission gating
- durable pre-submit authorization
- frozen package handoff
- durable post-submit truth linkage
- mandatory run/result evidence

## Proven downstream boundary

The repo-proven downstream intake remains:

- sealed `execution_package.v1.json`
- existing `submit_boundary_paper_v4`
- existing broker adapter boundary behind `IBPaperAdapterV2`

No redesign of paper trading or broker behavior is permitted under this contract.

## Required execution-kernel inputs

The execution kernel accepts exactly one upstream authorized per-trade execution artifact:

- `ExecutionIntent`

`ExecutionIntent` must already be authorized upstream. The execution kernel must not reopen advisory truth, promotion truth, or live household state.

For governed multi-delta rebalance sets, `ExecutionSetIntent` is the only permitted set-level adapter artifact, and it may feed this contract only by deterministically yielding one ordered sequence of member `ExecutionIntent` values.

## SubmissionDecision contract

`SubmissionDecision` must be:

- pure
- deterministic
- side-effect free
- derived only from frozen inputs and durable duplicate state

Allowed first-cut outcomes:

- `submit`
- `blocked`
- `duplicate`
- `no_action` only if explicitly supported by repo-proven semantics

`SubmissionDecision` may evaluate only:

- upstream execution-intent validity
- missing required executable fields
- stale or superseded authorization
- durable duplicate state
- proven downstream readiness prerequisites
- explicit governed contract gates already relevant to execution

`SubmissionDecision` must not:

- recreate advisory logic
- reshape orders
- own broker strategy
- own retry strategy

## SubmissionRecord contract

`SubmissionRecord` is the sole durable submission authority.

It must:

- be created only after a `submit` decision
- freeze lineage to exactly one `ExecutionIntent`
- freeze the exact sealed `execution_package.v1.json` path and hash
- freeze the deterministic `submission_id`
- carry explicit lifecycle status
- carry stable idempotency identity
- prevent or detect duplicate creation durably

No downstream submission may occur without a valid `SubmissionRecord`.

## Submission identity rule

The submission identity chain must remain deterministic:

- `ExecutionIntent.execution_intent_id`
- derived `trade_instance_id` if present
- `SubmissionRecord.submission_id`
- `execution_package.v1.json.submission_id`
- `broker_submission_record.v2.json.submission_id`
- `ExecutionRunEnvelope`

Independent ad hoc id generation is forbidden.

## Frozen-package rule

`SubmissionRecord` must freeze the exact downstream-submittable package before handoff:

- package path
- package canonical hash
- candidate path if needed for replay
- build artifact ref if needed for replay

The paper-trading boundary may receive only that frozen package, through a tiny explicit adapter from `SubmissionRecord`.

## Choke-point enforcement rule

The only valid submit-capable path after cutover is:

`ExecutionIntent -> SubmissionDecision -> SubmissionRecord -> execution_package.v1.json -> submit_boundary_paper_v4`

Direct raw-candidate submission is forbidden for kernel execution.

Direct raw multi-trade submission lists are also forbidden.

Legacy `c2_submit_paper_v1` through `c2_submit_paper_v4` and their underlying legacy boundaries are non-authoritative and must be blocked from downstream consequence.

## Durable execution truth rule

After handoff, the kernel must record or link durable execution truth for:

- accepted submission
- rejected submission
- downstream lifecycle state
- downstream broker identifiers if present
- fill state if present

Existing authoritative execution evidence families may be reused. Read models may not substitute for them.

## Envelope rule

Every execution-kernel run must emit one durable immutable `ExecutionRunEnvelope`.

It must reference:

- `ExecutionIntent`
- `SubmissionDecision`
- `SubmissionRecord` if created
- downstream handoff result if attempted
- durable execution truth refs if created or updated

If mandatory envelope persistence cannot be proven, submission must fail closed before handoff.
