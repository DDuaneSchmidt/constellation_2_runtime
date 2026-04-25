# Execution Kernel Boundary Contract

## Purpose

Define the smallest operational execution kernel for Constellation.

This kernel starts only from an already-authorized upstream execution artifact and drives the existing paper-trading path without recreating advisory authority, broker strategy, or a broader workflow platform.

## Canonical execution chain

The only valid execution chain under this contract is:

`ExecutionIntent -> SubmissionDecision -> SubmissionRecord -> execution_package.v1.json -> submit_boundary_paper_v4 -> durable execution truth`

Every execution-kernel run must also emit one immutable `ExecutionRunEnvelope`.

For governed multi-delta rebalance sets, a narrow set-level extension is permitted ahead of the existing per-trade submission chain:

`ApprovedChangeSet -> MultiDeltaExecutionDecision -> MultiDeltaExecutionRecord -> ExecutionSetIntent -> ordered member ExecutionIntentV1 -> SubmissionDecision -> SubmissionRecord -> execution_package.v1.json -> submit_boundary_paper_v4 -> durable execution truth`

## Required role ownership

- `ExecutionIntent` is the required upstream authorized per-trade execution artifact.
- `ApprovedChangeSet`, `MultiDeltaExecutionDecision`, `MultiDeltaExecutionRecord`, and `ExecutionSetIntent` are the required set-level authorities for governed multi-delta handoff only.
- `SubmissionDecision` is a pure gate result only.
- `SubmissionRecord` is the sole durable submission authorization authority.
- `execution_package.v1.json` remains the frozen downstream-submittable payload.
- `submit_boundary_paper_v4` remains the only paper-trading submit boundary.
- durable execution truth remains explicit under the runtime truth spine.
- `ExecutionRunEnvelope` is mandatory audit evidence for every run.

## Non-authority rule

The execution kernel must not recreate:

- advisory business logic
- portfolio construction
- promotion authority
- broker strategy
- retry orchestration platforms
- status dashboards or read models

Status and read models may derive from execution truth. They may not authorize execution.

## One-choke-point rule

All downstream execution consequence must pass through one hard choke point only:

`submit_boundary_paper_v4`

No direct, legacy, raw-candidate, or alternate execution path may remain capable of downstream consequence after cutover.

## Upstream authorization rule

No per-trade execution-kernel run may proceed without exactly one valid `ExecutionIntent`.

No multi-delta set-level run may proceed without exactly one valid `ExecutionSetIntent`.

No downstream submission may occur unless exactly one valid `SubmissionRecord` already exists first.

`SubmissionRecord` is the only authority artifact allowed to authorize handoff into the paper-trading boundary, except for a tiny explicit adapter that passes the frozen package path into the existing boundary.

## Frozen payload rule

The exact downstream-submittable payload must be sealed and frozen before submission.

For the current repo-proven path, that payload is:

- sealed `execution_package.v1.json`

No post-freeze mutation is allowed before boundary handoff.

## Idempotency and duplicate-prevention rule

Duplicate prevention must be durable and concurrency-safe.

At minimum the kernel must preserve a deterministic identity chain across:

- `ExecutionIntent.execution_intent_id`
- `SubmissionDecision`
- `SubmissionRecord.submission_id`
- `execution_package.v1.json.submission_id`
- downstream broker/request identity if returned
- durable execution state truth
- `ExecutionRunEnvelope.run_id`

The same authorized input must not create duplicate downstream consequence unless an explicit governed contract allows it.

## Fail-closed rule

If any of the following are missing, invalid, ambiguous, duplicate, superseded, or non-deterministic:

- `ExecutionIntent`
- `SubmissionDecision`
- `SubmissionRecord`
- sealed `execution_package.v1.json`
- mandatory lineage or identity alignment
- durable run/result evidence persistence

then:

- no downstream submission may occur
- the run must emit a durable blocked, duplicate, or rejected envelope if possible
- if mandatory evidence cannot be persisted, the run must fail closed before submission

## Durable execution truth rule

Durable execution truth must remain explicit and auditable under the runtime truth spine.

The kernel may reuse existing authoritative execution evidence families, but projections must not replace them.

At minimum the kernel must preserve auditable truth for:

- submission acceptance or rejection
- downstream request/order identifiers if returned
- lifecycle state
- timestamps
- fill or partial-fill state when available
- lineage back to `SubmissionRecord`

## Legacy path rule

Legacy or alternate execution entrypoints may survive only as:

- blocked wrappers
- read-only projections
- historical replay surfaces

They may not remain submit-capable.
