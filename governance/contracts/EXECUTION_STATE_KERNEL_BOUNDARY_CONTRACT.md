# Execution State Kernel Boundary Contract

Contract ID: `EXECUTION_STATE_KERNEL_BOUNDARY_CONTRACT_V1`

## Purpose

Define the smallest governed lifecycle-truth kernel that may advance post-submit execution state.

## Canonical Kernel Shape

The execution-state kernel is limited to:

- explicit lifecycle input assembly from durable submission authority and downstream evidence
- pure lifecycle transition decision
- immutable `ExecutionStateRecord`
- immutable lifecycle run envelope

## Sole Lifecycle Authority Surface

`ExecutionStateRecord` is the only durable lifecycle-truth surface for operators and downstream readers.

`ExecutionSubmissionRecord` remains submission authority only. It must not become long-lived execution-state truth.

## Allowed Evidence Sources

- `broker_submission_record.v2.json`
- `execution_event_record.v1.json`
- `fill_ledger.v1.json`
- pre-submit `HANDOFF_ENTERED` attempt evidence already written by the existing hard submit boundary

No dashboard, report, or reconciled-trade projection may act as lifecycle authority.

## Transition Boundary

Lifecycle advancement may evaluate only:

- valid `ExecutionSubmissionRecord` linkage
- current authoritative `ExecutionStateRecord`, if one exists
- explicit downstream evidence identity and hashes
- duplicate evidence detection
- legal/illegal lifecycle progression
- terminal-state protection

Contradictory or regressive lifecycle evidence must fail closed.

## Frozen Truth Rule

Every lifecycle state advance must produce a frozen `ExecutionStateRecord`.

The current authoritative lifecycle truth for one submission is the record with the highest governed transition index.

## Run Evidence Rule

Every lifecycle-processing run must emit a lifecycle run envelope, including blocked, duplicate, and successful advances.

If lifecycle envelope persistence fails, the lifecycle-processing run must fail closed.

## Forbidden Expansion

This kernel must not become:

- a broker redesign
- a generalized event platform
- a dashboard/reporting authority layer
- a second submission authority path
- a broad reconciliation platform
