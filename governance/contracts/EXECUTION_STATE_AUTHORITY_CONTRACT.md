# Execution State Authority Contract

Contract ID: `EXECUTION_STATE_AUTHORITY_CONTRACT_V1`

## Purpose

Define `ExecutionStateRecord` as the sole durable lifecycle-truth artifact for one governed submission.

## Required Lineage

Every `ExecutionStateRecord` must carry at minimum:

- `submission_record_id`
- `submission_id`
- `execution_intent_id`
- `transition_index`
- `previous_execution_state_record_id` when not the first state
- explicit evidence refs
- explicit lifecycle status
- explicit terminal-state indicator
- explicit timestamps

## Allowed Lifecycle States

This contract version allows only:

- `HANDOFF_ENTERED`
- `UNKNOWN`
- `PENDINGSUBMIT`
- `PRESUBMITTED`
- `SUBMITTED`
- `ACKNOWLEDGED`
- `OPEN`
- `PARTIALLY_FILLED`
- `FILLED`
- `REJECTED`
- `CANCELLED`
- `INACTIVE`

## Transition Rules

- lifecycle transitions must be explicit and monotonic
- duplicate evidence must not create contradictory second truth
- terminal states may not regress to working states
- same effective lifecycle state may advance only when supported by stronger or new deterministic evidence
- contradictory broker identifiers, fill quantities, or average fill prices must fail closed

## Evidence Semantics

Evidence precedence for this contract version is:

1. `fill_ledger.v1.json`
2. `execution_event_record.v1.json`
3. `broker_submission_record.v2.json`
4. pre-submit `HANDOFF_ENTERED` record

`reconciled_trade_state_v1` remains a derived surface only and is not lifecycle authority.

## Replay Expectations

Same submission authority plus the same frozen lifecycle evidence set must produce the same lifecycle decision and the same `ExecutionStateRecord`.

## Run Evidence

Every lifecycle-processing run must emit an immutable lifecycle run envelope that links:

- lifecycle input identity
- lifecycle decision
- resulting `ExecutionStateRecord` when advanced
- blocked or duplicate reason when not advanced
