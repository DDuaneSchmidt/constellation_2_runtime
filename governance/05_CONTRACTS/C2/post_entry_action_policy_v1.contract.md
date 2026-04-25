---
id: C2_POST_ENTRY_ACTION_POLICY_CONTRACT_V1
title: "C2 Post Entry Action Policy Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_action_policy
---

# C2 Post Entry Action Policy Contract V1

## Policy basis owner

This contract is the governed action-policy basis for Core 3.

It does not own reconciled trade truth or broker transmission.

## Governed action taxonomy

- `HOLD`
- `ADD_INITIAL_PROTECTION`
- `AMEND_PROTECTION`
- `REDUCE_POSITION`
- `CLOSE_POSITION`
- `CANCEL_ORPHAN_CHILD`
- `OPERATOR_REVIEW_REQUIRED`
- `BLOCK_ALL_ACTIONS`

## Action semantics

### `ALLOWED`

The action may be taken autonomously if it survives conflict resolution and the final posture remains autonomous.

### `REQUIRED`

The action must be surfaced as the governed next action because policy deems the state unsafe or incomplete without it.

### `FORBIDDEN`

The action is incompatible with the current governed policy basis even if the gate is actionable.

### `BLOCKED`

The action is blocked by gate posture, missing preconditions, or unresolved conflict.

### `REVIEW_REQUIRED`

This is an explicit non-autonomous posture. It is not equivalent to `ALLOWED`.

## Preconditions and policy rules

### `HOLD`

- Allowed when gate is `ACTIONABLE` and lifecycle remains open or working-entry.
- Allowed when gate is `DEGRADED_REVIEW_REQUIRED` only as a hold-safe derived posture.

### `ADD_INITIAL_PROTECTION`

- Required when gate is `ACTIONABLE`, ownership is `CONSTELLATION_OWNED`, lifecycle is an open position, and `protection_status == PROTECTION_NOT_OBSERVED`.
- Blocked when lifecycle is flat or closed.

### `AMEND_PROTECTION`

- Allowed when gate is `ACTIONABLE`, lifecycle is an open position, and `protection_status == PROTECTION_WORKING_PRESENT`.
- Forbidden when no protection is working.

### `REDUCE_POSITION`

- Allowed when gate is `ACTIONABLE`, lifecycle is an open position, current quantity is non-zero, and a working order opposite the current side exists at quantity strictly below full close quantity.
- Forbidden when quantity is flat or opposite-side reduction evidence is absent.

### `CLOSE_POSITION`

- Allowed when gate is `ACTIONABLE`, lifecycle is an open position, and current quantity is non-zero.
- Forbidden when lifecycle is flat or closed.

### `CANCEL_ORPHAN_CHILD`

- Required when gate is `ACTIONABLE` and orphan child facts are present.
- Blocked when orphan ownership is ambiguous.

### `OPERATOR_REVIEW_REQUIRED`

- Required when gate is `DEGRADED_REVIEW_REQUIRED`.
- Required when actionable inputs still leave unresolved policy ambiguity.

### `BLOCK_ALL_ACTIONS`

- Required when gate is `BLOCKED`.

## Policy blocker taxonomy

At minimum Core 3 must preserve:

- `ACTION_PRECONDITION_UNMET`
- `ACTION_CLASS_UNSUPPORTED`
- `PROTECTION_POLICY_UNRESOLVED`
- `CLOSE_POLICY_UNRESOLVED`
- `REQUIRED_POLICY_BASIS_MISSING`
- `DEGRADED_TRUTH_HOLD_ONLY`
- `DEGRADED_TRUTH_FORBIDS_AUTONOMOUS_MODIFICATION`

## Policy basis references

Every required, forbidden, or blocked action outcome must carry an explicit reference to:

- this policy contract
- the conflict-resolution contract
- the gate contract when gate posture constrained the action
