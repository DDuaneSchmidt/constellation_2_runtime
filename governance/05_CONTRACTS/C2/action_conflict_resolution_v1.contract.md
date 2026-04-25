---
id: C2_ACTION_CONFLICT_RESOLUTION_CONTRACT_V1
title: "C2 Action Conflict Resolution Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_action_conflicts
---

# C2 Action Conflict Resolution Contract V1

## Purpose

This contract defines deterministic resolution when multiple candidate actions are simultaneously nominated.

## Candidate conflict classes

- gate-block conflict
- review-required conflict
- protection-close conflict
- reduce-close conflict
- orphan-cleanup conflict
- unsupported-action conflict

## Deterministic precedence

1. `BLOCK_ALL_ACTIONS` from a blocked gate wins immediately.
2. `OPERATOR_REVIEW_REQUIRED` from a degraded gate suppresses autonomous modifying actions.
3. `CANCEL_ORPHAN_CHILD` required cleanup outranks discretionary `HOLD`.
4. `ADD_INITIAL_PROTECTION` required protection outranks discretionary `CLOSE_POSITION`.
5. `CLOSE_POSITION` outranks `REDUCE_POSITION`.
6. `REDUCE_POSITION` outranks `HOLD`.
7. `AMEND_PROTECTION` outranks discretionary `HOLD` but loses to `CLOSE_POSITION`.

## Tie-break rules

If two candidates remain at the same precedence tier:

1. required outranks allowed
2. narrower safety repair outranks broader discretionary action
3. lexical sort on `action_code` resolves any remaining tie

## Contradiction elimination

The final authority must never publish incompatible final postures, including:

- `CLOSE_POSITION` and `REDUCE_POSITION` both required
- `ADD_INITIAL_PROTECTION` and `BLOCK_ALL_ACTIONS` both final required actions
- autonomous modifying action allowed while gate is `DEGRADED_REVIEW_REQUIRED`
- any autonomous action allowed while gate is `BLOCKED`

Conflicted candidates must move into explicit rejected or blocked status with rule-backed reasons.
