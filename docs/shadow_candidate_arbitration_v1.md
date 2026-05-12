# Shadow Candidate Arbitration v1

## Purpose

`shadow_candidate_arbitration_v1` ranks the candidate rows emitted by Phase 1 candidate observability artifacts without changing production arbitration or execution behavior.

The artifact is diagnostic only. It compares the shadow-ranked winner with the current production selected intent and records whether they match.

## Inputs

- `candidate_generation_manifest_v1`
- `sleeve_invocation_ledger_v1`
- `portfolio_scoring_v1`, when available
- current `intent_arbitration_v1` / `selected_intent_pointer`, when available

## Output

The output artifact is:

`reports/shadow_candidate_arbitration_v1/<day_utc>/<run_id>/shadow_candidate_arbitration.v1.json`

It includes:

- all candidate-manifest rows in deterministic diagnostic order
- executable shadow ranks only for candidates that remain executable
- suppressed, signal-only, blocked, and no-signal candidates as non-executable diagnostics
- shadow winner
- production selected intent
- winner match flag
- score delta and gate-reason diagnostics

## Safety Boundaries

The artifact is non-authoritative:

- `non_authoritative=true`
- `execution_authority_granted=false`
- `order_submission_attempted=false`
- `trading_behavior_changed=false`

It does not update `selected_intent_pointer`.
It does not modify `intent_arbitration_v1`.
It does not submit orders.
It does not enable transmit.
It does not clear kill switches.
It does not create approvals.
It does not wire multi-intent execution.

## Missing or Stale Inputs

Missing or stale candidate manifests or sleeve ledgers produce `status=DIAGNOSTIC_UNAVAILABLE`.

That state is fail-closed for diagnostics only and grants no execution authority.

## Current Integration Status

This module is standalone. It must be invoked explicitly by future governed tooling or a later observability bundle. Production arbitration and selected-intent behavior remain unchanged.
