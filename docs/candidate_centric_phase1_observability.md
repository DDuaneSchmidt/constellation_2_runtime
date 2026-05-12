# Candidate-Centric PAPER Phase 1 Observability

## Summary
Phase 1 adds per-run candidate visibility only. It writes sleeve invocation evidence and candidate manifest artifacts, but it does not change arbitration, selected-intent behavior, submit behavior, authorization, kill-switch behavior, or trading policy.

## Artifacts
`sleeve_invocation_ledger_v1` records each evaluated engine and symbol or pair:
- run id
- day
- scheduled run timestamp
- engine id
- symbol or pair
- status
- reason codes
- input artifact paths
- output artifact paths
- lineage hash

`candidate_generation_manifest_v1` records candidate visibility:
- generated candidates
- no-signal rows
- blocked rows
- suppressed rows
- signal-only rows
- portfolio gate decision when available
- lineage hash

## Authority Boundary
Candidate visibility is not execution authorization.

The artifacts always carry:
- `selected_intent_pointer_authoritative=true`
- `execution_authority_granted=false`
- `order_submission_attempted=false`
- `trading_behavior_changed=false`

## Runtime Behavior
The current `intent_arbitration_v1` logic remains unchanged. `selected_intent_pointer.v1.json` remains the single authoritative selected intent for PAPER v1.

The candidate manifest may be enriched by `portfolio_activation_gate_v1` so suppressed and signal-only decisions are visible. This is diagnostic only and is not consumed by submit, authorization, kill-switch, or execution paths.

## Non-Goals
Phase 1 does not implement:
- shadow candidate arbitration
- multi-intent execution
- submit-boundary changes
- authorization changes
- kill-switch changes
- order submission
- transmit enablement
- policy changes
