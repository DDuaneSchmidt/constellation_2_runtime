# Paper Multi-Intent Execution Plan v1

## Summary
This stage adds a disabled-by-default PAPER multi-intent planning contract. It does not wire runtime execution, submit orders, enable transmit, create approvals, clear kill switches, or change the existing one-selected-intent path.

## Governed Defaults
The governed policy is `governance/02_REGISTRIES/C2_PAPER_MULTI_INTENT_EXECUTION_POLICY_V1.json`.

Default values:
- `multi_intent_paper_execution_enabled=false`
- `shadow_multi_intent_plan=false`
- `max_paper_intents_per_run=1`
- `max_total_paper_risk_cents=0`
- `correlation_regime_suppression_retained=true`

The validator rejects enabled multi-intent execution for this stage. Future execution requires a separate governed change.

## Artifact
`paper_multi_intent_execution_plan_v1` is a pure offline plan artifact. It records:
- selected candidates
- rejected candidates
- total risk
- per-intent risk
- correlation buckets
- authorization status
- kill-switch status
- submit-boundary status

The artifact schema is `governance/04_DATA/SCHEMAS/C2/REPORTS/paper_multi_intent_execution_plan.v1.schema.json`.

## Default Behavior
When multi-intent execution and shadow planning are disabled, the plan selects at most the current selected intent and marks `selected_intent_pointer_authoritative=true`. Existing `selected_intent_pointer.v1.json` remains authoritative.

## Shadow Behavior
If a future governed policy enables `shadow_multi_intent_plan=true`, the planner may produce a non-authoritative candidate plan for comparison. It still sets `order_submission_attempted=false` and cannot submit anything.

## Future Execution Boundary
Future multi-intent PAPER execution is not implemented.

When it is designed later, the intended Phase 5 model is distributed multi-intent PAPER execution across scheduled runs, not simultaneous basket execution.

Phase 5 must preserve these rules:
- max 1 new PAPER entry per scheduled run
- max 1 new PAPER entry per sleeve per day
- max daily entries equals the number of enabled executable sleeves, currently 7
- once a sleeve receives a PAPER entry, it is excluded from new entries for the rest of the day
- each run ranks remaining unused sleeves and candidates
- each selected entry must independently pass risk, authorization, kill-switch, and submit-boundary gates
- the aggregate daily PAPER risk cap still applies
- unsupported paired and signal-only sleeves remain non-executable until explicitly supported
- no simultaneous multi-order basket execution

Recommended future 7-run PAPER schedule:
- 09:31
- 10:15
- 11:00
- 11:45
- 13:30
- 14:30
- 15:30

Before any distributed PAPER entry can exist, each selected candidate must satisfy:
- authorization PASS per intent
- kill switch inactive
- submit boundary READY
- broker transmit explicitly enabled
- aggregate risk PASS
- unsupported paired execution blocked

## Non-Goals
This stage does not add:
- runtime hooks
- broker calls
- order submission
- transmit enablement
- approval creation
- kill-switch mutation
- authorization changes
- live behavior
