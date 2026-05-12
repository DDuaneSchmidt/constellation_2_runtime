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
Future multi-intent PAPER execution is not implemented. Before it can exist, each selected candidate must satisfy:
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
