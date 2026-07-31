# Aegis Research Follow-Through Control Design v1

## Design

The producer consumes these backend artifacts:

- `aegis_research_quality_engine_v1`
- `aegis_hypothesis_decision_policy_v1`
- `aegis_research_allocation_recommendation_v1`
- `aegis_hypothesis_workflow_state_v1`

Rows are joined by `hypothesis_id`. The producer maps each quality recommendation into exactly one follow-up item using deterministic precedence: data resolution, repair investigation, paper tracking flow watch, candidate flow watch, sample accumulation watch, then automatic monitoring.

## State Derivation

Follow-through state is derived from current backend evidence only. It does not create data sources, run repairs, generate candidates, mutate allocation weights, or retire hypotheses. It records whether the next required research loop is open, watching, stalled, or ready for David action.

## Re-Score Trigger

Every follow-up includes a `re_score_trigger` field. The trigger is advisory only: downstream operators or scheduled workflows may run the research quality engine again after data connection, candidate flow, sample accumulation, repair completion, or retirement review evidence appears.

## UI Boundary

The Research page renders follow-through rows from the backend artifact only. The UI must not infer follow-up type, status, blocking reason, next action, or David-action requirement.

## Safety Boundary

The design preserves all existing Aegis safety gates. It cannot enable trade advice, broker execution, live trading, real capital, autonomous execution, allocation mutation, code repair, order management, or real-world position management.
