# Aegis Research UI Dumb Renderer Design V1

## Goal

The Research UI must stop inferring hypothesis workflow state. It renders backend artifacts only.

## Inputs

The UI consumes:

- `aegis_hypothesis_workflow_state_v1`
- `aegis_operator_action_queue_v1`

## Layout

The Research screen renders:

1. David Action Queue
2. Hypothesis Status Summary
3. Hypothesis List

If the action queue is empty, the UI shows: `No David action required. Aegis will continue automatically.`

## Button Rule

The UI must not render any action button unless that exact button appears in the backend queue item. It must not show primary labels `Production`, `Monitoring only`, `Trade Recommendation`, `Manual Capture`, or `UNKNOWN`.


## Command Center Dumb Renderer Extension
The Command Center follows the same dumb-renderer rule for Operator Decision Dashboard v1. It renders backend artifacts only and must not infer daily progress, David action requirements, generated-hypothesis blocker labels, Oil Shock blocker text, validation sufficiency, allocation recommendations, or safety state in browser code.

The top five sections render from `aegis_research_daily_scorecard_v1`, `aegis_operator_action_queue_v1`, `aegis_generated_hypothesis_throughput_v1`, `aegis_oil_shock_candidate_flow_v1`, `aegis_research_allocation_recommendation_v1`, and verified runtime graph as attached to the backend payload.


## Operator Decision Dashboard Section Order
The Command Center operator decision dashboard order is TODAY'S RESEARCH RESULT, DAVID ACTIONS, GENERATED HYPOTHESIS PROGRESS, VALIDATION PROGRESS, CURRENT BOTTLENECK, then detailed research metrics below the fold.
