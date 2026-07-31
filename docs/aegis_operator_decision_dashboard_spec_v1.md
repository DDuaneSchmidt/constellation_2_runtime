# Aegis Operator Decision Dashboard Spec v1

## Read Model
The Command Center consumes backend artifacts already attached to the operator payload. Browser code may format strings and counts, but must not infer workflow state, blockers, allocation decisions, or action requirements.

## Section 1: TODAY'S RESEARCH RESULT
Source: `aegis_research_daily_scorecard_v1`.

Fields:

- `daily_progress_status`
- `primary_message`
- `latest_run_time`
- `run_status`
- `daily_delta_metrics.new_valid_candidates`
- `daily_delta_metrics.new_paper_observations`
- `daily_delta_metrics.new_closed_outcomes`
- `daily_delta_metrics.new_included_validation_samples`
- `daily_delta_metrics.generated_hypotheses_advanced`
- `daily_delta_metrics.generated_hypotheses_blocked`

## Section 2: DAVID ACTIONS
Source: `aegis_research_daily_scorecard_v1.david_action_summary`, with source evidence from `aegis_operator_action_queue_v1` and queue artifacts where available.

Behavior:

- If count is positive, show one or more action cards with backend message and backend buttons.
- If no count, show `No David action required. Aegis will continue automatically.`
- Do not render `needs review` unless at least one explicit backend button is present in the same action section.

## Section 3: GENERATED HYPOTHESIS PROGRESS
Sources: `aegis_research_daily_scorecard_v1.generated_hypothesis_progress`, `aegis_generated_hypothesis_throughput_v1`, and `aegis_oil_shock_candidate_flow_v1` for Oil Shock blocker text.

Fields per row:

- `hypothesis_name`
- current workflow state from `today_state` or throughput row state
- `throughput_status`
- `next_expected_step`
- `blocker`
- `david_action_required`

Oil Shock rule: if the Oil Shock candidate-flow artifact supplies `exact_blocker`, the UI shows that exact value. It must not translate `PRODUCER_MISSING` to `MISSING_DATA` or any other label.

## Section 4: VALIDATION PROGRESS
Source: `aegis_research_daily_scorecard_v1.validation_progress`.

Fields:

- `included_samples_total`
- `included_samples_delta`
- `closed_outcomes_total`
- `closed_outcomes_delta`
- `closest_hypothesis_to_sufficiency`
- `samples_needed_for_next_sufficiency`
- `hypotheses_still_underpowered`

## Section 5: CURRENT BOTTLENECK
Source: `aegis_research_daily_scorecard_v1.primary_bottleneck` and health/validation/generated hypothesis fields from the same artifact, with Oil Shock exact blocker from `aegis_oil_shock_candidate_flow_v1` when Oil Shock is selected as the bottleneck.

Selection precedence is backend-driven by the scorecard. The browser may format the message but must not select a new bottleneck from raw metrics.

## Research Allocation Card
Source: `aegis_research_allocation_recommendation_v1.summary` and `recommendations`.

Fields:

- recommendation count
- HOLD count
- PAUSE count
- INCREASE count
- DECREASE count
- David review required count from `recommendations[].requires_david_review`

If recommendations exist, the card must not say `No allocation decisions yet`.

## Demoted Detail
The following stay below the five operator sections: raw candidate generation card, open observations card, validation pipeline strip, paper promotion card, detailed last run summary, detailed safety policy, and diagnostic evidence.

## Safety
The UI cannot enable broker execution, live trading, trade advice, real capital, autonomous execution, order management, paper observations, retirements, or allocation mutation.

### Oil Shock Blocker Precedence
For Oil Shock, `aegis_oil_shock_candidate_flow_v1` is the authoritative source for candidate-flow blocker display. Command Center must show `candidate_flow_status`, `exact_blocker`, `ui_message`, `david_action_required`, and `next_expected_step` from that artifact. It must not translate `PRODUCER_MISSING` to `MISSING_DATA`, and it must not use generated-hypothesis throughput or generic workflow state to replace an Oil Shock candidate-flow blocker.

Expected `PRODUCER_MISSING` display:

- Oil Shock candidate flow: `BLOCKED`
- blocker: `PRODUCER_MISSING`
- message: `Oil Shock deterministic candidate producer is missing.`
- David action required: `false`
- next expected step: `implement/run deterministic Oil Shock producer`

If Macro Calendar has missing data and Oil Shock has producer missing, the UI must show both separately: Macro Calendar as missing macro event calendar, and Oil Shock as producer missing.

## Data Action Routing Display

Command Center generated-hypothesis rows must show blocker, data-action classification, owner, David action yes/no, and next step from backend artifacts. The UI must not display two generic `MISSING_DATA` rows without explaining why only one is actionable.

Macro Calendar displays `OPERATOR_PROVIDED_DATA_REQUIRED`, `owner=DAVID`, and the David action buttons from the operator action queue. Oil Shock displays the explicit non-David owner/classification from `aegis_data_action_routing_v1` unless Aegis has created an explicit Oil Shock data-source action.
