# Aegis Research Daily Scorecard Spec v1

## Artifact
`aegis_research_daily_scorecard_v1` is written to `reports/aegis_research_daily_scorecard_v1/{day}/research_daily_scorecard.v1.json`.

## Top-Level Fields
- `target_day`
- `latest_run_time`
- `last_completed_run_type`
- `run_status`: `RAN`, `PARTIAL`, `FAILED`, or `NOT_RUN`
- `daily_progress_status`: `PROGRESS`, `NO_PROGRESS`, `BLOCKED`, or `ACTION_REQUIRED`
- `david_action_count`
- `primary_message`
- `primary_bottleneck`
- `generated_at_utc`

## Daily Delta Metrics
- `new_raw_signals`
- `new_valid_candidates`
- `new_auto_promoted_observations`
- `new_paper_observations`
- `new_closed_outcomes`
- `new_included_validation_samples`
- `new_manual_review_items`
- `new_hypothesis_proposals`
- `generated_hypotheses_advanced`
- `generated_hypotheses_blocked`

## Validation Progress
The `validation_progress` object includes total and delta counts for included validation samples and closed outcomes, hypotheses reaching sufficiency, hypotheses still underpowered, closest hypothesis to sufficiency, and samples needed for next sufficiency.

## Generated Hypothesis Progress
Each row includes hypothesis name, yesterday state when available, today state, state change boolean, throughput status, next expected step, blocker, and David action requirement.

## David Action Summary
The scorecard must include action count, action types, top action, action message, and exact button needed if one is present in source artifacts. It must not emit vague `needs review` text.

## Health / Blocker Summary
The scorecard must include audit status, audit blocker count, producer failures, stale artifact warnings, missing entry marks, missing current marks, workflow replay status, and safety gate change status.

## Decision Rules
1. `PROGRESS` if included validation samples, closed outcomes, paper observations, valid candidates, or generated hypothesis state advancement increased since the prior comparable run.
2. `ACTION_REQUIRED` if David action count is greater than zero.
3. `BLOCKED` if audit is not READY, audit blockers are present, workflow replay fails, stale primary-truth artifacts exist, or a required producer failed.
4. `NO_PROGRESS` if the run completed successfully but no meaningful research delta occurred.

Decision precedence is `BLOCKED`, then `ACTION_REQUIRED`, then `PROGRESS`, then `NO_PROGRESS`.

## Safety Flags
Every artifact must set research-only and no-trading safety flags to true or disabled values and must state that allocation and workflow mutation are not performed.

## Oil Shock Candidate-Flow Blocker Precedence
For Oil Shock generated-hypothesis progress, the authoritative blocker source is `aegis_oil_shock_candidate_flow_v1`. The scorecard must copy Oil Shock `candidate_flow_status`, `exact_blocker`, `reason_codes`, `ui_message`, `david_action_required`, and `next_expected_step` from that artifact when it is present. Generic generated-hypothesis throughput and workflow state may provide the row shell, but they must not infer, translate, or override the Oil Shock blocker.

If `aegis_oil_shock_candidate_flow_v1.reason_codes` contains `PRODUCER_MISSING`, the scorecard row must show `blocker: PRODUCER_MISSING`, message `Oil Shock deterministic candidate producer is missing.`, David action required `false`, and next expected step `implement/run deterministic Oil Shock producer`. The scorecard must not display `MISSING_DATA` for Oil Shock unless `aegis_oil_shock_candidate_flow_v1` itself reports `MISSING_DATA`.

## Data Action Routing

The scorecard consumes `aegis_data_action_routing_v1` when present and attaches these fields to generated-hypothesis missing-data rows:

- `data_action_classification`
- `data_action_owner`
- `missing_data_description`
- `data_action_next_step`
- `data_action_source`

For `MISSING_DATA`, the scorecard must preserve the distinction between David-owned data work and non-David system/market-data waits. Macro Calendar routes to `OPERATOR_PROVIDED_DATA_REQUIRED` with David action required. Oil Shock routes to a non-David classification unless an explicit Oil Shock data-source action exists.
