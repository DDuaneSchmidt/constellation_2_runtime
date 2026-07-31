# Aegis Operator Decision Dashboard Requirements v1

## Purpose
The Command Center primary purpose is an operator decision dashboard, not a system monitoring dashboard. It must answer, in under 30 seconds after the 9:50 or 2:50 research run: what happened, whether Aegis made research progress, whether validation advanced, whether generated hypotheses advanced, what is blocked, and whether David needs to act.

## Scope
This package changes the Command Center UI/read model only. It must not change candidate generation, paper lifecycle, outcome validation, quality scoring, follow-through logic, broker behavior, trading behavior, allocation mutation, or safety gates.

## Required Section Order
The first visible Command Center content under PAPER MODE must be:

1. TODAY'S RESEARCH RESULT
2. DAVID ACTIONS
3. GENERATED HYPOTHESIS PROGRESS
4. VALIDATION PROGRESS
5. CURRENT BOTTLENECK
6. Detailed research metrics below the fold

## Above The Fold
Above the fold must contain the five operator decision sections. Detailed cards for raw candidate generation, open observations, validation pipeline, paper promotion, last-run detail, and safety policy are demoted below these five sections.

## Authoritative Sources
Visible primary fields must come from backend artifacts only:

- Daily result, latest run time, run status, deltas, validation progress, David action count/message/buttons, and primary bottleneck: `aegis_research_daily_scorecard_v1`.
- David action source evidence: `aegis_operator_action_queue_v1` and Command Center queue evidence where present; visible button text comes from backend action/button fields.
- Generated hypothesis names, states, throughput, next step, blocker, and David action flag: `aegis_research_daily_scorecard_v1` and `aegis_generated_hypothesis_throughput_v1`.
- Oil Shock blocker label: `aegis_oil_shock_candidate_flow_v1` exact blocker or reason code; the UI must not translate it.
- Validation sufficiency totals and needed count: `aegis_research_daily_scorecard_v1`, backed by validation samples/statistical sufficiency artifacts.
- Allocation count/action counts/David review requirement: `aegis_research_allocation_recommendation_v1`.
- Safety status: verified runtime graph and runtime safety policy artifacts.

## Forbidden Primary Labels
The primary Command Center region must not show `Production`, `Monitoring only`, vague `Needs review` without explicit action buttons, `Trade Recommendation`, `Manual Capture`, `UNKNOWN`, or `Values incomplete` as primary status labels. Lower diagnostic evidence may include raw policy vocabulary only below the operator sections.

## David Actions
If `action_count > 0`, the page must show exact action cards and exact buttons. For Macro Calendar on 2026-06-01 this is the macro event calendar data source action with Connect Source, Upload Dataset, and Defer visible. If no action exists, the section says: `No David action required. Aegis will continue automatically.`

## Generated Hypothesis Progress
Each generated hypothesis row must show hypothesis name, current workflow state, throughput status, next expected step, blocker if any, and David action required yes/no. Oil Shock must use `aegis_oil_shock_candidate_flow_v1` for blocker wording.

## Validation Progress
Validation progress must show included validation samples total/delta, closed outcomes total/delta, closest hypothesis to sufficiency, samples needed for next sufficiency, and hypotheses still underpowered.

## Bottleneck Wording
Current bottleneck must be one plain-English sentence. It must identify the specific issue, such as Macro Calendar missing macro event calendar, Oil Shock candidate producer missing, validation sample sufficiency for a named hypothesis and needed count, or audit not READY. Generic `Validation sample sufficiency / underpowered hypotheses` is not sufficient unless the hypothesis and needed count are also shown.

## Acceptance Tests
Tests must prove section order, backend-only rendering, exact David actions, Oil Shock blocker authority, allocation recommendation display, demotion of detailed metrics, primary-label exclusions, docs updates, and unchanged broker/trading/safety gates.
