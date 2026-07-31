# Aegis Command Center Visual Design Requirements V1

## Purpose

The Aegis Command Center must read as a paper-research operating center, not a dim system terminal. This document governs visual design, readability, and layout only. It does not authorize backend architecture changes, new registries, new orchestrators, new research subsystems, workflow changes, trade advice, manual capture, broker execution, live trading, or autonomous execution.

## Product Framing

Aegis is in PAPER MODE. Paper observations are research observations, not investment decisions. The primary system of record for the Command Center is Paper Trading plus Hypothesis Validation. Aegis Lite is legacy compatibility evidence only and must not be a primary Command Center concept.


## Truth Source Authority

Every primary Command Center field must be sourced from its authoritative report, not from cached operator-state generation time, browser fallback math, or local page interpretation. If the authoritative report is missing, the field must fail closed as unavailable or blocked; it must not display a stale fallback value.

Primary field ownership:

- Aegis mode/status: operator safety policy envelope and runtime truth, displayed as PAPER MODE / Paper Research Mode / Runtime Guarded, never Monitoring only as Command Center chrome.
- Raw signals: `aegis_candidate_generation_diagnostics_v1`.
- Valid candidates: `aegis_candidate_contracts_v1` through the candidate diagnostics projection.
- Auto-promoted: `aegis_candidate_to_paper_lifecycle_v1`.
- Open observations and closed outcomes: `aegis_outcome_registry_v1`.
- Closed today and manual review queue: `aegis_paper_outcome_auto_closure_v1`.
- Included samples: `aegis_validation_samples_v1`.
- Missing entry marks: `aegis_entry_reference_price_certification_v1`.
- David action: `aegis_command_center_queue_audit_v1`.
- Research allocation decisions and allocation state: `aegis_research_capital_allocation_v1`.
- Current bottleneck: `aegis_statistical_sufficiency_v1` plus validation/outcome evidence.
- Last successful run and next scheduled run: `aegis_paper_session_ledger_v1`; past runs must not display as next scheduled runs.
- Latest material change: latest material research evidence, such as paper outcome auto-closure or validation sample generation.

The Command Center API must expose a truth audit for these fields with displayed label, displayed value, authoritative report source, source path, source value, and match status.

## Primary Design Goals

1. Readability first.
2. Strong contrast.
3. Clear visual hierarchy.
4. Less dark-on-dark layering.
5. Less faint gray text.
6. Fewer oversized cards with sparse information.
7. Important numbers and statuses readable at a glance.
8. PAPER MODE visually dominates.
9. Safety remains visible but secondary.
10. The page feels like a research command center, not a dim system terminal.

## Typography And Color

- Primary labels use high-contrast text and medium font weight.
- Important statuses and metric values use bold weight.
- Body text uses normal or medium weight and must remain readable on the dark shell background.
- Faint gray is not allowed for primary labels, primary numbers, primary statuses, or current bottleneck copy.
- All-caps text is limited to short status badges such as PAPER MODE.
- Primary information must not rely on pale blue-gray text.
- Text hierarchy must come from contrast, grouping, and spacing before font size.

## Background And Panels

- The Command Center uses a brighter, higher-contrast paper-research surface over the dark shell.
- Primary panels must have clearer separation through lighter backgrounds, stronger borders, and restrained shadows.
- Avoid dark translucent cards on a dark page for primary content.
- Avoid large empty cards with one or two isolated values.
- Summary panels are compact, dense, and easy to compare.
- Panel content should be table-like where that improves scanning.

## Header Requirements

The header must show these concepts prominently:

- PAPER MODE
- Research observations only
- No live trading
- No broker execution
- Last successful run

PAPER MODE is the dominant visible status. Production and Monitoring only must not be visible primary statuses in the Command Center. Paper Research Mode may appear in shell chrome as the view environment label.

## Primary Summary Layout

The above-the-fold summary uses compact high-contrast panels, not oversized sparse cards.

Required panels:

- Candidate Generation: raw signals, valid candidates, auto-promoted count.
- Paper Observations: open, usable, blocked, missing entry marks.
- Outcome Validation: open outcomes, closed outcomes, included samples.
- Current Bottleneck: one plain-English bottleneck.
- David Action: whether action is required.
- Research Allocation: allocation decision count or No allocation decisions yet.

## Validation Pipeline

The pipeline remains visible and compact:

Raw Signals -> Valid Candidates -> Auto-Promoted -> Usable Observations -> Blocked -> Closed Outcomes -> Included Samples

Pipeline requirements:

- Values are high contrast.
- Labels are readable.
- The pipeline fits in one row on desktop when possible.
- Steps use compact connected boxes or step cells.
- Faint labels are not allowed.

## Activity Section

The main Command Center must not show a long chronological feed. The activity section shows only:

- Last successful run
- Next scheduled run
- Latest material change
- View full activity log

## Safety Policy

Safety policy remains secondary and collapsed. It must show:

- Live trading disabled
- Broker execution disabled
- Autonomous execution disabled
- Trade advice disabled
- Manual capture disabled

Safety policy must not visually dominate candidate generation, paper observations, validation, current bottleneck, David action, or research allocation.

## Left Navigation

The left navigation remains, but readability must improve:

- Secondary labels use increased contrast.
- Active navigation is obvious through border/background contrast.
- Monitoring only must not dominate the Command Center experience or be presented as the Command Center primary status.

## Forbidden Primary UI Outcomes

The primary Command Center layout must not show:

- Monitoring only
- Trade Recommendation
- Manual Capture
- Estimated Value
- UNKNOWN
- Aegis Lite terminology
- Long scrolling activity feed

Lower-level diagnostics may retain policy evidence where necessary, but the primary layout must stay paper-research oriented.

## Verification Requirements

Rendered UI or source tests must verify:

- PAPER MODE is visible and primary.
- Monitoring only is not visible in the primary Command Center region.
- Trade Recommendation is absent from the primary layout.
- Manual Capture is absent from the primary layout.
- Estimated Value is absent from the primary layout.
- Candidate Generation, Paper Observations, Outcome Validation, Current Bottleneck, David Action, and Research Allocation are visible.
- Safety policy is present only as secondary/collapsed content.
- High-contrast Command Center classes are applied to primary labels and values.
- `npm run aegis:audit` reports verified graph READY and audit blockers 0.
- Safety gates remain disabled.


## Operator Decision Dashboard v1 Amendment
The Command Center primary purpose is an operator decision dashboard, not a system monitoring dashboard. The top of the page must lead with operator answers, not raw system metrics.

Required primary section order under PAPER MODE:

1. TODAY'S RESEARCH RESULT
2. DAVID ACTIONS
3. GENERATED HYPOTHESIS PROGRESS
4. VALIDATION PROGRESS
5. CURRENT BOTTLENECK
6. Detailed research metrics below the fold

Authoritative primary sources are `aegis_research_daily_scorecard_v1`, `aegis_operator_action_queue_v1`, `aegis_hypothesis_workflow_state_v1`, `aegis_generated_hypothesis_throughput_v1`, `aegis_oil_shock_candidate_flow_v1`, `aegis_research_quality_engine_v1`, `aegis_hypothesis_decision_policy_v1`, `aegis_research_allocation_recommendation_v1`, `aegis_research_follow_through_control_v1`, `aegis_ai_research_intelligence_summary_v1`, and verified runtime graph.

The following are demoted below the five decision sections: raw candidate generation card, open observations card, validation pipeline strip, paper promotion card, detailed last run summary, and detailed safety policy.

Forbidden primary labels include Production, Monitoring only, vague Needs review without action buttons, Trade Recommendation, Manual Capture, UNKNOWN, and Values incomplete.
