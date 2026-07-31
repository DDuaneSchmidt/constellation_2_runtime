# Aegis Today / Command Center Product Specification

## Purpose

The rebuilt Today / Command Center screen is the daily operator starting point for Aegis.

It must answer, in plain English:

```text
Is Aegis okay today, what happened, what is open, what is waiting, what is blocked, do I need to do anything, and what happens next?
```

This is a product specification only. It does not define layout, wireframes, implementation, routing, or code.

## Product Principles

1. The visible UI must be workflow-first, not architecture-first.
2. The first visible message must answer whether Aegis is okay today.
3. Internal governance may inform the answer but must not be the answer.
4. The screen must clearly distinguish user action, system work, waiting for time, waiting for data, and monitoring-only states.
5. If data is stale, missing, contradictory, or wrong-day, the screen must fail closed in operator language.
6. Ask Aegis is supportive, not the main workflow.
7. Evidence is available after the core status is clear.
8. No trading, broker, live, autonomous, or trade-advice capability may be implied.


## Command Center Truth Source Authority

The Today / Command Center screen must not invent primary truth in the browser and must not use cached/operator-generated fallback state for visible primary fields. The `/api/aegis/operator/today` envelope owns mapping from authoritative reports to displayed fields. The browser may format values, but it must not substitute alternate counts or stale fallback statuses.

Required primary field sources:

- Aegis mode/status: operator safety policy envelope and runtime truth; Command Center chrome uses PAPER MODE / Paper Research Mode / Runtime Guarded, not Monitoring only.
- Raw signals: `aegis_candidate_generation_diagnostics_v1`.
- Valid candidates: `aegis_candidate_contracts_v1` through candidate diagnostics.
- Auto-promoted: `aegis_candidate_to_paper_lifecycle_v1`.
- Open observations, usable observations, blocked observations, and closed outcomes: `aegis_outcome_registry_v1` plus paper lifecycle blockers where applicable.
- Missing entry marks: `aegis_entry_reference_price_certification_v1`.
- Closed today and manual review queue: `aegis_paper_outcome_auto_closure_v1`.
- Included samples: `aegis_validation_samples_v1`.
- David action: `aegis_command_center_queue_audit_v1`.
- Research allocation decisions and allocation state: `aegis_research_capital_allocation_v1`.
- Current bottleneck: `aegis_statistical_sufficiency_v1`, validation samples, and outcome evidence; when outcomes are closing and samples exist, the bottleneck is validation sample sufficiency / underpowered hypotheses, not outcome closure throughput.
- Last successful run: the last completed paper/research session source, not operator-state generation time.
- Next scheduled run: future paper/research session evidence only; if none exists, display `No future run confirmed`.
- Latest material change: latest material research evidence such as paper outcome auto-closure or validation sample generation.

The API must include a Command Center truth audit row for each visible primary field: displayed label, displayed value, authoritative report source, source path, source value, and match status. A mismatch is a test failure.

## Primary Questions

### 1. Is Aegis okay today?

Required data:

* requested day
* source day
* current operator state
* runtime truth summary
* verified graph status
* current-day surface/readiness state for Today
* semantic consistency result for Today inputs
* safety gate summary

Source system:

* runtime truth kernel
* verified runtime graph
* canonical operator state
* operator surface/readiness gates as backend inputs
* audit handoff/control packet for safety gates

Update frequency:

* On page load.
* After every scheduled Aegis run.
* After manual refresh.
* After any runtime truth/audit rebuild.

Empty state:

```text
Aegis has no activity to report today. Monitoring is active.
```

Blocked state:

```text
Aegis cannot show a trustworthy current-day summary.
```

Must include reason and impact.

Degraded state:

```text
Aegis is readable, but some information is incomplete.
```

Must state what remains safe to use.

User action state:

If David action is required:

```text
David action required.
```

Must state the exact action count and list only true user actions.

### 2. What happened today?

Required data:

* scheduled run list for requested day
* last run time
* last successful run time
* failed/missed runs
* candidate generation status
* research run status
* market/mark update status
* performance build status

Source system:

* run history
* runtime timeline projection
* canonical operator state
* candidate generation diagnostics
* research doctor/status artifacts
* performance/P&L report status

Update frequency:

* After each scheduled run.
* After run history updates.
* On manual refresh.

Empty state:

```text
Nothing has run yet today.
```

Include next scheduled run.

Blocked state:

```text
Run history is unavailable, so Aegis cannot confirm what happened today.
```

Degraded state:

```text
Some run details are missing, but Aegis can still show the known activity.
```

User action state:

Only if a run requires David to perform a real manual step. Otherwise show system-side next step.

### 3. What is currently open?

Required data:

* open paper position count
* open symbols summary
* mark coverage
* unrealized P&L availability
* sleeve attribution coverage
* data quality for position source

Source system:

* paper position ledger/read model
* canonical market marks
* paper P&L report
* sleeve attribution recovery/sleeve analytics summary

Update frequency:

* After position ledger changes.
* After mark refresh.
* After P&L/performance build.

Empty state:

```text
No open paper positions are recorded today.
```

Blocked state:

```text
Open position state is unavailable.
```

Must not show stale or partial positions as current.

Degraded state:

```text
Open positions are visible, but marks or attribution are incomplete.
```

User action state:

No position trading action may appear on Today. The only allowed action is to open Positions or Position Review.

### 4. What is waiting?

Required data:

* next scheduled run
* workflows waiting for time
* workflows waiting for data
* research sample accumulation state
* market close/evidence observation windows
* system repair/waiting states

Source system:

* runtime timeline
* scheduler/run history
* research validation samples
* candidate pipeline status
* engineering priority queue if system-side waiting is relevant

Update frequency:

* On schedule changes.
* After research/sample status updates.
* After runtime timeline updates.

Empty state:

```text
Nothing is waiting right now.
```

Blocked state:

```text
Aegis cannot determine what is waiting because schedule or runtime state is unavailable.
```

Degraded state:

```text
Some waiting states are unknown.
```

User action state:

Waiting is not user action unless the workflow explicitly requires David.

### 5. What is blocked?

Required data:

* blocking issues affecting Today
* issue severity
* affected workflow
* reason
* impact
* next step
* whether blocker is user-side, system-side, data-side, or time-side

Source system:

* runtime truth kernel
* engineering priority queue
* surface readiness/semantic invariant results as backend inputs
* candidate/research/performance status summaries

Update frequency:

* After audit/runtime truth build.
* After repair/status producer updates.

Empty state:

```text
No blockers are currently reported.
```

Blocked state:

If the blocked-state source itself is unavailable:

```text
Aegis cannot verify blocker status.
```

Degraded state:

```text
Some blocker details are incomplete.
```

User action state:

Only show as David action if David must do something. System repair is not David action.

### 6. Do I need to do anything?

Required data:

* true user-action rows
* action type
* reason
* deadline/urgency if any
* allowed action controls
* action blocked status

Source system:

* command center queue audit
* candidate readiness/actionability
* operator command state
* manual capture command state
* research/user decision artifacts where applicable

Update frequency:

* After candidate generation.
* After command processing.
* After research status changes.
* On manual refresh.

Empty state:

```text
No David action required.
```

Blocked state:

```text
Aegis cannot verify whether David action is required.
```

In blocked state, no action buttons may render.

Degraded state:

```text
Aegis can show known actions, but some action checks are incomplete.
```

User action state:

Must show exact action, why it exists, and allowed next action. Do not show system repairs as David actions.

### 7. What happens next?

Required data:

* next scheduled activity
* next expected system workflow
* next expected market/data event
* next research observation if applicable
* next user action if one exists

Source system:

* schedule/runtime timeline
* run history
* research validation samples
* candidate pipeline state
* engineering priority queue

Update frequency:

* On page load.
* When schedule/run state updates.
* After any state transition.

Empty state:

```text
No next activity is scheduled.
```

Blocked state:

```text
Aegis cannot determine the next scheduled activity.
```

Degraded state:

```text
Aegis knows some next steps, but schedule details are incomplete.
```

User action state:

If David action exists, it should be the first next step. Otherwise show the next system/time event.

### 8. When is the next scheduled run?

Required data:

* next scheduled run time
* run label
* expected output
* time zone
* whether schedule is active
* whether current day is non-trading/historical

Source system:

* scheduler/run history
* runtime timeline
* paper session ledger

Update frequency:

* On schedule load.
* After each run.
* After manual refresh.

Empty state:

```text
No scheduled run is known.
```

Blocked state:

```text
Schedule state is unavailable.
```

Degraded state:

```text
Next run time is estimated from partial schedule data.
```

User action state:

No action unless schedule requires manual start, which must be explicit.

### 9. Can Aegis act or is it monitoring only?

Required data:

* trade advice allowed
* broker execution allowed
* broker submit/transmit allowed
* live trading allowed
* autonomous live trading allowed
* paper/manual capture policy
* current operating mode

Source system:

* runtime truth kernel
* audit handoff
* control packet
* mode readiness
* safety policy bundle

Update frequency:

* On page load.
* After audit/control packet rebuild.
* After policy changes.

Empty state:

Not allowed. If safety policy cannot be read, show blocked state.

Blocked state:

```text
Aegis cannot verify safety mode. Actions are disabled.
```

Degraded state:

Not preferred. Safety must be explicit; if uncertain, block.

User action state:

No broker/live/autonomous action is ever shown on Today.

### 10. Why should I trust today's state?

Required data:

* requested/source day match
* current evidence age
* verified graph/audit state
* runtime truth summary
* source freshness summary
* explicit limitations
* evidence availability

Source system:

* verified runtime graph
* audit handoff
* runtime truth kernel
* surface readiness/semantic invariants as backend inputs
* source data manifest

Update frequency:

* After audit.
* After runtime truth build.
* After artifact refresh.

Empty state:

```text
Evidence is not available for today's state.
```

Blocked state:

```text
Aegis cannot prove today's state. Treat the screen as unavailable.
```

Degraded state:

```text
Aegis can prove the main state, but some evidence is incomplete.
```

User action state:

Evidence review is optional unless a user action depends on it.

# Section 1: Executive Summary

## What Should Be Visible Above The Fold

The above-the-fold content must answer:

* Is Aegis okay today?
* Do I need to do anything?
* What is open?
* What is blocked or waiting?
* What happens next?
* Is Aegis monitoring only?

Required components:

1. Today status statement.
2. David action required statement.
3. Open positions count.
4. Current candidate state.
5. Blocked/waiting state.
6. Next scheduled activity.
7. Safety mode statement.
8. Trust/evidence summary.

## Proposed Components

### Component: Today Status Statement

Purpose: One sentence answering whether Aegis is okay today.
Operator question answered: Is Aegis okay today?
Value score: 10
Classification: NEW
Justification: Current UI has many status indicators but no dominant plain-English answer.

### Component: David Action Required Statement

Purpose: Say whether David must do anything now.
Operator question answered: Do I need to do anything?
Value score: 10
Classification: IMPROVE
Justification: Current attention queue exists but mixes system and user action semantics.

### Component: Open Positions Count

Purpose: Show whether paper exposure exists.
Operator question answered: What is currently open?
Value score: 9
Classification: KEEP
Justification: Current open position count is valuable and should remain above the fold.

### Component: Candidate State Summary

Purpose: Summarize candidate count/actionability without workflow detail.
Operator question answered: Are there candidates?
Value score: 8
Classification: IMPROVE
Justification: Current candidate displays are duplicated and too detailed.

### Component: Next Scheduled Activity Summary

Purpose: Show the next expected run or observation.
Operator question answered: What happens next and when?
Value score: 10
Classification: NEW
Justification: Current runtime timeline has this information but Today does not surface it cleanly.

### Component: Safety Mode Statement

Purpose: State whether Aegis can act or is monitoring only.
Operator question answered: Can Aegis act?
Value score: 10
Classification: IMPROVE
Justification: Safety data exists but appears in audit/evidence language.

# Section 2: Attention Required

This section appears only when true user action exists.

It must not appear for:

* system repair
* waiting for data
* waiting for time
* diagnostics-only warnings
* already captured rows
* monitor-only rows

Required data:

* user action count
* action rows
* reason for each action
* allowed action type
* urgency/severity
* source day/session match

Allowed user action examples:

* confirm manual IB capture
* mark candidate not captured
* defer candidate
* acknowledge a blocked state if policy requires acknowledgement
* review a research result only when operator decision is truly required

## Proposed Components

### Component: Attention Required Queue

Purpose: Show real David actions only.
Operator question answered: Do I need to do anything?
Value score: 10
Classification: IMPROVE
Justification: Current queue concept is valuable but must be semantically strict.

### Component: No Action Required Message

Purpose: Replace empty action queues with a clear statement.
Operator question answered: Do I need to do anything?
Value score: 9
Classification: NEW
Justification: Prevents operators from interpreting an empty list as a failure.

# Section 3: Today's Activity

This section answers what happened today.

Required data:

* run list summary
* last run time
* last successful run
* missed/failed runs
* candidate generation result
* research activity result
* performance/mark update status

This section must distinguish:

* ran successfully
* ran with no outputs
* did not run yet
* missed schedule
* failed
* not scheduled

## Proposed Components

### Component: Activity Timeline Summary

Purpose: Show compact sequence of today’s key events.
Operator question answered: What happened today?
Value score: 9
Classification: NEW
Justification: Runtime Timeline contains pieces, but Today needs a digest.

### Component: Last Run / Last Successful Run

Purpose: Provide run recency and reliability.
Operator question answered: Did anything run?
Value score: 8
Classification: IMPROVE
Justification: Current timeline has this but not in daily summary form.

### Component: Missed Run Alert

Purpose: Show only if an expected run missed its window.
Operator question answered: What failed?
Value score: 8
Classification: KEEP
Justification: Current Runtime Timeline “Morning AI Run past grace window” is useful.

# Section 4: Open Positions Summary

High-level only. No detailed position review.

Required data:

* open position count
* top symbols or count only
* mark coverage state
* P&L availability state
* sleeve attribution state

This section must not include:

* detailed position review briefs
* thesis summaries
* raw candidate contract IDs
* exit workflow tables
* record-exit action buttons

## Proposed Components

### Component: Open Positions Summary Card

Purpose: Show count and health of current paper positions.
Operator question answered: What is open?
Value score: 9
Classification: KEEP
Justification: Current position count is one of the most useful signals.

### Component: Position Data Quality Indicator

Purpose: Say whether marks/P&L/attribution are trustworthy.
Operator question answered: Can I trust position summary?
Value score: 8
Classification: NEW
Justification: Current UI shows `n/a` values without enough interpretation.

### Component: Open Positions Link

Purpose: Navigate to detailed Positions screen.
Operator question answered: Where do I inspect positions?
Value score: 7
Classification: KEEP
Justification: Detail belongs off Today.

# Section 5: Candidate Summary

High-level only. No detailed candidate workflow.

Required data:

* candidate generation ran/not run
* output candidate count
* candidates requiring David count
* rejected/diagnostic lineage count summarized only if relevant
* current paper session day match
* candidate workflow blocked/degraded reason if any

This section must not include:

* candidate workflow tables unless action required
* rejected lineage rows
* construction boundary detail
* raw signal evidence labels
* incomplete candidate rows

## Proposed Components

### Component: Candidate Count Summary

Purpose: Say whether candidates exist today.
Operator question answered: Are there candidates?
Value score: 8
Classification: IMPROVE
Justification: Current candidate count is scattered across Positions, Command Center, and Candidate Funnel.

### Component: Candidate Action Summary

Purpose: Say whether any candidate requires David.
Operator question answered: Do candidates need action?
Value score: 9
Classification: IMPROVE
Justification: Candidate actionability is critical but must be strict.

### Component: Candidate Workflow Blocked Message

Purpose: Explain if candidate state is unavailable.
Operator question answered: What is blocked?
Value score: 8
Classification: NEW
Justification: Prevents stale or broken candidate pages from misleading the operator.

# Section 6: Research Summary

High-level only.

Required data:

* active hypotheses count
* collecting evidence count
* qualified for paper validation count
* blocked research count
* next research observation/run
* operator action required count

This section must not include:

* full hypothesis cards
* raw recommendation-ready labels
* research artifact IDs
* diagnostics tables

## Proposed Components

### Component: Research State Summary

Purpose: Show whether research is running, collecting evidence, blocked, or idle.
Operator question answered: What is waiting or researching?
Value score: 8
Classification: IMPROVE
Justification: Current Research Lab has useful counts but too much detail for Today.

### Component: Next Research Observation

Purpose: Show when research can progress.
Operator question answered: What happens next?
Value score: 7
Classification: NEW
Justification: Particularly important for collecting-evidence states.

# Section 7: System Health Summary

Operator language only. No engineering language.

Required data:

* top blocker if any
* degraded workflows
* affected operator surfaces
* whether daily paper-mode monitoring is still readable
* safety gates summary

This section must not show:

* raw contract JSON
* semantic invariant names
* runtime kernel internals
* artifact paths
* producer contract rows

## Proposed Components

### Component: System Health One-Liner

Purpose: Translate technical health into operator impact.
Operator question answered: Is anything blocked?
Value score: 9
Classification: REPLACE
Justification: Current Engineering/contract banners expose raw internals instead of impact.

### Component: Top Blocker Summary

Purpose: Show the most important blocker affecting Today.
Operator question answered: What failed or is blocked?
Value score: 8
Classification: IMPROVE
Justification: Engineering has blocker data but Today needs one plain summary.

### Component: Safety Gates Summary

Purpose: State monitoring-only/paper-only/no broker/no live/no autonomous.
Operator question answered: Can Aegis act?
Value score: 10
Classification: IMPROVE
Justification: Safety is essential and must be visible in operator language.

# Section 8: Next Scheduled Activity

Must answer:

```text
What happens next?
```

Required data:

* next scheduled run label
* next scheduled run time
* expected output/result
* workflow waiting for time/data
* timezone
* schedule confidence

## Proposed Components

### Component: Next Scheduled Run

Purpose: Show next scheduled Aegis activity.
Operator question answered: When is the next scheduled run?
Value score: 10
Classification: NEW
Justification: Current shell does not reliably surface this above the fold.

### Component: Waiting Reason

Purpose: Explain why Aegis is not doing anything now.
Operator question answered: What is waiting?
Value score: 9
Classification: NEW
Justification: Prevents quiet states from looking broken.

### Component: Expected Output

Purpose: Show what should change after the next run.
Operator question answered: What happens next?
Value score: 8
Classification: NEW
Justification: Helps operator know what to check later.

# Section 9: Trust and Evidence

Must answer:

```text
Why should I believe this?
```

Required data:

* requested/source day match
* evidence currentness
* latest audit timestamp
* verified graph summary
* runtime truth summary translated to operator language
* data limitations
* evidence detail link

This section must be compact. It should not lead the page.

## Proposed Components

### Component: Trust Statement

Purpose: Plain-English explanation of why Today is trustworthy or not.
Operator question answered: Why should I trust today's state?
Value score: 9
Classification: NEW
Justification: Current evidence is fragmented and architecture-heavy.

### Component: Evidence Detail Link

Purpose: Open detailed audit/evidence page.
Operator question answered: Where is the proof?
Value score: 7
Classification: KEEP
Justification: Evidence must remain available but not primary.

### Component: Known Limitations Summary

Purpose: State what the screen cannot prove today.
Operator question answered: What should I not trust?
Value score: 8
Classification: NEW
Justification: Prevents over-trust when runtime truth is partial or analytics are degraded.

# Components That Must Never Appear On Today

The following must not appear as primary Today content:

* Raw contract JSON.
* Surface Contract as primary content.
* Semantic invariant displays.
* Internal invariant IDs.
* Runtime truth kernel raw labels as headlines.
* Raw governance architecture diagrams.
* Candidate workflow details.
* Candidate construction boundary rows.
* Rejected-intent lineage rows.
* Position Review briefs or thesis details.
* Detailed position tables beyond compact summary.
* Engineering diagnostics.
* Artifact paths.
* Source mtime/hash tables.
* Empty placeholder groups.
* Legacy Operator Cockpit content.
* Stale prior-day candidate rows.
* Raw JavaScript/runtime errors.
* Broker/live/autonomous execution actions.
* Trade advice language.
* Ask Aegis as the dominant page content before status is clear.

# Component Register

| Component | Purpose | Operator Question Answered | Value | Classification | Justification |
| --- | --- | --- | ---: | --- | --- |
| Today Status Statement | Answer whether Aegis is okay today | Is Aegis okay today? | 10 | NEW | Missing dominant answer today. |
| David Action Required Statement | Say if David must act | Do I need to do anything? | 10 | IMPROVE | Existing queues need stricter semantics. |
| Next Scheduled Run | Show next run time | When is the next scheduled run? | 10 | NEW | Critical daily workflow answer. |
| Safety Mode Statement | Say monitoring-only/paper-only/no live | Can Aegis act? | 10 | IMPROVE | Safety exists but is not productized. |
| Open Positions Count | Show active exposure count | What is open? | 9 | KEEP | Current high-value signal. |
| Attention Required Queue | List true David actions | Do I need to do anything? | 10 | IMPROVE | Keep only for real user actions. |
| Candidate Count Summary | Summarize current-day candidates | Are there candidates? | 8 | IMPROVE | Must avoid candidate workflow detail. |
| Candidate Action Summary | Show candidate action need | Do candidates need David? | 9 | IMPROVE | Critical but currently duplicated. |
| Activity Timeline Summary | Summarize what ran | What happened today? | 9 | NEW | Current runtime details are scattered. |
| Last Run / Last Success | Show freshness of run activity | Did anything run? | 8 | IMPROVE | Useful from runtime timeline. |
| Missed Run Alert | Alert on missed expected run | What failed? | 8 | KEEP | Valuable when present. |
| Position Data Quality | Summarize mark/P&L/attribution quality | Can I trust positions? | 8 | NEW | Needed to explain n/a or degraded metrics. |
| Research State Summary | Summarize research progress | What is being researched? | 8 | IMPROVE | Current counts useful but too detailed. |
| Next Research Observation | Show when research progresses | What is waiting? | 7 | NEW | Useful for collecting evidence. |
| System Health One-Liner | Translate health impact | Is anything blocked? | 9 | REPLACE | Replace engineering language. |
| Top Blocker Summary | Show the highest-impact blocker | What is blocked? | 8 | IMPROVE | Engineering data should be summarized. |
| Waiting Reason | Explain quiet state | What is waiting? | 9 | NEW | Prevents no-activity confusion. |
| Expected Output | Explain what next run should produce | What happens next? | 8 | NEW | Provides follow-up expectation. |
| Trust Statement | Explain evidence confidence | Why trust this? | 9 | NEW | Required for confidence without architecture noise. |
| Evidence Detail Link | Let operator inspect proof | Where is the proof? | 7 | KEEP | Secondary access only. |
| Known Limitations Summary | State what is not proven | What should I not trust? | 8 | NEW | Prevents overconfidence. |
| Ask Aegis Prompt | Ask why/what next | Why? | 6 | IMPROVE | Useful only after core status. |
| Refresh Control | Refresh current summary | Is this current? | 6 | KEEP | Useful, secondary. |

# Top 10 Most Important Elements On Today

1. Today Status Statement.
2. David Action Required Statement.
3. Next Scheduled Run.
4. Safety Mode Statement.
5. Open Positions Count.
6. Attention Required Queue when real user actions exist.
7. Candidate Action Summary.
8. Activity Timeline Summary.
9. System Health One-Liner.
10. Trust Statement.

# Acceptance Criteria

The Today / Command Center screen passes product acceptance only when visible screenshot evidence proves a non-engineer can answer:

* Is Aegis okay today?
* What happened today?
* What is currently open?
* What is waiting?
* What is blocked?
* Do I need to do anything?
* What happens next?
* When is the next scheduled run?
* Can Aegis act or is it monitoring only?
* Why should I trust today’s state?

Tests do not replace screenshot acceptance.


## Operator Decision Dashboard v1 Amendment
Today / Command Center is the daily operator decision dashboard. Its first screen answers: what happened, whether Aegis made research progress, whether validation advanced, whether generated hypotheses advanced, what is blocked, and whether David must act.

The required top order is TODAY'S RESEARCH RESULT, DAVID ACTIONS, GENERATED HYPOTHESIS PROGRESS, VALIDATION PROGRESS, CURRENT BOTTLENECK, then detailed research metrics. Raw cards and safety details remain available but are not the primary answer.

Primary field ownership: scorecard fields come from `aegis_research_daily_scorecard_v1`; David buttons come from backend action artifacts; generated hypothesis blocker state comes from scorecard/throughput with Oil Shock exact blocker from `aegis_oil_shock_candidate_flow_v1`; allocation recommendation counts come from `aegis_research_allocation_recommendation_v1`; safety comes from verified runtime graph.
