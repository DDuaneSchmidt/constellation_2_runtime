# Aegis Operator Screen Spec

## Purpose

This document defines the first rebuild scope for the Aegis operator shell.

The shell has seven screens:

* Today / Command Center
* Positions
* Candidate Pipeline
* Performance
* Research
* System Health
* Evidence / Audit Detail

The visible UI must be workflow-first, plain-English, deterministic, low-density, and screenshot-approved.

## Shared Screen Rules

Every screen must show:

* requested day
* source day if different
* primary visible state
* one-sentence answer to the screen's primary question
* what happens next
* whether David action is required

Every screen must avoid:

* raw backend governance vocabulary as the headline
* stale prior-day rows in current-day primary views
* duplicated fallback pages
* unrelated Position Review content
* contradictory `READY` and `BLOCKED` panels
* evidence drawers when no evidence exists
* prominent Ask Aegis before core status is clear
* internal contract names as main UI content

Diagnostics and raw artifact detail belong in Evidence / Audit Detail.

## Preferred Operator API Boundary

The rebuilt UI should consume operator-ready summaries from these endpoints:

```text
/api/aegis/operator/today
/api/aegis/operator/positions
/api/aegis/operator/candidates
/api/aegis/operator/performance
/api/aegis/operator/research
/api/aegis/operator/health
/api/aegis/operator/evidence
```

These endpoints should translate backend governance into operator state and plain language.

Existing endpoints may be preserved behind this boundary if useful, but the visible UI should not call multiple raw artifact endpoints and assemble product meaning in the browser.

Required envelope shape:

```json
{
  "ok": true,
  "requested_day": "YYYY-MM-DD",
  "source_day": "YYYY-MM-DD",
  "state": "NORMAL|NO_ACTIVITY|WAITING_FOR_NEXT_RUN|BLOCKED|DEGRADED|NEEDS_USER_ACTION|MANUAL_IB_CAPTURE_READY|RESEARCH_RUNNING|RESEARCH_UNAVAILABLE|PERFORMANCE_UNAVAILABLE",
  "headline": "Plain-English answer",
  "summary": "One or two sentences",
  "did_anything_run": true,
  "user_action_required": false,
  "next_step": "Plain-English next step",
  "next_scheduled_run": null,
  "counts": {},
  "items": [],
  "warnings": [],
  "blockers": [],
  "evidence_available": true,
  "evidence_refs": []
}
```

Raw artifact paths, internal classifications, and detailed diagnostics may be included only under a diagnostics/evidence field, not in primary display fields.

## Screen 1: Today / Command Center


### Command Center Source Authority

Today / Command Center primary fields must be populated by `/api/aegis/operator/today` from authoritative reports, not browser fallback calculations or cached operator-state generation time. The endpoint must expose a field-level truth audit covering Aegis mode/status, candidate metrics, paper observation metrics, validation metrics, David action, research allocation, current bottleneck, last successful run, next scheduled run, and latest material change.

Past scheduled runs must not be rendered as next scheduled runs. When no future run is confirmed, the visible value is `No future run confirmed`.

### Purpose

Daily overview for what happened today, what is open, what is waiting, what failed, and whether David must act.

### Primary Question Answered

```text
Is Aegis okay today, and do I need to do anything?
```

### Required Data

* requested day
* source day
* operating mode summary
* last run and last successful run
* next scheduled run
* user-action count
* open position count
* current-day candidate count
* blocked/degraded summary
* research activity summary
* safety mode summary: monitoring/paper-only/no broker/live/autonomous execution

### Empty State

If no activity occurred and this is expected:

```text
No activity today. Aegis is monitoring and waiting for the next scheduled run.
```

Show next scheduled run if known.

### Blocked/Degraded State

If current-day state is missing or unsafe:

```text
Aegis cannot show a trustworthy current-day command summary.
```

Show reason, impact, and next step. Hide action buttons.

### User Action State

If action is required, show a short queue with only real user actions. Each row must show what David needs to do and why.

### Acceptance Criteria

A non-engineer can answer within 10 seconds:

* Did Aegis run?
* Are there positions?
* Are there candidates?
* Is anything blocked?
* Do I need to do anything?
* What happens next?

## Screen 2: Positions

### Purpose

Show current open paper positions and closed/recent position context without mixing candidate, review, or diagnostics shells.

### Primary Question Answered

```text
What positions are active today?
```

### Required Data

* open position count
* open position rows: symbol, sleeve, entry price, current mark, P&L, age/duration, data quality
* mark coverage summary
* sleeve attribution coverage summary
* closed/recent positions if applicable
* evidence availability for position source

### Empty State

```text
No open paper positions are recorded for the requested day.
```

If this is a historical/non-trading day, say so plainly.

### Blocked/Degraded State

If open positions cannot be trusted:

```text
Position state is unavailable for the requested day.
```

Show exact missing input or day mismatch. Do not show stale paper sessions as primary content.

### User Action State

Positions may show review/detail links. They must not show capture, submit, execute, buy, sell, or live broker actions.

### Acceptance Criteria

The page answers:

* how many positions are open
* what they are
* whether marks/P&L are trustworthy
* what to monitor next

## Screen 3: Candidate Pipeline

### Purpose

Explain candidate generation and operator-review readiness for the requested day.

### Primary Question Answered

```text
Are there any current-day candidates that need David?
```

### Required Data

* output-intent candidate count
* candidates ready for operator review
* candidates blocked or excluded, summarized by reason
* rejected lineage count, diagnostics-only
* duplicate suppression count
* construction/readiness blockers summarized plainly
* current paper session id for requested day

### Empty State

If generation completed with no candidates:

```text
No candidates qualified today.
```

If no run happened yet:

```text
Candidate generation has not run yet. Waiting for the next scheduled run.
```

### Blocked/Degraded State

If candidate state cannot be trusted:

```text
Candidate workflow is unavailable for the requested day.
```

Hide capture buttons. Show reason and next step.

### User Action State

Only current-day output-intent candidates that are ready and actionable may show:

* Confirm Captured
* Mark Not Captured
* Defer
* View Details

### Acceptance Criteria

No stale candidate, rejected intent, duplicate-suppressed row, or incomplete candidate appears as an actionable current-day row.

## Screen 4: Performance

### Purpose

Show paper portfolio performance only when metrics are trustworthy enough to display.

### Primary Question Answered

```text
How did the paper portfolio perform?
```

### Required Data

* open position count
* mark coverage
* missing mark count
* realized P&L
* unrealized P&L
* total P&L if canonical
* data quality state
* sleeve attribution coverage
* plain explanation if noncanonical

### Empty State

If no positions exist:

```text
No paper performance is available because there are no open or closed paper positions.
```

### Blocked/Degraded State

If metrics would be misleading:

```text
Performance analytics are unavailable.
```

Show reason and impact. Do not render unavailable metric grids.

### User Action State

No trade actions. Only view evidence or open health details.

### Acceptance Criteria

The page never shows `NOT_CANONICAL` with `PASS`, complete coverage with missing inputs, or canonical-looking P&L when required evidence is absent.

## Screen 5: Research

### Purpose

Show what Aegis research is doing, what is collecting evidence, what qualified for paper validation, and what is blocked.

### Primary Question Answered

```text
What is being researched or validated?
```

### Required Data

* hypothesis counts by visible state
* active research runs
* collecting-evidence sample counts
* qualification state
* paper-testing sleeve state
* next sample/run expected time
* whether user action is required

### Empty State

```text
No active research work is scheduled for the requested day.
```

### Blocked/Degraded State

```text
Research status is unavailable for the requested day.
```

Show the missing source or runtime reason.

### User Action State

Research should not require manual approval before paper validation unless an explicit safety policy requires it. User action may appear only for blocked review/diagnostic decisions.

### Acceptance Criteria

The page clearly says whether research is running, collecting evidence, blocked, qualified, or waiting for time/data.

## Screen 6: System Health

### Purpose

Show what is broken, degraded, waiting, or safe.

### Primary Question Answered

```text
What is broken, why, and what happens next?
```

### Required Data

* runtime truth summary
* verified graph summary
* blockers and degraded issues
* current-day source availability
* next repair or recovery plan if available
* safety gates summary
* scheduled run health

### Empty State

If all monitored systems are healthy:

```text
No system blockers are currently reported.
```

### Blocked/Degraded State

Show the top issue first, in plain English:

* problem
* cause
* impact
* next step
* verification path

### User Action State

Only label something as user action if David must do it. System repair, diagnostics, and waiting are not user action.

### Acceptance Criteria

The page answers what is broken and whether it affects daily paper-mode operation without requiring artifact knowledge.

## Screen 7: Evidence / Audit Detail

### Purpose

Provide drill-down evidence for operators or engineers after the primary screens have answered the workflow questions.

### Primary Question Answered

```text
What evidence supports the visible state?
```

### Required Data

* audit handoff
* verified graph reference
* runtime truth references
* source artifacts grouped by screen
* day/source consistency details
* diagnostic/raw state only after expansion

### Empty State

```text
No evidence detail is available for this requested day.
```

### Blocked/Degraded State

If evidence cannot be read, explain which evidence source is missing and whether primary screens are blocked because of it.

### User Action State

No workflow actions. Evidence may offer copy/open diagnostics actions only.

### Acceptance Criteria

Raw artifact names, backend contract terms, and diagnostics are available here, not on primary workflow screens.

## Screenshot Acceptance Rule

A screen is not accepted until a visible screenshot proves:

* the first visible message answers the primary question
* no internal backend vocabulary leads the page
* no unrelated legacy content appears
* no stale/wrong-day content appears as primary content
* empty/blocked/degraded states are readable without engineering context


## Command Center Operator Decision Dashboard v1 Amendment
Screen 1 is now explicitly an operator decision dashboard, not a system monitoring dashboard. It must lead with the five sections: TODAY'S RESEARCH RESULT, DAVID ACTIONS, GENERATED HYPOTHESIS PROGRESS, VALIDATION PROGRESS, and CURRENT BOTTLENECK.

A non-engineer must not need to inspect raw cards to know whether the run was useful or whether David has work to do. Detailed research metrics and diagnostics are below those five sections.

Primary labels must not say Production, Monitoring only, Trade Recommendation, Manual Capture, UNKNOWN, Values incomplete, or Needs review without explicit action buttons.
