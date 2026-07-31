# Aegis Candidates Screen Product Specification

## Purpose

The Candidates screen answers one question:

```text
What might we own next?
```

It is the operator surface for current candidate opportunity, readiness, qualification, blockers, and next evaluation timing. It is not a holdings, exposure, P&L, or position review surface.

The Today / Command Center screen may summarize whether candidates need attention and link here, but candidate workflow detail belongs here.

## Product Boundary

Candidates must include:

* candidate universe
* actionable candidates
* non-actionable candidates
* qualification status
* readiness status
* blockers
* next evaluation time
* reason no candidate is actionable

Candidates must not include:

* open holdings
* position P&L
* portfolio exposure
* position performance
* position review details

## Operator Questions

### Are there candidates that need review?

Required data:

* actionable candidate count
* non-actionable candidate count
* current paper session id
* requested day and source day
* row-level actionability classification

Source system:

* candidate state
* candidate lifecycle projection
* command center queue audit
* candidate readiness artifacts
* operator surface readiness / operator state as gating evidence

Update frequency:

* after candidate generation
* after readiness, duplicate, contract, or construction boundary updates
* after operator command processing

Empty state:

```text
No candidates qualified for review today.
```

Blocked state:

```text
Candidate workflow is unavailable because current-day candidate evidence is missing, stale, or for a different day.
```

Degraded state:

```text
Candidate summary is visible, but some readiness evidence is incomplete.
```

User action state:

* Candidate actions may appear only for current-day actionable candidates whose readiness and duplicate gates allow operator action.
* No position, P&L, exposure, or broker/live action appears.

### What candidate universe was evaluated?

Required data:

* candidate universe count
* output-intent count
* non-actionable count
* rejected/excluded count as diagnostics or summary
* source day/session

Source system:

* candidate lifecycle projection
* candidate generation diagnostics
* signal evidence boundary
* candidate state

Update frequency:

* after candidate generation and lifecycle projection

Empty state:

* no candidate generation has run yet, or generation completed with zero candidates.

Blocked state:

* missing candidate state or lifecycle projection for requested day.

Degraded state:

* universe count exists but readiness/qualification evidence is partial.

User action state:

* none at universe-summary level.

### Why is a candidate actionable or not actionable?

Required data:

* qualification status
* readiness status
* blocker reason
* duplicate classification
* signal evidence outcome
* contract generation outcome
* construction boundary outcome
* market data status

Source system:

* candidate readiness
* signal evidence boundary
* contract generation boundary
* construction input boundary
* duplicate candidate report
* market data inputs / coverage

Update frequency:

* after each boundary report is built

Empty state:

* no candidates to explain.

Blocked state:

* candidate cannot be trusted because an upstream boundary is missing.

Degraded state:

* candidate exists but one non-action-critical explanation source is incomplete.

User action state:

* actionable candidates may expose allowed candidate review/capture actions.
* non-actionable candidates show reason and next evaluation/repair path only.

### What happens next?

Required data:

* next scheduled candidate generation or evaluation time
* current waiting reason
* expected data dependency
* next system action

Source system:

* run history
* runtime timeline
* candidate diagnostics
* operator state

Update frequency:

* after scheduler/run-history update

Empty state:

```text
Candidate generation has not run yet. Waiting for the next scheduled run.
```

Blocked state:

* no next evaluation can be determined because scheduling or candidate state evidence is unavailable.

Degraded state:

* next expected run is known but some candidate evidence is incomplete.

User action state:

* none unless a candidate is explicitly actionable.

## Proposed Components

### Component: Candidate Status Summary

Purpose: Give the first visible answer to whether anything might become owned next and whether operator action is required.

Operator question answered: Are there candidates today, and do I need to do anything?

Data source: candidate state, lifecycle projection, queue audit, surface readiness.

Empty state: No candidates qualified for review today.

Blocked state: Candidate workflow is unavailable with exact missing/wrong-day evidence.

Degraded state: Candidate counts are visible, but readiness evidence is incomplete.

User action state: Link to actionable candidate queue when current-day actions are allowed.

Classification: NEW

Value score: 10

Justification: Candidate actionability is currently scattered and confused with positions.

### Component: Actionable Candidate Queue

Purpose: List only candidates that require or allow operator review/capture action.

Operator question answered: Which candidate, if any, needs David?

Data source: candidate state, candidate readiness, queue audit, duplicate classification, current paper session.

Empty state: No current candidates require operator action.

Blocked state: Hidden when current-day candidate actionability cannot be verified.

Degraded state: Only rows with complete actionability evidence can show actions; other rows move to non-actionable explanations.

User action state: Confirm Captured, Mark Not Captured, Defer, or View Details only when allowed by readiness and day/session gates.

Classification: NEW / IMPROVE

Value score: 10

Justification: This is the core candidate workflow and must not be split across Positions or Today.

### Component: Candidate Universe Summary

Purpose: Show how broad the current candidate evaluation was without raw diagnostic overload.

Operator question answered: What was evaluated today?

Data source: candidate lifecycle projection, candidate generation diagnostics, signal evidence boundary.

Empty state: No generation run yet, or generation completed with no candidates.

Blocked state: Universe unavailable because lifecycle or diagnostic evidence is missing.

Degraded state: Universe count exists but source lineage is partial.

User action state: none.

Classification: NEW

Value score: 8

Justification: Operators need to distinguish “nothing qualified” from “nothing ran.”

### Component: Non-Actionable Candidate List

Purpose: Explain candidates that exist but cannot be acted on.

Operator question answered: Why is this candidate not reviewable?

Data source: candidate readiness, duplicate report, signal evidence boundary, contract generation boundary, construction boundary, market data coverage.

Empty state: Hidden if no non-actionable candidates exist.

Blocked state: Hidden when candidate evidence cannot be trusted.

Degraded state: Rows show the most specific known reason and flag missing explanation evidence.

User action state: View Details or Evidence only; no capture controls.

Classification: NEW

Value score: 8

Justification: Non-actionable candidates are useful only when they explain why no action is available.

### Component: Qualification and Readiness Columns

Purpose: Show why a candidate is ready, blocked, rejected, duplicate-suppressed, or waiting.

Operator question answered: What is stopping this from becoming actionable?

Data source: readiness report, signal evidence boundary, contract generation boundary, construction boundary, duplicate candidate report.

Empty state: No candidate rows.

Blocked state: Columns show unavailable only when the boundary owner is missing.

Degraded state: Missing non-critical explanation fields are marked without enabling actions.

User action state: Drives whether actions render on row.

Classification: NEW

Value score: 9

Justification: Candidate decisions require readiness explanation, but it must be operator-language, not raw artifact vocabulary.

### Component: Candidate Blocker Summary

Purpose: Summarize why no candidate is actionable or why some candidates are blocked.

Operator question answered: Why can’t I act on candidates?

Data source: candidate state, readiness, market data, duplicate, contract, construction, signal boundary reports.

Empty state: Hidden when no blockers exist.

Blocked state: Shows the missing blocker source if candidate workflow itself is unavailable.

Degraded state: Shows known blockers and missing explanation count.

User action state: Evidence/diagnostics link only unless specific candidate row is actionable.

Classification: NEW

Value score: 9

Justification: A candidate screen without blocker explanation recreates the previous silent omission failure mode.

### Component: Next Candidate Evaluation

Purpose: Show when candidate state is expected to change.

Operator question answered: What happens next?

Data source: run history, runtime timeline, candidate diagnostics, scheduler state.

Empty state: No scheduled candidate evaluation known.

Blocked state: Schedule unavailable with reason.

Degraded state: Next time known, but candidate readiness is partial.

User action state: none.

Classification: NEW

Value score: 8

Justification: Waiting states must be explicit so the operator does not interpret quiet candidates as failure.

### Component: Candidate Detail Drawer

Purpose: Show candidate thesis, readiness, boundary outcomes, and evidence after a row is selected.

Operator question answered: Why is this candidate shown this way?

Data source: candidate lifecycle projection, signal evidence, readiness, contracts, construction, duplicate, market data.

Empty state: No drawer without selected row.

Blocked state: Shows missing row-level evidence.

Degraded state: Shows available details and missing sources.

User action state: row-local actions only if the row is actionable.

Classification: NEW / IMPROVE

Value score: 8

Justification: Detailed candidate evidence is useful, but only after the actionability summary is clear.

### Component: Candidate Evidence Drawer

Purpose: Keep candidate boundary and audit evidence inspectable without making raw diagnostics the primary UI.

Operator question answered: Why should I trust this candidate state?

Data source: source artifacts and hashes for candidate evidence.

Empty state: Hidden if no evidence exists.

Blocked state: Shows evidence missing reason.

Degraded state: Shows partial evidence limitations.

User action state: Expand evidence.

Classification: KEEP

Value score: 6

Justification: Evidence supports trust, but primary candidate UI should remain workflow-first.

### Component: Position Holdings or P&L on Candidates

Purpose: Holdings/P&L display.

Operator question answered: None for Candidates.

Data source: position ledger, performance/P&L reports.

Empty state: Not applicable.

Blocked state: Not applicable.

Degraded state: Not applicable.

User action state: Not allowed on Candidates.

Classification: REMOVE

Value score: 1

Justification: Current ownership belongs to Positions. Candidate pages may mention duplicate/open-position blockers without showing holdings or P&L.

## Screenshot Acceptance

A Candidates screenshot passes only if visible browser output proves:

1. The first meaningful content answers whether anything might be owned next.
2. Actionable and non-actionable candidates are clearly separated.
3. If no candidate is actionable, the reason is visible.
4. Next evaluation/waiting state is visible when relevant.
5. Candidate qualification, readiness, blockers, duplicate status, and source-day/session boundaries are summarized in operator language.
6. Open holdings, position P&L, portfolio exposure, position performance, and position review details do not appear as primary content.
7. Candidate actions appear only for current-day, current-session, readiness-approved, non-suppressed, operator-actionable candidates.
8. Evidence is available only behind secondary/collapsed detail.
