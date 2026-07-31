# Aegis Change Control UI Screen Spec

## Purpose

The Change Control UI is a governed operator screen for Aegis enhancement requests, audit findings, bugs, product improvements, and technical debt. V1 remains read-only for implementation, validation, closure, and item editing, but may append controlled decision records.

It answers:

* What improvements are open?
* What is highest priority?
* What is awaiting validation?
* What is planned for V1.1?
* What was recently completed?
* What is deferred or rejected?
* Which items need David's decision?

The page must make product memory visible without requiring JSON inspection or CLI commands.

## Primary User

David or an Aegis operator reviewing current improvement state.

## Required Sections

### Open P0/P1

Purpose: show the highest-priority open improvements.

Empty state:

```text
No open P0/P1 items.
```

### Awaiting Decision

Purpose: show captured or triaged items that need a decision before implementation.

Empty state:

```text
No items awaiting decision.
```

### Awaiting Validation

Purpose: show implemented items that cannot close until validation evidence exists.

Empty state:

```text
No items awaiting validation.
```

### V1.1 Backlog

Purpose: show planned P2/P3 work that should not block V1.

Empty state:

```text
No V1.1 backlog items.
```

### Recently Closed

Purpose: show completed items with validation evidence.

Empty state:

```text
No recently completed items.
```

### Deferred / Rejected

Purpose: show items explicitly deferred or rejected and avoid silent loss of feedback.

Empty state:

```text
No deferred or rejected items.
```

### All Items Table

Columns:

* ID
* Title
* Status
* Severity
* Priority
* Domain
* Origin
* Validation state

## Detail Expansion

Each item may expand read-only details:

* problem
* decision summary
* implementation summary
* validation evidence
* linked screenshots
* linked tests
* safety impact
* closure reason

## Operator Language

Use:

* Open improvements
* Awaiting validation
* Completed
* Deferred
* Needs decision

Avoid as primary content:

* raw JSON
* schema internals
* implementation stack traces
* artifact dumps

## Error State

If the register cannot load, show:

```text
Change Control is unavailable.
The enhancement register could not be loaded. No stale or fake items are shown.
```

## Screenshot Acceptance Criteria

A non-engineer must be able to answer:

* What enhancements are open?
* What is highest priority?
* What needs validation?
* What is planned for V1.1?
* What was recently completed?

without reading JSON or running CLI commands.


## V1.1 Lifecycle Pipeline Summary

The page must show lifecycle counts for captured, triaged, decided, implemented, validating, validated, closed, deferred, and rejected items. The pipeline is not drag/drop and may only change status through the controlled decision workflow for approve, reject, or defer decisions.

The pipeline answers:

* Where is work stuck?
* How many items are captured but undecided?
* How many items are implemented but not validated?
* How many items are validated but not closed?

## V1.1 Validation Dashboard

The page must show:

* Items awaiting validation.
* Items missing required evidence.
* UI items missing screenshots.
* truth/data items missing artifact/API/browser proof.
* safety items missing safety-gate proof.

## V1.1 Decision Dashboard

The page must show items awaiting decision, the reason a decision is required, and the proposed next step.

## V1.1 Closure Evidence Summary

For validated or closed items, the page must summarize screenshot evidence, test evidence, audit evidence, safety proof, and closure reason.

## V1.1 Item Cards

Each item card must show ID, title, lifecycle status, severity, priority, affected domain, decision status, implementation status, validation status, and evidence completeness. It must explicitly communicate that implemented does not mean closed.


## Phase 14 Controlled Decision Workflow

Change Control may provide a controlled decision workflow for operator governance decisions only. Allowed actions are:

* Approve
* Reject
* Defer
* Prioritize
* Add decision notes

The workflow may append Decision Records and update intake status or priority when the transition is valid. It must not close, validate, or implement a change. Those states remain evidence-driven through Implementation and Validation Records.

Forbidden controls and endpoints:

* Close
* Validate
* Implement
* Assign
* Drag/drop

Every controlled decision must preserve safety gates and must not modify trading logic, broker execution, live trading, autonomous execution, sleeve logic, candidate generation, or canonical trading artifacts.

## V1.1 Parent / Child Dependency View

The Change Control UI must show governed relationships without becoming a Kanban board. It must display:

* parent item
* child items
* dependency graph edges
* required-child completion rollup
* blockers preventing parent validation

The UI must make clear that a parent item cannot be validated while required child records remain open. The view remains read-only except for the already controlled decision workflow.
