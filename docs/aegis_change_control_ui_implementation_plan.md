# Aegis Change Control UI Implementation Plan

## Route

Route:

```text
/aegis-change-control
```

Route ID:

```text
aegis_change_control
```

Navigation label:

```text
Change Control
```

## Data Source

Canonical register:

```text
aegis/change_control/aegis_change_control_register_v1.json
```

Existing CLI sources:

```bash
npm run aegis:change-control-validate
npm run aegis:change-control-report
```

## API Endpoint

Read-only endpoint:

```text
/api/aegis/change-control
/api/aegis/change-control/latest
```

Payload:

* `ok`
* `data.register`
* `data.report`
* `data.validation`
* `data.source_path`

No free-form mutation endpoint is allowed in V1. The only allowed write endpoint is the controlled decision endpoint for approve, reject, defer, prioritize, and notes.

## Component Plan

### Summary Cards

* Total items
* Open P0/P1
* Awaiting validation
* V1.1 backlog
* Recently closed

### Priority Sections

* Open P0/P1
* Awaiting decision
* Awaiting validation
* V1.1 backlog

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

### Detail Expansion

Read-only details per item:

* problem
* linked decisions
* linked implementation records
* linked validation evidence
* screenshots
* tests
* safety proof
* closure reason

## Empty States

* No change-control items recorded.
* No open P0/P1 items.
* No items awaiting validation.
* No V1.1 backlog items.
* No recently completed items.
* No deferred or rejected items.

## Error States

If the register cannot be read or validated:

* show a safe unavailable message
* include operator-safe reason
* do not render stale fake rows
* do not show stack traces

## Validation Rules

The UI must not bypass backend validation. It displays validation status from the endpoint.

It must not:

* edit records
* close records
* create records
* call mutation endpoints other than the controlled decision endpoint
* infer closure without validation evidence

## Screenshot Acceptance Criteria

Visible screenshot must show:

* page title `Change Control`
* summary cards
* at least one Open P0/P1 item when seeded records exist
* V1.1 backlog section
* all-items table
* read-only details available by expansion
* no edit, save, close, delete, or drag/drop controls


## V1.1 Implementation Addendum

Add report-derived sections to `/aegis-change-control`:

* Lifecycle Pipeline Summary from `report.lifecycle_counts`.
* Validation Dashboard from `report.validation_dashboard`.
* Decision Dashboard from `report.decision_dashboard`.
* Closure Evidence Summary from `report.closure_evidence_summary`.

The route remains read-only for item editing, implementation, validation, closure, assignment, and drag/drop. The controlled decision workflow may POST approve, reject, defer, prioritize, and decision-note records only.


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

## V1.1 Relationship Implementation

The Change Control API report includes `relationship_graph` with parent rows, child rows, dependency edges, and completion rollups. `/aegis-change-control` renders this graph as a read-only section after the lifecycle pipeline summary. No edit, close, validate, implement, drag/drop, or assignment controls are added.

Validation additions:

* parent/child links resolve and are bidirectional
* dependency, blocker, prerequisite, and downstream links resolve
* parent `VALIDATED`/`CLOSED` state fails while required children remain open
* ACC-20260530-008 shows ACC-20260530-008A through ACC-20260530-008D as required child records
