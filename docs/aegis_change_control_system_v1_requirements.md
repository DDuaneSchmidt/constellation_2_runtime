# Aegis Change Control System V1 Requirements

## Purpose

Aegis Change Control System V1 is the governed product-memory system for Aegis changes.

It captures, decides, implements, validates, and closes:

* bugs
* enhancements
* operator feedback
* audit findings
* product improvements
* architecture changes
* technical debt
* safety improvements

Core rule:

```text
No change is complete until validation evidence exists.
```

## Scope

V1 governs change records and evidence requirements. It does not replace source control, tests, runtime truth, product requirements, or audit artifacts. It links them.

V1 covers:

* intake capture
* decision rationale
* implementation summary
* validation proof
* status transitions
* closure rules
* evidence requirements by change type
* seed backlog for known Aegis operational/product issues

## Non-Goals

V1 must not:

* become a Kanban board
* replace Git history
* replace Aegis runtime truth
* create trading behavior
* enable broker execution
* enable live trading
* enable autonomous trading
* loosen safety gates
* require a complex UI

## Problems Solved

The system prevents:

* “fixed” claims without browser or artifact proof
* repeated rediscovery of the same operator issues
* audit findings being lost after a session ends
* UI changes closing without screenshots
* truth/data changes closing without API/artifact/browser consistency evidence
* safety changes closing without explicit safety-gate proof
* unclear deferral or rejection decisions

## Record Types

Every change item is represented by four linked record families:

1. Intake Register: what was requested or observed.
2. Decision Record: what was decided and why.
3. Implementation Record: what changed.
4. Validation Record: how we know it worked.

## Lifecycle States

Allowed statuses:

```text
CAPTURED
TRIAGED
DECIDED
IMPLEMENTED
VALIDATING
VALIDATED
CLOSED
DEFERRED
REJECTED
```

## Required Evidence By Change Type

### UI

Required before `CLOSED`:

* browser screenshot path
* rendered-browser proof when displayed truth is involved
* tests or documented reason tests are not applicable

### Data / Truth

Required before `CLOSED`:

* artifact path
* API payload summary or API proof
* browser consistency proof if operator-facing

### Safety

Required before `CLOSED`:

* safety gate proof showing trade advice, broker execution, broker submit/transmit, live trading, and autonomous live trading remain disabled unless an explicit future policy authorizes otherwise

### Product / Architecture / Technical Debt

Required before `CLOSED`:

* implementation record or explicit no-code decision
* validation record proving the intended product or engineering outcome

## Closure Rules

* `CLOSED` requires at least one linked Validation Record.
* `CLOSED` requires all required evidence for the change type.
* `VALIDATED` requires a linked Validation Record.
* `IMPLEMENTED` requires a linked Implementation Record unless the item is documentation-only or no-code by decision.
* `REJECTED` requires a Decision Record.
* `DEFERRED` requires a Decision Record with revisit condition.

## Safety Rules

Change Control V1 is read-only governance. It must not alter:

* trading logic
* broker execution
* live trading
* autonomous execution
* sleeve logic
* candidate generation
* canonical trading artifacts
* safety gates

## Auditability Requirements

Every record must include:

* stable ID
* created timestamp
* owner
* status
* severity
* priority
* source
* linked records
* evidence references

The register must be machine-readable and deterministic so tests can enforce closure and evidence rules.



## V1.1 Lifecycle Dashboards

The read-only Change Control UI must show lifecycle progress without becoming a Kanban board. It should summarize counts for:

```text
CAPTURED
TRIAGED
DECIDED
IMPLEMENTED
VALIDATING
VALIDATED
CLOSED
DEFERRED
REJECTED
```

It must make clear that implementation is not closure. Closure requires validation evidence appropriate to the change type.
