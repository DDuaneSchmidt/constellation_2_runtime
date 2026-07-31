# Aegis Change Control UI Boundary

## This UI May Show

* enhancement ID
* title
* status
* severity
* priority
* affected domain
* origin
* validation status
* linked evidence
* closed/open state
* decision summary
* implementation summary
* validation evidence
* linked screenshots
* linked tests
* safety impact
* closure reason

## This UI Must Not

* allow editing
* allow closing items
* modify the registry
* create implementation tasks automatically
* trigger trading logic
* trigger broker execution
* trigger live trading
* trigger autonomous execution
* trigger sleeve logic
* trigger candidate generation
* modify canonical trading artifacts
* modify safety gates

## Ownership Boundary

The Change Control UI owns visibility into improvement state. It does not own implementation, validation, trading workflow, research workflow, candidate workflow, or runtime safety policy.

## Evidence Boundary

The UI displays evidence references already present in the register. It does not certify evidence on its own. Closure authority remains in `aegis_change_control_register_v1` validation.


## V1.1 Boundary Clarification

The UI may emphasize lifecycle and evidence completeness. Lifecycle cards, validation dashboards, decision dashboards, and closure evidence summaries are read-only report views; only the controlled decision workflow may append Decision Records or update valid decision/priority fields.

Forbidden controls remain forbidden:

* edit
* close
* approve
* reject
* validate
* assign
* drag/drop


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
