# Aegis Change Control System V1 Specification

## Canonical Register

Path:

```text
aegis/change_control/aegis_change_control_register_v1.json
```

Schema ID:

```text
aegis_change_control_register_v1
```

## ID Format

Item IDs:

```text
ACC-YYYYMMDD-NNN
```

Linked record IDs:

```text
ACD-YYYYMMDD-NNN
ACI-YYYYMMDD-NNN
ACV-YYYYMMDD-NNN
```

Where:

* `ACC` = intake item
* `ACD` = decision record
* `ACI` = implementation record
* `ACV` = validation record

## Intake Register Schema

Required fields:

```json
{
  "id": "ACC-20260530-001",
  "title": "...",
  "description": "...",
  "change_type": "BUG|ENHANCEMENT|OPERATOR_FEEDBACK|AUDIT_FINDING|PRODUCT_IMPROVEMENT|ARCHITECTURE_CHANGE|TECHNICAL_DEBT|SAFETY_IMPROVEMENT",
  "status": "CAPTURED|TRIAGED|DECIDED|IMPLEMENTED|VALIDATING|VALIDATED|CLOSED|DEFERRED|REJECTED",
  "severity": "P0|P1|P2|P3",
  "priority": "P0|P1|P2|P3",
  "owner": "...",
  "source": "...",
  "created_at": "...",
  "updated_at": "...",
  "decision_ids": [],
  "implementation_ids": [],
  "validation_ids": [],
  "tags": []
}
```

## Decision Record Schema

Required fields:

```json
{
  "id": "ACD-20260530-001",
  "change_id": "ACC-20260530-001",
  "decision": "APPROVE|DEFER|REJECT|NEEDS_MORE_INFORMATION",
  "rationale": "...",
  "decided_by": "...",
  "decided_at": "...",
  "revisit_condition": null,
  "evidence_refs": []
}
```

## Implementation Record Schema

Required fields:

```json
{
  "id": "ACI-20260530-001",
  "change_id": "ACC-20260530-001",
  "summary": "...",
  "files_changed": [],
  "commands_added": [],
  "behavior_changed": false,
  "implemented_by": "...",
  "implemented_at": "...",
  "evidence_refs": []
}
```

## Validation Record Schema

Required fields:

```json
{
  "id": "ACV-20260530-001",
  "change_id": "ACC-20260530-001",
  "validated_at": "...",
  "validated_by": "...",
  "validation_summary": "...",
  "commands_run": [],
  "test_results": [],
  "screenshot_evidence": [],
  "browser_evidence": [],
  "artifact_evidence": [],
  "api_evidence": [],
  "safety_gate_evidence": [],
  "remaining_gaps": []
}
```

## Status Transition Rules

Allowed forward transitions:

* `CAPTURED -> TRIAGED|DEFERRED|REJECTED`
* `TRIAGED -> DECIDED|DEFERRED|REJECTED`
* `DECIDED -> IMPLEMENTED|DEFERRED|REJECTED`
* `IMPLEMENTED -> VALIDATING|VALIDATED|DEFERRED`
* `VALIDATING -> VALIDATED|DEFERRED`
* `VALIDATED -> CLOSED|DEFERRED`
* `DEFERRED -> TRIAGED|DECIDED|REJECTED`

Terminal states:

* `CLOSED`
* `REJECTED`

`CLOSED` may not be entered directly from any state except `VALIDATED`.

## Severity Rules

* `P0`: current operator trust, safety, or stale-action failure.
* `P1`: should fix before broad use.
* `P2`: V1.1 improvement.
* `P3`: polish or housekeeping.

## Priority Rules

Priority defaults to severity unless explicitly lowered or raised in a Decision Record.

## Ownership Rules

Every item must have an owner. `SYSTEM` is allowed for seeded records. Human operator names or role owners are allowed.

## Validation Rules

Validation fails if:

* required fields are missing
* IDs are duplicated
* linked records do not resolve
* status is invalid
* severity or priority is invalid
* status transition is invalid when `previous_status` is present
* `CLOSED` lacks a linked Validation Record
* `VALIDATED` lacks a linked Validation Record
* UI item is `CLOSED` without screenshot evidence
* UI item involving displayed truth is `CLOSED` without browser evidence
* data/truth item is `CLOSED` without artifact, API, and browser consistency evidence when operator-facing
* safety item is `CLOSED` without safety gate evidence

## Links Between Records

The Intake Register owns the item identity. Decision, Implementation, and Validation Records must each reference a valid `change_id`.

Each intake item must list its linked IDs. The validator checks both directions.



## V1.1 Report Fields

The change-control report includes read-only dashboard fields for the UI:

* `lifecycle_counts` by allowed status.
* `validation_dashboard` for awaiting validation and missing closure evidence.
* `decision_dashboard` for captured or triaged items awaiting a decision.
* `closure_evidence_summary` for validated or closed items.

These report fields are derived from the canonical register and linked records. They do not mutate status.


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

## V1.1 Relationship Fields

Change-control intake records may define governed relationships:

* `parent_id`: optional parent change record ID.
* `child_ids`: all child records owned by this parent.
* `required_child_ids`: child records that must complete before parent validation or closure.
* `dependency_ids`: records this item depends on.
* `blocker_ids`: records currently blocking this item.
* `prerequisite_records`: records that must be addressed first.
* `downstream_records`: records that depend on this item.
* `relationship_role`: `PARENT`, `CHILD`, or omitted.

Relationship rules:

* All linked records must resolve to existing intake records.
* Parent/child links must be bidirectional.
* A record may not link to itself.
* A parent may not move to `VALIDATED` or `CLOSED` while any required child remains open.
* Required children are considered complete only when `VALIDATED`, `CLOSED`, or explicitly `REJECTED`.

ACC-20260530-008 is the parent for the Research Validation Engine rework children ACC-20260530-008A through ACC-20260530-008D.
