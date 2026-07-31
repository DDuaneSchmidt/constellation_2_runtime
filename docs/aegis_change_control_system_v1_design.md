# Aegis Change Control System V1 Design

## Storage Location

Canonical project-local storage:

```text
aegis/change_control/
```

Primary register:

```text
aegis/change_control/aegis_change_control_register_v1.json
```

This location is intentionally inside the repository because it governs product memory, not runtime market truth.

## Artifact Format

The register is JSON with deterministic ordering. It contains:

* metadata
* intake register rows
* decision records
* implementation records
* validation records

V1 uses one file to keep review simple. It can be split later if size or write contention justifies it.

## CLI Entrypoints

Required V1 commands:

```bash
npm run aegis:change-control-validate
npm run aegis:change-control-report
```

`aegis:change-control-validate` enforces schemas, links, status rules, and closure evidence.

`aegis:change-control-report` prints a compact read-only report:

* open P0/P1 items
* items awaiting validation
* recently closed items
* deferred items
* V1.1 backlog

## APIs and UI

V1 does not require a UI or API. It is a governed register plus validation/report commands.

A future read-only System Health or Evidence page may surface the report, but V1 must not become a Kanban board or duplicate Jira.

## Relationship To Aegis Docs And Manifests

The operator portal manifest references:

* the requirements document
* the specification document
* the design document
* the register artifact
* the validate/report commands
* the unit tests

This makes Change Control discoverable through the existing Aegis module graph without giving it authority over trading behavior.

## How This Avoids Becoming A Jira Clone

V1 tracks only evidence-backed change lifecycle records. It does not implement:

* drag/drop workflow boards
* sprint planning
* comments
* notifications
* time tracking
* assignment workflows

The useful primitive is closure evidence, not task management.

## How This Prevents Claimed-Fixed-But-Browser-Still-Wrong

UI-related changes cannot close unless a Validation Record includes screenshot evidence. When the issue involves displayed truth, rendered-browser evidence is also required.

Data/truth changes cannot close unless validation evidence proves artifact/API/browser consistency.

Safety changes cannot close unless safety gate proof is present.

The validator enforces these rules mechanically, so a text claim that something is fixed is not enough.

