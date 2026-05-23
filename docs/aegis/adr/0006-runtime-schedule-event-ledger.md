# ADR 0006: Aegis Runtime Schedule Event Ledger

## Status
Accepted

## Context
Runtime Timeline previously derived operator-facing schedule status from planned schedule rows and projection heuristics. That allowed past events to remain visually labeled as waiting even when the scheduled time and grace window had passed. Planned schedule alone cannot answer whether a job ran, whether the expected artifact was produced, whether a retry is active, or whether a later successful event superseded an earlier scheduled attempt.

## Decision
Aegis Runtime Timeline now uses `schedule_event_ledger_v1` as the status authority for scheduled runtime events. The ledger separates four concepts:

- planned schedule: when an event was expected to occur
- actual execution: observed start/completion evidence when available
- expected artifacts: governed outputs that prove the event completed
- operator status: plain-language status rendered in the UI

The ledger is written to:

`reports/schedule_event_ledger_v1/<day_utc>/schedule_event_ledger.v1.json`

Each event includes event identity, operational day, scheduled timestamp, owning job, expected artifact type/path, grace window, observed artifacts, run status, operator status, retry count, lineage metadata, and a content hash.

## Status Rules
Internal `run_status` values are:

- `PLANNED`
- `QUEUED`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `SKIPPED`
- `SUPERSEDED`
- `MISSED`
- `UNKNOWN`

Operator-facing statuses are:

- Scheduled
- Running
- Completed
- Waiting on data
- Missed
- Failed
- Superseded
- Not scheduled today

Rules:

- Future events are `Scheduled`.
- If the expected artifact exists and is not failed, the event is `Completed`.
- If the expected artifact exists and reports failure/rejection/blocking, the event is `Failed`.
- If `scheduled_at + grace_window` is past and no expected artifact or active dependency exists, the event is `Missed`.
- A past event may be `Waiting on data` only when a recorded active dependency exists.
- A past event may be `Running` only inside its grace window or when active-job evidence exists.
- A successful later event for the same expected artifact may mark earlier scheduled attempts `Superseded`.
- Historical/replay views must label historical ambiguity explicitly instead of pretending a current run is waiting.

Hard invariant:

No event older than `scheduled_at + grace_window` may render as `Scheduled` or generic `Waiting` without an active dependency.

## Replay Behavior
Replay uses the ledger event content hashes plus observed artifact references to prove what the operator saw at runtime. Historical replay does not reinterpret past schedules as current operational waiting states.

## UI Contract
Runtime Timeline consumes ledger rows and renders:

- next event: earliest unresolved overdue operator event, otherwise earliest future scheduled event
- today/tomorrow event groups
- completed past events collapsed by default
- missed events and active dependencies in diagnostics
- simple operator statuses, not raw scheduler internals

Diagnostics may expose expected artifacts, observed artifacts, retry count, status reason, and owning job. Primary UI must not derive status from schedule rows alone.

## Non-goals
This ADR does not change scheduling behavior, certification timing, scoring logic, broker submit/transmit controls, autonomous execution controls, order routing, trade advice policy, or the execution firewall.
