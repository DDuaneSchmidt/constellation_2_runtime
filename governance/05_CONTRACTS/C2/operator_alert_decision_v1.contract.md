title: "C2 Operator Alert Decision V1"
status: "DRAFT"
doc_type: "contract"
owner: "Constellation Governance"
last_updated: "2026-04-14"

# C2 Operator Alert Decision V1

## Purpose

This contract governs the normalized operator alert decision used by desktop, durable-log, email, and SMS delivery layers.

The contract separates:

1. Truth reads
2. Alert decision derivation
3. Channel policy
4. Delivery

Delivery layers MUST NOT reinterpret raw health or startup state once the decision object is built.

## Truth Inputs

The decision layer reads authoritative artifacts only.

Primary inputs:

- `reports/trading_day_state_v1/<day>/trading_day_state.v1.json`
- `reports/day_start_blocked_v1/<day>/day_start_blocked.v1.json`

Fallback input when the bridge artifacts are absent:

- `reports/trading_day_state_machine_v1/<day>/trading_day_state_machine.v1.json`

The decision layer MUST NOT rewrite or mutate canonical truth.

## BOD Authority

The BOD schedule is derived from:

- `ops/systemd/user/c2-paper-day-orchestrator.timer`

The governed default grace window is:

- `5 minutes`

The decision layer MUST expose the next expected event and next expected ET timestamp using the timer-derived BOD schedule.

## Required Phases

The normalized phase MUST be one of:

- `PRE_OPEN`
- `BOD_WINDOW`
- `ACTIVE_SESSION`
- `POST_CLOSE`
- `UNKNOWN`

## Required States

The normalized state MUST be one of:

- `PRE_OPEN_WAITING`
- `BOD_DUE_NOT_STARTED`
- `STARTING`
- `STARTED_HEALTHY`
- `STARTED_DEGRADED`
- `BLOCKED`
- `FAILED`
- `UNKNOWN`

## Required Severity

The normalized severity MUST be one of:

- `INFO`
- `WARN`
- `ERROR`
- `CRITICAL`

## Decision Object

The decision object MUST include, at minimum, semantics for:

- `day`
- `phase`
- `state`
- `severity`
- `notify_email`
- `notify_desktop`
- `notify_log`
- `dedupe_key`
- `operator_message`
- `first_failure`
- `blocked_stage`
- `orchestrator_started`
- `next_expected_event`
- `next_expected_time_et`
- `evidence`

Implementations MAY include compatibility aliases such as `day_utc` or `alert_key`, but the normalized semantics above are authoritative.

## Channel Policy

### PRE_OPEN_WAITING

- `notify_email = false`
- `notify_desktop = false`
- `notify_log = true`
- operator message MUST say that waiting for BOD is expected and orchestrator non-start is expected

### STARTING

- `notify_email = false`
- `notify_desktop = false`
- `notify_log = true`
- operator message MUST say startup evidence is still within the BOD grace window

### BOD_DUE_NOT_STARTED

- `notify_email = true`
- `notify_desktop = true`
- `notify_log = true`
- this state MUST only occur after the governed BOD grace window has elapsed

### STARTED_HEALTHY

- `notify_email = false`
- `notify_desktop = false`
- `notify_log = true`

### STARTED_DEGRADED

- `notify_email = true`
- `notify_desktop = true`
- `notify_log = true`

### BLOCKED

- `notify_email = true`
- `notify_desktop = true`
- `notify_log = true`

### FAILED

- `notify_email = true`
- `notify_desktop = true`
- `notify_log = true`

### UNKNOWN

- pre-BOD ambiguity MUST NOT trigger email
- unresolved ambiguity after BOD grace MAY trigger email and desktop notification
- `notify_log = true`

## Transition / Dedupe Rules

Alerts are transition-driven.

The dedupe key MUST change only when actionable semantics materially change, including at least:

- normalized `phase`
- normalized `state`
- normalized `severity`
- `first_failure`
- `blocked_stage`
- `orchestrator_started`
- materially relevant evidence used to classify the state

Repeated unchanged states MUST NOT resend email or desktop notifications.

## Delivery Contract

Email, SMS, desktop, and durable-log layers MUST:

- consume the normalized decision object
- honor the `notify_*` flags directly
- avoid deriving independent health judgments from raw heartbeat or state fields

## Runtime Boundary

If multiple repo roots exist, their operator alert implementations MUST remain behaviorally aligned to this contract.

Shadow or legacy runtime copies MUST NOT introduce divergent pre-BOD or delivery semantics.
