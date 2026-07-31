# Aegis Operator Jobs

## Purpose

This document defines the operator jobs Aegis must support before any new operator shell implementation begins.

The operator shell exists to help David understand what Aegis did today, what is currently open, what is waiting, what failed, whether any action is required, and whether Aegis is safe to trust for the current operating day.

The backend governance stack may provide safety evidence, but it must not dictate the visible UI language. The visible UI must be workflow-first and plain-English.

## Core Principle

Aegis must answer operator questions before it exposes artifacts, diagnostics, internal states, or architecture labels.

The first screen should answer:

```text
Is Aegis okay today, and do I need to do anything?
```

## Operator Jobs

### 1. Understand Aegis Status Today

The operator needs to know the current daily state in one glance.

The UI must answer:

* Is Aegis operating normally today?
* Is Aegis only monitoring?
* Is Aegis blocked or degraded?
* Is today a trading day, non-trading day, or historical view?
* Is the visible state current for the requested day?

The UI must not lead with raw readiness graph terms, contract names, artifact paths, or backend status codes.

### 2. Know Whether Anything Ran

The operator needs to know if scheduled Aegis work happened.

The UI must answer:

* Did the expected daily run execute?
* When did the last run happen?
* Did sleeves run?
* Did research run?
* Did candidate generation run?
* Did performance/mark updates run?
* If nothing ran, was that expected?

The UI must distinguish:

* not scheduled yet
* waiting for next run
* run completed with no activity
* run failed
* source data unavailable

### 3. Know Whether There Are Candidates

The operator needs to know if any paper candidate requires manual attention.

The UI must answer:

* Are there current-day candidates?
* Are they output-intent candidates or diagnostic/rejected lineage?
* Are any candidates ready for manual IB capture confirmation?
* Are candidates blocked by readiness, duplicate, construction, or missing data rules?
* If there are no candidates, is that a normal no-signal day or a pipeline failure?

The UI must not show stale prior-day candidates as current-day actions.

### 4. Know Whether Positions Are Open

The operator needs to know what Aegis believes is open.

The UI must answer:

* How many paper positions are open?
* Which symbols are open?
* Are positions marked with current prices?
* Are P&L and sleeve attribution reliable?
* Are any position records missing required evidence?

The UI must not mix current open positions with stale sessions, review shells, or unrelated diagnostics.

### 5. Know Whether Anything Is Blocked

The operator needs a clear blocked-state explanation.

The UI must answer:

* What is blocked?
* Why is it blocked?
* What is affected?
* Is this blocking operator work, analytics, research, or only engineering diagnostics?
* Is there a known repair path?

The UI must not show a bare `READY`, `BLOCKED`, `FAILED`, `PARTIAL`, or `UNKNOWN` label without plain-English impact.

### 6. Know Whether User Action Is Required

The operator needs to know if David must do something now.

The UI must answer:

* Does David need to capture, correct, defer, review, or acknowledge anything?
* If no action is required, what is Aegis doing automatically?
* Is the next step system-side, time-based, data-based, or user-side?

The UI must not call system repair, diagnostics, waiting, or monitoring rows “operator action required.”

### 7. Know What Happens Next

The operator needs the next expected transition.

The UI must answer:

* When is the next scheduled run?
* What is Aegis waiting for?
* What artifact or process is expected next?
* What will change when the next run completes?
* What should David check after that?

The UI must distinguish “next scheduled run” from “repair command” and “diagnostic command.”

### 8. Know Whether Aegis Is Safe To Trust Today

The operator needs an explicit trust statement.

The UI must answer:

* Is the current view safe to use for paper-mode monitoring?
* Are action buttons disabled because Aegis is blocked?
* Are backend safety gates still disabled for trade advice, broker execution, live trading, and autonomous trading?
* Are analytics canonical, degraded, or unavailable?

The UI must not imply that Aegis can trade, advise trades, submit broker orders, or act autonomously.

## Job Priority

Daily operator priority is:

1. Is anything urgent or blocked?
2. Is any user action required?
3. What is open?
4. Are there candidates?
5. What happened today?
6. What happens next?
7. Can analytics/research be trusted?
8. Where is the evidence?

Evidence and audit detail support the workflow; they are not the workflow.

## Non-Goals

The operator shell must not become:

* a raw artifact browser
* a backend readiness dashboard
* an internal architecture demo
* an engineering log viewer
* a generic AI chat page
* a trading execution system

## Acceptance Standard

A non-engineer should be able to answer within 10 seconds:

* Is Aegis okay today?
* Did anything run?
* Are there positions?
* Are there candidates?
* Is anything blocked?
* Do I need to do anything?
* What happens next?

If the visible page does not answer those questions, the UI is not acceptable, even if tests pass.

