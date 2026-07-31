# Aegis Operator State Model

## Purpose

This document defines the visible operator states for the rebuilt Aegis operator shell.

These are product-facing states. They are not backend governance states and must not expose raw artifact, readiness, invariant, or contract vocabulary as primary UI language.

## Core Principle

The operator state must explain:

```text
What is happening, why it matters, and what David should do.
```

Each state must drive visible labels, empty states, blocked states, and action availability.

## State Rules

* Exactly one primary visible state should lead each screen.
* Supporting states may appear as secondary chips or rows.
* A state must never contradict another visible panel.
* If backend evidence is inconsistent, show a blocked/degraded state rather than mixed signals.
* Action buttons may appear only when the state explicitly allows user action.

## States

### NORMAL

**Display label:** Aegis is operating normally.

**Meaning:** Required current-day data is present for the screen, no blocking issue affects the primary workflow, and normal monitoring or review content may be shown.

**When it appears:**

* The requested day matches the source day.
* Required data for the screen is current enough to render.
* Semantic checks do not detect contradictions.
* The screen has meaningful content or an accepted no-activity state.

**What the user should do:** Review the page normally. Act only if a row explicitly asks for user action.

**Must not show:** Raw readiness labels as the headline, contradictory blocked panels, stale prior-day content, or disabled action buttons mixed with actionable language.

### NO_ACTIVITY

**Display label:** No activity today.

**Meaning:** Aegis ran or checked the relevant workflow and found no current-day positions, candidates, research transitions, or user actions for that screen.

**When it appears:**

* The relevant workflow completed or was explicitly not scheduled.
* There are no rows to show.
* Lack of rows is not caused by missing data or failure.

**What the user should do:** Nothing now. Check the next scheduled run time.

**Must not show:** Failure language, empty tables without explanation, old rows, or repair commands.

### WAITING_FOR_NEXT_RUN

**Display label:** Waiting for the next scheduled run.

**Meaning:** Aegis is not blocked; it is waiting for the next scheduled workflow, market close, evidence window, or research observation.

**When it appears:**

* The next expected progress depends on time.
* Required current state is known.
* No user action is required.

**What the user should do:** Wait until the displayed next run or observation time.

**Must not show:** Manual review required, generic blocked labels, action buttons, or failure language.

### BLOCKED

**Display label:** Aegis is blocked.

**Meaning:** A required input, producer, artifact, source, or consistency check prevents the screen from showing trustworthy primary workflow content.

**When it appears:**

* Required current-day data is missing or stale.
* A semantic contradiction makes analytics unsafe to render.
* A workflow cannot proceed.
* The requested day/source day mismatch would make content misleading.

**What the user should do:** Read the reason and next step. Use System Health or Evidence / Audit Detail if repair is needed.

**Must not show:** Action buttons, stale rows, canonical-looking analytics, or “READY” as the dominant message.

### DEGRADED

**Display label:** Aegis is degraded but readable.

**Meaning:** The screen can render some current-day information, but some data quality, attribution, analytics, or diagnostics are incomplete.

**When it appears:**

* The primary workflow can still be understood.
* Missing data does not make the primary answer unsafe.
* The impact is known and visible.

**What the user should do:** Use the visible content with the stated limitation. Do not treat degraded analytics as canonical.

**Must not show:** `PASS` data quality if major displayed fields are unavailable, or complete-looking metric cards for noncanonical analytics.

### NEEDS_USER_ACTION

**Display label:** David action required.

**Meaning:** A current-day workflow is waiting for David to make a real decision or record a real manual action.

**When it appears:**

* A row is current-day.
* The row is not stale, already captured, duplicate-suppressed, or diagnostics-only.
* The row has an allowed user action.

**What the user should do:** Complete the displayed action or open details to understand it.

**Must not show:** System repair rows, waiting rows, monitor-only rows, or diagnostics as user-required actions.

### MANUAL_IB_CAPTURE_READY

**Display label:** Manual IB capture confirmation ready.

**Meaning:** A current-day paper candidate is ready for David to confirm whether it was manually captured in IB, mark it not captured, or defer it.

**When it appears:**

* The candidate is for the requested day and current paper session.
* It passed output-intent, readiness, construction, duplicate, and actionability gates.
* Actions are allowed by the current screen state.

**What the user should do:** Confirm Captured, Mark Not Captured, Defer, or open details.

**Must not show:** Rejected-intent lineage, incomplete candidates, duplicate-suppressed candidates, already-open positions, stale prior-day candidates, or capture buttons when actions are disabled.

### RESEARCH_RUNNING

**Display label:** Research is running.

**Meaning:** A research hypothesis is queued, running, collecting evidence, or waiting for observations without needing user action.

**When it appears:**

* Research execution or validation is active.
* The next progress condition is known.
* No paper-testing sleeve should be created until qualification is met.

**What the user should do:** Monitor progress. Review sample counts and next expected observation.

**Must not show:** Manual review required unless actual policy requires it, or recommendation-ready language that implies a terminal manual gate.

### RESEARCH_UNAVAILABLE

**Display label:** Research is unavailable.

**Meaning:** Research state cannot be trusted or displayed for the requested day.

**When it appears:**

* Research artifacts are missing, stale, or inconsistent.
* The research service or read model is unavailable.
* The requested day cannot be resolved safely.

**What the user should do:** Open System Health or Evidence / Audit Detail for the blocker. No research action is required unless explicitly stated.

**Must not show:** Old hypotheses as current, generic waiting labels, or manual review cards without current evidence.

### PERFORMANCE_UNAVAILABLE

**Display label:** Performance analytics unavailable.

**Meaning:** Performance or sleeve analytics cannot be displayed without misleading the operator.

**When it appears:**

* Marks, P&L, attribution, or semantic invariants are missing or contradictory.
* Full portfolio P&L cannot be treated as canonical.
* Required performance sources are unavailable.

**What the user should do:** Treat analytics as unavailable and inspect the stated reason. Do not use displayed P&L as canonical.

**Must not show:** Unavailable metric grids, canonical-looking cards, `NOT_CANONICAL` mixed with `PASS`, or total P&L presented as reliable when full coverage is missing.

## State-to-Action Rules

* `NORMAL`: normal content may render; actions render only for rows that require action.
* `NO_ACTIVITY`: no action buttons.
* `WAITING_FOR_NEXT_RUN`: no action buttons.
* `BLOCKED`: no primary workflow actions.
* `DEGRADED`: actions may render only if the affected workflow is not compromised.
* `NEEDS_USER_ACTION`: user action buttons may render.
* `MANUAL_IB_CAPTURE_READY`: capture confirmation buttons may render.
* `RESEARCH_RUNNING`: no user action unless explicit.
* `RESEARCH_UNAVAILABLE`: no research workflow actions.
* `PERFORMANCE_UNAVAILABLE`: no metric cards that imply completeness.

## Mapping From Backend Governance

Backend artifacts may help decide state, including runtime truth, surface readiness, semantic invariants, operator surface contract, candidate state, paper ledger, research artifacts, and performance reports.

The visible state must be translated into the operator labels above. Backend vocabulary remains available only in Evidence / Audit Detail.

