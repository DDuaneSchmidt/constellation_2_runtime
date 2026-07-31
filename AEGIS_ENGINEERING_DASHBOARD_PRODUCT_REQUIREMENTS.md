# Aegis Engineering Dashboard Product Requirements

## Purpose

The Engineering Dashboard exists to answer:

```text
What is broken?
Why is it broken?
What should be fixed first?
What evidence proves that?
```

The Engineering Dashboard is not:

* a generic artifact browser
* a raw diagnostic dump
* a debugging scratchpad

The Engineering Dashboard is the primary operational troubleshooting surface for Aegis.

## Core Principle

Current anti-pattern:

```text
Artifact
-> Artifact
-> Artifact
-> Artifact
```

Desired pattern:

```text
Fix First
-> Ask Aegis
-> Understand Cause
-> Repair
-> Verify
```

The Engineering Dashboard is a troubleshooting cockpit, not a metrics dashboard. The operator should not need to understand internal artifact names to identify the next repair action or verification step.

## Diagnosis / Repair / Verification Boundary

The Engineering Dashboard must distinguish:

```text
Diagnosis
Repair
Verification
```

A command that only audits, verifies, or reports status must never be shown as a repair command.

If no true repair command is available, the UI must say:

```text
Repair command unavailable.
Use Ask Aegis or inspect recovery plan.
```

Every issue must expose:

* Problem
* Cause
* Impact
* Repair Status
* Repair Command, if real
* Verification Command
* Ask Aegis
* Evidence summary
* Raw evidence, collapsed

Repair status must use one deterministic value:

```text
REPAIR_AVAILABLE
REPAIR_UNAVAILABLE
REPAIR_NEEDS_RECOVERY_PLAN
REPAIR_MANUAL_INVESTIGATION
VERIFY_ONLY
```

Self-checks and tests must fail if:

* repair_command equals verification_command
* an audit, status, smoke, report, or self-check command is labeled as a repair
* "Use recovery plan" appears without a linked or named recovery plan
* Copy Command appears for a verify-only command as if it repairs the issue

## Primary Operator Questions

The Engineering Dashboard must answer within 10 seconds:

1. What is currently broken?
2. What is currently degraded?
3. What should be fixed first?
4. What is waiting on data?
5. What is waiting on time?
6. What is waiting on operator action?
7. What evidence supports that conclusion?

If these questions are not answered, the dashboard is incomplete.

## Dashboard Structure

Required product hierarchy:

1. Fix First / Top Issue
2. Ask Aegis for the top issue
3. Operator Action Queue
4. Engineering Status Summary
5. Day Clarity
6. Active Blockers
7. Degraded but Functional
8. Run Health
9. Diagnostics, collapsed by default

Status cards must not appear above the top issue. The first visible operational content must be the highest-priority issue.

### Section 1 - Fix First / Top Issue

The top issue must be visible without scrolling on a standard desktop viewport. It must show:

* problem
* plain-English cause
* impact
* repair status
* repair command, if a true repair exists
* verification command
* Ask Aegis action
* evidence summary
* source artifacts, collapsed

The Top Issue card owns the main troubleshooting flow. It must not require the operator to inspect raw diagnostics before understanding what is broken and what to do first.

### Section 2 - Ask Aegis for the Top Issue

Ask Aegis must be integrated into the issue workflow. For the top issue and every Fix First row, show:

* Ask Aegis: Explain this issue
* Ask Aegis: What should be fixed first?

Ask Aegis must open with issue context prefilled. Ask Aegis must appear before Copy Repair Command.

Button order for issue rows:

1. Ask Aegis: Explain this issue
2. Ask Aegis: What should be fixed first
3. Copy Repair Command, only if a true repair command exists
4. Copy Verify Command

### Section 3 - Operator Action Queue

The Operator Actions Required count must never be detached from the queue. If the page says:

```text
Operator Actions Required: N
```

then the operator must be able to see or expand exactly N actions.

Each row must classify action type:

```text
USER_ACTION
SYSTEM_REPAIR
WAITING_FOR_DATA
WAITING_FOR_TIME
VERIFY_ONLY
```

Do not call system repair or verify-only rows "operator action required" unless the user must actually do something. If actions are system-side, label the summary:

```text
System repair actions
```

not:

```text
Operator actions required
```

Each action row must show:

* action
* why it exists
* source issue
* whether user action is truly required
* severity
* Ask Aegis action
* repair/verification command if available

### Section 4 - Engineering Status Summary

Status cards are secondary. They summarize, not lead. Required cards:

* Graph Validation
* Runtime Readiness
* Blocking Issues
* Warnings
* Operator Actions Required
* Data Readiness
* Last Successful Run

If Graph Validation is READY but Runtime Readiness is BLOCKED, the page must say:

```text
Graph validation passed, but runtime readiness is blocked because required evidence is missing or stale.
```

### Section 5 - Day Clarity

Dashboard must always display:

```text
Requested Day
Source Day
Operational Day
```

If they differ, show:

```text
Viewing historical operational session.
```

Never display ambiguous:

```text
latest valid session
```

without explicit day context.

### Section 6 - Active Blockers

Only true blockers. Examples:

```text
Missing artifact
Failed producer
Data unavailable
Runtime failure
Schema mismatch
```

Do not show informational warnings here.

### Section 7 - Degraded but Functional

Issues that do not prevent operation. Examples:

```text
Partial telemetry
Missing benchmark
Delayed report
```

### Section 8 - Run Health

Show:

* last run
* last successful run
* next expected run
* run status
* missing run visibility

### Section 9 - Diagnostics

Collapsed by default. Raw engineering details belong here.

## Problem Classification

Every issue must have exactly one classification.

Allowed:

```text
BLOCKING
DEGRADED
WAITING_FOR_DATA
WAITING_FOR_TIME
WAITING_FOR_OPERATOR
INFORMATIONAL
```

No issue may be unclassified.

## Priority Classification

Every issue must have priority:

```text
P0
P1
P2
P3
```

Definitions:

P0
System unusable.

P1
Core workflow impaired.

P2
Functionality degraded.

P3
Informational.

## Fix First Engine

Create:

```text
aegis_engineering_priority_queue_v1
```

Purpose:

Deterministically rank engineering issues.

Output:

* issue
* classification
* priority
* plain-English problem
* plain-English cause
* impact
* evidence
* repair action
* repair status
* repair command
* verification command
* recovery plan id or path, if applicable
* action type
* human-readable evidence summary
* raw evidence, collapsed
* repair owner
* Ask Aegis issue prompt

The dashboard reads this queue.

## Ask Aegis Integration

Engineering Dashboard must expose:

```text
Ask Aegis
```

directly and as the primary path for understanding issues before copying commands.

Operator examples:

```text
Why is this blocked?
Why did this run fail?
What should be fixed first?
What changed since yesterday?
```

Engineering Dashboard should be the most powerful consumer of the AI Operations Assistant.

## Navigation Requirements

Engineering must behave like a normal workspace.

Requirements:

* active state visible
* child routes visible
* active route highlighted
* same visual treatment as Command Center

No drawer-only navigation state.

## Navigation Label Requirements

Engineering left-nav sublabel must be operationally useful. Allowed examples:

* `1 blocker`
* `Runtime blocked`
* `All clear`

Forbidden:

* `Drawer open`
* implementation-only drawer state

## Layout Requirements

Avoid large unused whitespace. The primary engineering content should occupy the center of the screen. The top issue, Ask Aegis controls, and first repair action must be visible without scrolling.

Raw diagnostics and raw labels must remain collapsed.

## Diagnostics Requirements

Diagnostics must be hidden by default.

Operators should see:

```text
What matters.
```

Engineers may expand:

```text
Evidence.
```

## Evidence Requirements

Every issue must expose:

* evidence source
* artifact
* timestamp
* confidence
* human-readable supporting data first
* raw evidence labels only in collapsed details

No issue may be presented without evidence.

Primary evidence must be human-readable first.

Example:

```text
13 required runtime sources are stale or missing.
```

Raw labels like:

```text
runtime_truth_classification=PARTIAL_CONTEXT
```

belong only in collapsed raw evidence.

## Acceptance Criteria

The operator should be able to answer within 10 seconds:

1. What is the top issue?
2. Why is it happening?
3. What should I do first?
4. How do I verify the fix?
5. What evidence supports this?

The operator should not need to:

* inspect raw JSON
* inspect artifact paths
* inspect runtime internals

to understand the answer.

## Governance Rule

Engineering Dashboard is an operational troubleshooting surface.

It must prioritize:

```text
Fix First
-> Ask Aegis
-> Understand Cause
-> Repair
-> Verify
```

over artifact browsing or metric-first dashboard layouts.

`aegis/modules/operator_portal/aegis.module.yaml` must reference this document as the product authority for the Engineering Dashboard.
## Operator UI Architecture Authority

This document is subordinate to `AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md` for rendering architecture. Pages must consume the Operator Surface Contract through shared templates before rendering page-specific content.

