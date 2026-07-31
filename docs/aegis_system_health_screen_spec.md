# Aegis System Health Screen Specification

## Purpose

System Health answers:

```text
Can Aegis operate, what is degraded, what is blocked, and what must be repaired?
```

System Health is the operator surface for readiness, dependencies, data availability, blockers, repair ownership, and operational status.

System Health is not:

* performance reporting
* position ownership
* candidate actionability
* research findings
* trading execution
* trade advice

## Primary Operator Questions

### Can Aegis operate?

Required data:

* requested day
* source day
* overall operating state in plain language
* runtime truth readiness summary translated for operators
* verified graph status translated for operators
* safety mode summary
* whether Aegis is monitoring only, paper-review-capable, or blocked

Source system:

* runtime truth kernel
* verified runtime graph
* audit handoff
* mode readiness
* operator surface readiness / operator surface contract

Update frequency:

* after audit
* after runtime truth kernel run
* after readiness repair
* after producer runs

Empty state:

```text
System health has not been evaluated for this day.
```

Blocked state:

```text
Aegis cannot operate this workflow because required evidence is missing, stale, inconsistent, or blocked by policy.
```

Degraded state:

```text
Aegis is partially operational, but some dependencies or evidence are incomplete.
```

User action state:

* Show repair or verification actions only if a real user action is required.
* System repair and verify-only rows must not be counted as David action unless David must actually do something.

### Is required data available?

Required data:

* data readiness summary
* market data availability
* candidate data availability
* position/mark data availability
* research data availability
* stale/missing source count
* source-day mismatches

Source system:

* runtime truth kernel source manifest
* surface readiness
* semantic invariants
* domain certification / data registry
* producer contract coverage

Update frequency:

* after data producer runs
* after surface readiness and semantic invariant checks
* after audit

Empty state:

```text
Data readiness has not been evaluated.
```

Blocked state:

```text
Required data is missing or stale.
```

Degraded state:

```text
Some data is available, but one or more domains are delayed, stale, or incomplete.
```

User action state:

* Usually no operator action; repair ownership must be explicit.

### What is degraded?

Required data:

* degraded surfaces
* degraded data domains
* degraded producers
* degraded capabilities
* impact statement for each degradation

Source system:

* engineering priority queue
* surface readiness
* semantic invariants
* domain certification
* audit handoff

Update frequency:

* after audit and health generation

Empty state:

```text
No degraded dependencies are reported.
```

Blocked state:

```text
Degradation status cannot be evaluated.
```

Degraded state:

```text
Some dependencies are degraded but do not fully block operation.
```

User action state:

* Only show David action when the degradation requires a human decision or manual input.

### What is blocked?

Required data:

* active blockers
* blocker classification
* cause
* affected workflow/surface
* impact
* repair status
* recovery plan or repair command if available
* verification command

Source system:

* engineering priority queue
* runtime truth kernel blocker states
* audit handoff
* recovery plan
* surface readiness

Update frequency:

* after audit
* after runtime truth kernel
* after repair attempts

Empty state:

```text
No active blockers are reported.
```

Blocked state:

```text
One or more required dependencies block operation.
```

Degraded state:

```text
Blocker details are partial; inspect evidence before repair.
```

User action state:

* User action only if a row explicitly says David must perform or approve something.
* Verify-only rows are not user action required.

### What dependencies are unhealthy?

Required data:

* producer health
* artifact freshness
* route/API health
* scheduler health
* data domain health
* service restart status if applicable

Source system:

* runtime manifest / supervisor status
* audit outputs
* domain certification
* runtime timeline
* engineering priority queue

Update frequency:

* after supervisor checks
* after audit
* after scheduled producer runs

Empty state:

```text
Dependency health has not been evaluated.
```

Blocked state:

```text
Dependency health cannot be trusted because the health check itself failed.
```

Degraded state:

```text
One or more dependencies are unhealthy or delayed.
```

User action state:

* Repair/verify only when real repair ownership exists.

### What must be repaired?

Required data:

* fix-first list
* repair status
* repair command if a real repair exists
* recovery plan if no direct repair exists
* verification command
* repair owner

Source system:

* engineering priority queue
* recovery plan
* repair semantics report
* audit handoff

Update frequency:

* after audit
* after repair queue generation

Empty state:

```text
No repair action is currently required.
```

Blocked state:

```text
Repair status cannot be determined.
```

Degraded state:

```text
Repair details are incomplete; inspect evidence before acting.
```

User action state:

* Show user action only when the user must do something.
* Otherwise show system repair, waiting, or verify-only as separate categories.

## Required Screen Sections

### Section 1: Operating Status

Purpose: answer whether Aegis can operate today.

Operator question answered: Can Aegis operate?

Required data: overall health, mode, safety posture, top blocker.

Value score: 10

Classification: NEW

Justification: System Health must lead with plain operational status, not raw graphs.

### Section 2: Fix First

Purpose: show the highest-priority repair or blocker.

Operator question answered: What must be repaired first?

Required data: issue, cause, impact, repair status, repair/recovery plan, verify step.

Value score: 10

Classification: IMPROVE

Justification: previous Engineering work identified this as the core troubleshooting workflow.

### Section 3: Active Blockers

Purpose: list true blockers only.

Operator question answered: What is blocked?

Required data: blocker, cause, affected workflow, impact, next step.

Value score: 9

Classification: IMPROVE

Justification: blockers must not be mixed with warnings or informational diagnostics.

### Section 4: Degraded Dependencies

Purpose: show degraded but still functioning dependencies.

Operator question answered: What is degraded?

Required data: dependency/domain/surface, issue, impact, next expected recovery.

Value score: 8

Classification: NEW

Justification: degradation is operationally important but should not appear as a hard blocker.

### Section 5: Data Availability

Purpose: explain whether required data is available.

Operator question answered: Is required data available?

Required data: domain readiness, missing/stale counts, source-day mismatch counts.

Value score: 9

Classification: IMPROVE

Justification: current data readiness appears across multiple pages; System Health should own it.

### Section 6: Run and Dependency Health

Purpose: show whether jobs, producers, routes, and services are healthy.

Operator question answered: What dependencies are unhealthy?

Required data: last successful run, next expected run, route/service health, producer failures.

Value score: 8

Classification: IMPROVE

Justification: runtime timeline data is useful but must be summarized before detail.

### Section 7: Collapsed Evidence

Purpose: provide raw evidence for engineers without making it the operator experience.

Operator question answered: Why should I trust this health state?

Required data: audit paths, graph status, runtime truth, recovery plan, source refs.

Value score: 6

Classification: IMPROVE

Justification: evidence supports trust but should remain secondary.

## State Model

### HEALTHY

Visible message:

```text
Aegis is operational for the requested day.
```

Operator expectation: Monitor normally. No repair action required.

Components shown: Operating Status, Data Availability, Run and Dependency Health, Collapsed Evidence.

Components hidden: Fix First if empty, Active Blockers if empty.

### DEGRADED

Visible message:

```text
Aegis is operating with degraded dependencies.
```

Operator expectation: Continue monitoring with the stated limitations.

Components shown: Operating Status, Degraded Dependencies, Data Availability, Run and Dependency Health, Collapsed Evidence.

Components hidden: repair command buttons unless a real repair exists.

### BLOCKED

Visible message:

```text
Aegis is blocked for one or more workflows.
```

Operator expectation: Read Fix First, then inspect repair/recovery and verification steps.

Components shown: Operating Status, Fix First, Active Blockers, Data Availability, Run and Dependency Health, Collapsed Evidence.

Components hidden: performance/candidate/research business content, raw contract JSON as primary content.

### REPAIR_NEEDED

Visible message:

```text
A repair or recovery plan is required.
```

Operator expectation: Use the named repair plan or verify command. Do not assume the page itself repairs Aegis.

Components shown: Fix First, repair status, repair command if real, recovery plan if repair unavailable, verification command.

Components hidden: verify-only commands labeled as repair, system repair rows counted as David action.

## Components That Must Never Appear On System Health

* P&L charts or performance rankings
* positions/holdings tables
* candidate actionability tables
* research findings cards
* candidate capture controls
* broker/live/autonomous trading controls
* raw surface contract JSON as primary content
* semantic invariant labels as the main answer
* buy/sell/exit/increase/reduce language

## Screenshot Acceptance

A System Health screenshot passes only if a non-engineer can answer:

* Can Aegis operate?
* What is broken?
* What is degraded?
* What should happen next?

without reading raw artifact names or diagnostics dumps.
