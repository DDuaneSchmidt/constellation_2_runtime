# Aegis Performance / System Health Boundary

## Purpose

This document defines the product boundary between Performance and System Health.

```text
Performance = how the portfolio, positions, and strategies performed.
System Health = whether Aegis machinery and required evidence are healthy enough to operate.
```

The boundary prevents analytics pages from becoming repair dashboards and prevents health pages from becoming portfolio reporting pages.

## Core Ownership

### Performance Owns

* returns
* realized P&L
* unrealized P&L
* total P&L
* attribution
* benchmarks
* performance trends
* contributor rankings
* sleeve/strategy performance summaries
* performance data-quality statements

Performance may say analytics are unavailable or partial, but it must not own the repair workflow.

### System Health Owns

* readiness
* runtime truth
* verified graph status
* dependencies
* data availability
* artifact freshness
* source-day consistency
* blockers
* degraded operational dependencies
* repair status
* recovery plans
* verification commands

System Health may reference that Performance is blocked or degraded, but it must not present portfolio analytics as its primary content.

## What Belongs Only In Performance

Only Performance may own:

* portfolio P&L summary
* position performance attribution
* sleeve/strategy performance attribution
* benchmark comparison
* return percentages
* realized/unrealized split
* top/worst contributors
* performance trend summaries
* mark coverage as a performance trust input
* attribution coverage as a performance trust input

## What Belongs Only In System Health

Only System Health may own:

* runtime readiness
* operational blockers
* missing/stale source lists
* producer failures
* scheduler health
* route/API health
* repair plans
* recovery plans
* verification commands
* dependency health
* data-domain readiness
* surface readiness summaries
* semantic contradiction summaries translated into operator language

## What May Appear In Both As Summary

| Item | Performance use | System Health use |
| --- | --- | --- |
| Mark coverage | Whether P&L/attribution can be trusted. | Whether mark data dependency is healthy. |
| Attribution coverage | Whether contributor/sleeve analytics are complete. | Whether attribution dependency needs repair. |
| Data completeness | Performance trust statement. | Operational dependency status. |
| Blocked/degraded label | Explain why analytics cannot render. | Own blocker cause, recovery plan, and verification. |
| Evidence link | Collapsed evidence for performance sources. | Full evidence and repair diagnostics. |
| Source day | Trust context for analytics. | Day/source consistency health. |

Shared summary must not become shared workflow.

## Current Overlap And Confusion

Existing overlap identified by audits and component inventory:

* Performance raw contract panels expose System Health concepts on an analytics page.
* Semantic invariant wording can appear as performance content instead of a trust gate.
* Runtime blocked/degraded state appears in Engineering, Evidence Detail, Header, and audit outputs with inconsistent operator meaning.
* Performance and Sleeve Analytics both report analytics health, while System Health should own the repair path.
* Raw artifact names, hashes, and contract fields appear on pages where operators need plain-language answers.

## Architecture Leakage

The following concepts are useful backend governance but must not be primary Performance content:

* Surface Contract
* Semantic Invariants
* runtime truth labels
* raw contract JSON
* artifact paths and hashes
* source manifest internals

The following concepts may appear in System Health only after translation:

* runtime truth kernel results
* verified graph status
* source freshness
* producer coverage
* recovery plan references

Raw labels belong in Evidence / Audit Detail.

## Duplicated Concepts To Resolve

* Data readiness appears in Today, Performance, Sleeve Analytics, Engineering, and Evidence Detail. System Health owns the operational diagnosis; each business page only shows its local impact.
* Performance health appears on both Performance and Sleeve Analytics. Performance owns portfolio analytics; Sleeve Analytics owns sleeve-level analytics; System Health owns why analytics are blocked.
* Runtime repair appears in Engineering, Runtime Timeline, and Evidence Detail. System Health should be the operator-facing repair surface; Evidence Detail remains forensic.

## Empty / Blocked / Degraded Boundary

### Performance Empty

```text
No performance data is available for this day.
```

Meaning: there are no performance results to report. This is not necessarily a system failure.

### System Health Empty

```text
System health has not been evaluated for this day.
```

Meaning: health evidence itself is missing, which may be a health issue.

### Performance Blocked

```text
Performance cannot be shown because required marks, P&L, or attribution evidence is missing or inconsistent.
```

Performance shows impact and points to System Health for repair.

### System Health Blocked

```text
Aegis is blocked because a required operational dependency is missing, stale, inconsistent, or failed.
```

System Health owns cause, repair status, recovery plan, and verification.

### Performance Degraded

```text
Performance is partially available, but some analytics are incomplete.
```

Performance may show limited metrics with clear caveats.

### System Health Degraded

```text
Aegis is operating with degraded dependencies.
```

System Health shows dependency impact and next recovery expectation.

## What Must Never Appear In Performance

* candidate readiness or capture controls
* research cards or findings
* system repair queue as primary content
* raw contract JSON
* raw semantic invariant names as operator content
* broker/live/autonomous trading controls
* trade advice or execution recommendations

## What Must Never Appear In System Health

* portfolio P&L as primary content
* attribution rankings as primary content
* positions/holdings tables
* candidate actionability workflow
* research findings as primary content
* broker/live/autonomous trading controls
* trade advice or execution recommendations

## How Today Links To Performance And System Health

Today may show:

* high-level P&L summary if trustworthy
* performance unavailable/degraded summary if applicable
* system operating state
* top blocker or degraded dependency
* links to Performance and System Health

Today must not show:

* full performance tables
* repair queues
* raw diagnostics
* detailed attribution

Link rules:

* Performance summary links to Performance.
* Analytics unavailable or data readiness issue links to System Health.
* System blocker links to System Health.
* Evidence links go to Evidence / Audit Detail.

## Screenshot Acceptance Boundary

A Performance screenshot passes only if it answers performance questions without showing repair workflow as primary content.

A System Health screenshot passes only if it answers operational health questions without showing portfolio analytics as primary content.
