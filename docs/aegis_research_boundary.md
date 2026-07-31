# Aegis Research Product Boundary

## Purpose

This document defines the product boundary for the rebuilt Research screen.

Research answers:

```text
What is Aegis investigating, what has it learned, what is blocked, and what happens next?
```

Research is not a position, candidate, performance, or system-health workflow.

## Core Boundary

```text
Research = what Aegis is trying to learn.
Candidates = what Aegis might own next.
Positions = what Aegis owns now.
Performance = how owned positions and sleeves performed.
System Health = whether Aegis machinery is running and trustworthy.
```

The Research screen may reference other areas only as context. It must not own their workflows.

## What Belongs Only In Research

Only Research may own:

* investigations
* hypotheses
* research programs
* experiment status
* research task status
* evidence collection progress
* research findings
* research briefs
* qualification status for paper validation
* paper-testing sleeve status as research validation context
* research blockers
* next research steps
* whether operator research review or follow-up is required

Research may show affected symbols only as research context. It must not present affected symbols as trade candidates or position holdings.

## What Belongs Only In Candidates

Only Candidates may own:

* candidate universe
* actionable candidates
* non-actionable candidates
* candidate qualification/readiness for operator review
* candidate blockers
* candidate capture or review actions
* next candidate evaluation timing
* signal/contract/construction/duplicate boundary outcomes for candidate actionability

Research may say that a hypothesis has symbols under study, but Candidates owns whether any symbol is actionable as a candidate.

## What Belongs Only In Positions

Only Positions may own:

* open positions
* holdings
* exposure
* entry dates and prices
* current value
* mark coverage for owned positions
* realized/unrealized P&L for owned positions
* concentration/risk summary for owned positions
* position review links

Research may say a hypothesis affects a symbol also held in the portfolio, but Positions owns the position state and risk view.

## What Belongs Only In Performance

Only Performance may own:

* portfolio performance
* benchmark comparison
* position attribution
* sleeve performance
* realized/unrealized P&L reporting
* mark coverage as performance data quality
* performance analytics status

Research may use historical outcome evidence as research evidence, but Performance owns operator reporting on portfolio results.

## What Belongs Only In System Health

Only System Health may own:

* runtime readiness
* missing/stale artifacts
* producer failures
* scheduler health
* data readiness
* repair plans
* audit status
* route/API health

Research may display a research-specific blocker that depends on missing evidence, but System Health owns the operational repair workflow.

## Shared Summary Rules

The following may appear in Research as summary only:

| Item | Research use | Owning screen |
| --- | --- | --- |
| Symbol | Affected or studied symbol. | Candidates/Positions depending on workflow. |
| Sleeve | Paper-testing or research validation context. | Sleeve Analytics/Performance for performance, Research for validation. |
| Open position mention | Context that research may affect an owned symbol. | Positions. |
| Candidate mention | Context that a hypothesis may produce future candidates. | Candidates. |
| Performance result | Evidence for or against a hypothesis. | Performance. |
| Data blocker | Research-specific reason work cannot progress. | System Health owns repair. |

Shared summary must not become shared workflow.

## Forbidden Research Content

Research must never show as primary content:

* open positions table
* holdings summary
* portfolio exposure
* position P&L
* position review prose
* candidate capture workflow
* candidate action buttons
* candidate readiness tables as primary content
* performance dashboards
* engineering diagnostics as primary content
* broker submit/transmit controls
* live trading controls
* autonomous trading controls
* buy/sell/exit/increase/reduce recommendations
* raw artifact paths or internal schema names as the main operator experience

## Relationship To Today

Today / Command Center may summarize Research as:

* active research count
* blocked research count
* findings ready count
* whether operator action is required
* next research step
* link to Research

Today must not show research cards, hypothesis detail, review briefs, or evidence traces as primary content.

## Blocked And Empty State Differences

### Research Empty

Research empty means:

```text
No active research investigations are recorded for this day.
```

### Candidate Empty

Candidate empty means:

```text
No candidates qualified for review today.
```

### Position Empty

Position empty means:

```text
No open positions are recorded.
```

### Performance Unavailable

Performance unavailable means:

```text
Portfolio analytics cannot be trusted from available marks/attribution.
```

### System Health Blocked

System Health blocked means:

```text
An operational dependency is missing, stale, inconsistent, or failed.
```

## Screenshot Acceptance Boundary

A Research screenshot passes the boundary only if a reviewer can tell:

* what investigations exist
* what is active
* what has been learned
* what is blocked
* what happens next
* whether operator action is required

without seeing holdings, candidate capture controls, portfolio P&L dashboards, or raw system diagnostics as the primary content.
