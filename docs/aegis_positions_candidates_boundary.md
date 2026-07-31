# Aegis Positions / Candidates Product Boundary

## Purpose

This document defines the product separation between Positions and Candidates.

The boundary exists to prevent the operator from confusing current ownership with possible future ownership.

```text
Positions = what we own now.
Candidates = what we might own next.
Today = high-level summary and links only.
```

## Core Rule

Positions and Candidates must not own each other's workflow.

Positions may show candidate provenance only when it explains an existing position.

Candidates may show open-position conflicts only when they explain candidate actionability.

Neither screen may use the other's data as primary content.

## Operator Questions

| Screen | Primary question | Secondary questions |
| --- | --- | --- |
| Positions | What do we currently own? | What is the exposure? Are marks/P&L trustworthy? What position should be monitored? |
| Candidates | What might we own next? | Which candidates are actionable? Which are blocked? What happens next? |
| Today / Command Center | What happened today, and do I need to do anything? | How many positions/candidates exist? Where should I go next? |

## What Belongs Only In Positions

Only Positions may own:

* open positions
* holdings
* position quantity
* entry dates and entry prices
* current position value
* unrealized and realized P&L if available
* mark coverage and stale price warnings for owned positions
* portfolio exposure and concentration
* sleeve attribution for existing positions
* position data quality
* position review links
* entry receipt provenance for existing positions
* closed/recent position context when shown as position history

Positions must explain incomplete values as ownership data-quality problems, not candidate blockers.

## What Belongs Only In Candidates

Only Candidates may own:

* candidate universe
* output-intent candidates
* actionable candidates
* non-actionable candidates
* candidate qualification status
* candidate readiness status
* signal evidence boundary outcomes
* contract generation boundary outcomes
* construction boundary outcomes
* candidate market-data readiness
* duplicate candidate classification
* candidate blockers
* candidate next evaluation time
* candidate review/capture actions when allowed
* rejected or excluded candidate lineage as diagnostics/history

Candidates must explain open-position conflicts as candidate actionability blockers, not as holdings or exposure.

## What May Appear In Both As Summary

The following may appear on both screens only as summary or provenance:

| Item | Positions use | Candidates use |
| --- | --- | --- |
| Symbol | Identifies owned position. | Identifies possible future position. |
| Sleeve | Attributes existing position. | Explains candidate source or intended sleeve. |
| Source day | Confirms current/historical ownership evidence. | Confirms current/historical candidate evidence. |
| Data-quality badge | Marks/P&L/attribution trust. | Candidate readiness/evidence trust. |
| Evidence link | Position evidence. | Candidate evidence. |
| Candidate id | Provenance for an existing position only. | Candidate row identity/detail. |
| Open-position conflict | Not primary; may appear as provenance. | Candidate blocker/duplicate reason. |
| Count summary | Open position count. | Candidate/actionable count. |

Shared summary must not become shared workflow.

## What Must Never Appear In Either Screen

Positions and Candidates must never show:

* live broker submit/transmit controls
* autonomous trading controls
* trade advice language
* buy/sell/exit/increase/reduce execution recommendations
* raw contract JSON as primary content
* raw semantic invariant or surface contract vocabulary as primary content
* unrelated Research Lab cards as primary content
* Engineering diagnostics as primary content
* stale or wrong-day actionable rows
* action buttons when actionability is not proven for the requested day
* unsupported current-day claims from prior-day artifacts
* Today / Command Center workflow summaries as the primary page body

Positions must never show candidate capture or candidate qualification workflow.

Candidates must never show holdings, P&L, exposure, or position review prose as primary content.

## How Today Links To Positions And Candidates

Today / Command Center remains the daily start page.

Today may show:

* open position count
* position data-quality summary
* candidate count
* actionable candidate count
* whether David action is required
* next scheduled run
* links to Positions and Candidates

Today must not show:

* full positions table
* full candidate table
* candidate workflow detail
* position review detail
* candidate capture controls
* holdings/P&L detail

Link rules:

* Open Positions summary links to Positions.
* Candidate summary links to Candidates.
* Position data-quality issue links to Positions or Evidence/System Health.
* Candidate blocker links to Candidates or Evidence/System Health.

## Empty, Blocked, Degraded, And User-Action States

### Positions

Empty means:

```text
No open paper positions are recorded for this day.
```

Blocked means:

```text
Current ownership cannot be trusted because the position ledger, marks, or source day is missing, stale, or inconsistent.
```

Degraded means:

```text
Positions exist, but marks, P&L, sleeve attribution, or review evidence is incomplete.
```

User action means:

* view detail
* view review
* inspect evidence

It does not mean candidate capture or trade execution.

### Candidates

Empty means:

```text
No candidates qualified for review today.
```

or:

```text
Candidate generation has not run yet. Waiting for the next scheduled run.
```

Blocked means:

```text
Candidate workflow cannot be trusted because candidate generation, lifecycle, readiness, market data, signal evidence, contract generation, construction, or duplicate evidence is missing, stale, or inconsistent.
```

Degraded means:

```text
Candidate summary exists, but some readiness or explanation evidence is incomplete.
```

User action means:

* candidate review/capture action only when current-day actionability is proven.
* otherwise view detail/evidence only.

It does not mean position review, P&L inspection, or portfolio exposure.

## Component Boundary Matrix

| Component | Belongs in Positions | Belongs in Candidates | May appear in Today summary | Notes |
| --- | --- | --- | --- | --- |
| Open positions table | Yes | No | No | Core Positions content. |
| Holdings/exposure summary | Yes | No | Count/quality only | No exposure on Candidates. |
| Entry dates/prices | Yes | No | No | Candidate planned entry belongs to Candidates; actual entry belongs to Positions. |
| Current value/P&L | Yes | No | High-level summary only | Candidates must not show portfolio performance. |
| Mark quality warnings | Yes | Only candidate market-data readiness | Summary only | Different meanings by screen. |
| Candidate actionable queue | No | Yes | Count/link only | Candidate actions never on Positions. |
| Candidate qualification/readiness | No | Yes | Summary only | Positions may show candidate id as provenance only. |
| Duplicate candidate blocker | No | Yes | Summary only | Open-position duplicate can reference existing position without showing holdings. |
| Position Review content | Link only | No | Link only | Review details stay on Position Review. |
| Evidence drawer | Yes, collapsed | Yes, collapsed | Link only | Raw artifacts never primary. |

## Proposed Boundary Components

### Component: Positions Ownership Summary

Purpose: Define the top of Positions around owned positions only.

Operator question answered: What do we currently own?

Data source: position ledger, current marks, sleeve attribution, P&L report.

Empty state: No open paper positions recorded.

Blocked state: Position state unavailable due missing/stale ledger or marks.

Degraded state: Positions visible with incomplete marks/P&L/attribution.

User action state: View details/review/evidence only.

Classification: NEW

Value score: 10

### Component: Candidates Opportunity Summary

Purpose: Define the top of Candidates around future opportunities only.

Operator question answered: What might we own next?

Data source: candidate state, lifecycle projection, readiness, queue audit.

Empty state: No candidates qualified today or waiting for generation.

Blocked state: Candidate workflow unavailable due missing/stale candidate evidence.

Degraded state: Candidate summary visible with incomplete readiness explanations.

User action state: Candidate review/capture only when current-day actionability is proven.

Classification: NEW

Value score: 10

### Component: Today Cross-Link Summary

Purpose: Let Today route the operator to the right detailed screen without importing the details.

Operator question answered: Where should I go next?

Data source: `/api/aegis/operator/today` envelope summaries for positions and candidates.

Empty state: No action required; links remain available.

Blocked state: Link to System Health/Evidence if target surface unavailable.

Degraded state: Link with limitation reason.

User action state: Navigate only.

Classification: IMPROVE

Value score: 8

### Component: Shared Evidence Access

Purpose: Provide auditability without mixing product workflows.

Operator question answered: Why should I trust this screen?

Data source: screen-specific evidence refs and source hashes.

Empty state: Hidden when no evidence exists.

Blocked state: Shows evidence missing reason.

Degraded state: Shows partial evidence limitation.

User action state: Expand evidence.

Classification: KEEP

Value score: 6

### Component: Candidate Workflow On Positions

Purpose: Candidate capture and review.

Operator question answered: None for Positions.

Data source: candidate workflow artifacts.

Empty state: Not applicable.

Blocked state: Not applicable.

Degraded state: Not applicable.

User action state: Forbidden on Positions.

Classification: REMOVE

Value score: 1

### Component: Holdings/P&L On Candidates

Purpose: Position ownership/performance display.

Operator question answered: None for Candidates.

Data source: position ledger, P&L report, performance artifacts.

Empty state: Not applicable.

Blocked state: Not applicable.

Degraded state: Not applicable.

User action state: Forbidden on Candidates.

Classification: REMOVE

Value score: 1

## Screenshot Acceptance Requirements

### Positions screenshot must prove:

1. The first visible content answers what is currently owned.
2. Candidate capture, candidate qualification, manual IB capture, and candidate blockers do not appear as primary content.
3. Open positions, exposure, entry dates, current value, P&L if available, and mark quality are visible or explicitly unavailable.
4. No candidate action buttons appear.
5. No trade advice, broker submit, live trading, or autonomous execution controls appear.
6. Evidence is secondary/collapsed.

### Candidates screenshot must prove:

1. The first visible content answers what might be owned next.
2. Actionable and non-actionable candidates are separated.
3. If nothing is actionable, the reason and next evaluation state are visible.
4. Holdings, P&L, portfolio exposure, performance, and position review content do not appear as primary content.
5. Candidate actions appear only when current-day actionability is proven.
6. Evidence is secondary/collapsed.

### Today screenshot must prove:

1. Today summarizes Positions and Candidates without owning either detail workflow.
2. Today links to Positions for ownership details.
3. Today links to Candidates for opportunity details.
4. Today does not render position tables, candidate tables, capture workflow, or position review prose as primary content.
