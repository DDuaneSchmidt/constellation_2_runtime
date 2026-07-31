# Aegis Command Center UX Requirements

## Purpose

Define the operator-facing UX requirements for the Aegis Command Center so the page is readable, low-scroll, action-oriented, and suitable for daily operation.

## Problem

The current Command Center is difficult to use because:

* candidate cards are too tall
* too much vertical scrolling is required
* key decisions are buried
* repeated fields consume space
* actions are not visually prioritized
* the page reads like raw workflow state instead of an operator command surface

## Core Principle

Command Center must answer within 10 seconds:

1. What needs my attention?
2. What is safe to ignore?
3. What action, if any, should I take?
4. Why?

The page must prioritize operator decisions over raw system detail.

## Required Layout

Use a compact command layout:

1. Header summary
2. Needs Attention queue
3. Candidate Review table
4. Collapsed detail drawer
5. Diagnostics collapsed by default

Do not render every candidate as a tall card.

## Header Summary

Top summary cards must show only:

* Open Positions
* Unrealized P&L
* Candidates Awaiting Review
* Needs Attention
* Mode Readiness

Each card must link to the relevant section.

## Candidate Review Table

Replace tall candidate cards with a compact table.

Required columns:

* Symbol
* Sleeve
* Entry
* State
* Why shown
* Recommended operator action
* Last updated
* Actions

Rows should be one line by default.

No row should exceed two lines unless expanded.

## Row Expansion

Clicking a row opens an inline drawer or side panel.

Expanded details may show:

* thesis
* signal summary
* evidence
* receipts
* diagnostics
* raw IDs

Raw IDs must not appear in collapsed table rows.

## Action Priority

Primary action must be visually obvious.

Allowed visible primary actions:

* Confirm Captured
* Mark Not Captured
* Defer
* View Review
* View Details

Do not show low-priority debug actions in the main row.

## Readability Requirements

* Avoid large full-width cards for repeated candidates.
* Avoid duplicated labels.
* Avoid raw pipeline language in primary view.
* Keep row height compact.
* Use plain language for why the candidate is shown.
* Show internal/debug fields only in expanded details.

## Scrolling Requirements

For a normal daily load of up to 50 candidates:

* The operator should see at least 8 candidate rows without scrolling on a standard desktop viewport.
* Header summary should remain visible or easily reachable.
* Candidate actions should not require opening multiple panels unless detail is needed.

## Empty / No-Action States

If no operator action is required, show:

```text
No operator action required.
Aegis is monitoring automatically.
```

Do not show empty queues as failures.

## Awaiting Review Semantics

The page must distinguish:

* Action required from operator
* No action required
* Waiting for system
* Already captured
* Duplicate suppressed
* Research only
* Diagnostics only

Do not label a row “Awaiting Review” unless an operator decision is actually required.

## Diagnostics

Diagnostics must be collapsed by default.

Diagnostics should be categorized:

* Performance
* Candidate workflow
* Runtime health
* Research
* Sleeve evaluation

Do not mix runtime engineering warnings into the primary Command Center queue unless they require operator action.

## UX Acceptance Criteria

Add UI tests proving:

* Candidate review renders as compact table.
* At least 8 rows are visible in default viewport.
* Raw IDs are hidden unless row expanded.
* Diagnostics are collapsed by default.
* “Awaiting Review” appears only when operator action is required.
* Primary action appears in the Actions column.
* No candidate card layout remains for daily candidate queue.
* Empty state says “No operator action required” when appropriate.

## Governance Rule

Command Center is the daily operator surface.

It must not become a raw artifact/debug page.

Raw details belong behind expansion, diagnostics, or Engineering routes.

Update:

```text
aegis/modules/operator_portal/aegis.module.yaml
```

to reference this document as the product authority for Command Center UX.
## Operator UI Architecture Authority

This document is subordinate to `AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md` for rendering architecture. Pages must consume the Operator Surface Contract through shared templates before rendering page-specific content.

