# Aegis Operator UI Architecture Requirements

## Purpose

Define the top-level UI architecture for every Aegis operator-facing page.

This document is the highest UI authority. It governs how operator pages consume truth, render status, show empty states, and expose Ask Aegis.

## Authority Hierarchy

```text
UI Architecture
  -> Operator Surface Contract
      -> Page Requirements
          -> Page Implementations
```

If a page requirement or implementation conflicts with this document, this document wins.

## Core Architecture

```text
Canonical Artifacts
    -> Surface Readiness
    -> Semantic Invariants
    -> Operator Surface Contract
    -> Shared UI Templates
    -> Page Renderers
```

No page may render primary content without going through a shared template.

## Shared Templates

### OperatorInboxTemplate

Used by Command Center.

Purpose: What requires operator attention?

Required sections:

* Status
* Attention Queue
* Ask Aegis
* Diagnostics

No legacy candidate diagnostics may appear above the attention queue.

### EntityListTemplate

Used by Positions, History, and Research Queue.

Purpose: What entities exist today?

Empty state must come only from the Operator Surface Contract.

### AnalyticsTemplate

Used by Performance and Sleeve Analytics.

Purpose: How did something perform?

If `metrics_allowed=false`, render the contract state instead of analytics cards.

### ReviewTemplate

Used by Position Review and Research Review.

Purpose: What matters most?

Unavailable states must use contract rendering. Raw review shells are forbidden as primary content.

### TroubleshootingTemplate

Used by Engineering.

Purpose: What is broken? Why? How do I repair it?

## Contract Ownership

The Operator Surface Contract owns:

* status
* reason
* impact
* next_step
* actions_allowed
* metrics_allowed
* diagnostics_allowed
* ask_aegis_prompt

Pages may not implement local versions of these fields.

Forbidden:

```javascript
if (openPositions === 0) {
  showCustomEmptyState();
}
```

Required:

```javascript
renderContractState(contractRow);
```

## Non-READY Behavior

For these statuses:

```text
DEGRADED
BLOCKED
UNAVAILABLE
HISTORICAL
INCONSISTENT
```

The page must render only:

* Status
* Reason
* Impact
* Next Step
* Ask Aegis
* Diagnostics collapsed

Legacy workflow sections must not appear first.

## READY Behavior

READY pages may render primary workflow content only after a visible contract-owned status banner appears first.

Action buttons may render only if:

```text
actions_allowed=true
```

Analytics cards may render only if:

```text
metrics_allowed=true
```

## Ask Aegis

Ask Aegis is surface-aware through `ask_aegis_prompt` from the Operator Surface Contract.

Examples:

* Command Center: Why is no operator action required today?
* Performance: Why are analytics unavailable?
* Engineering: Why is runtime blocked?
* Research: Why is this hypothesis waiting?

## Route Inventory

Every route must be represented in:

```text
aegis_operator_route_inventory_v1
```

Each route declares:

* route_id
* surface_id
* template_type
* contract_required
* status_source

No orphan routes are allowed.

## Visual Golden Tests

Acceptance must verify visible text, not hidden DOM attributes only.

For each operator route, tests must verify:

1. First visible message
2. Status
3. Reason
4. Impact
5. Next Step
6. Ask Aegis visibility
7. Diagnostics collapsed
8. Legacy workflow hidden when not READY

## Safety

This architecture does not enable:

* trade advice
* broker execution
* broker submit/transmit
* live trading
* autonomous live trading

Aegis UI explains and displays governed truth only.
