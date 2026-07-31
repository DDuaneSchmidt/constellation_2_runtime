# Aegis Sleeve Analytics Product Requirements

## Purpose

Define the product-level purpose, operator decisions, and user experience for Sleeve Analytics.

This document is about what Sleeve Analytics should help the operator decide, not how metrics are calculated.

This document is subordinate to:

* `AEGIS_OPERATOR_WORKFLOW.md`
* `AEGIS_SLEEVE_ANALYTICS_REQUIREMENTS.md`

It should not duplicate formula or artifact details already covered in `AEGIS_SLEEVE_ANALYTICS_REQUIREMENTS.md`.

## Product Purpose

Sleeve Analytics exists to help the operator evaluate which Aegis sleeves are working, which are degrading, and which need more history before judgment.

It should answer:

* Which sleeves are making money?
* Which sleeves are losing money?
* Which sleeves are producing useful candidates?
* Which sleeves are producing poor candidates?
* Which sleeves are too new to judge?
* Which sleeves need investigation?
* Which sleeves may deserve more or less attention?

## Operator Decisions Supported

Sleeve Analytics should support these operator decisions:

* Continue monitoring a sleeve
* Investigate a sleeve
* Validate a new sleeve
* Reduce confidence in a sleeve
* Retire a weak sleeve
* Compare sleeves
* Review allocation evidence

It must not automatically recommend trades or allocation changes.

## Core Product Questions

The UI must help answer:

1. Is this sleeve profitable?
2. Is this sleeve improving or degrading?
3. Does this sleeve have enough history to judge?
4. Are current results driven by open unrealized P&L or closed realized P&L?
5. Are results reliable or affected by missing data?
6. How does this sleeve compare to other sleeves?
7. Which candidates/trades contributed most to this sleeve's result?

## User Experience Principle

Sleeve Analytics must feel like a portfolio manager dashboard, not a raw attribution table.

Attribution answers:

```text
Where did P&L come from?
```

Analytics answers:

```text
Is this sleeve worth trusting?
```

## Page Structure

Create or evolve the Sleeve Analytics UI around these sections:

1. Executive Summary
2. Sleeve Scorecard
3. Sleeve Detail View
4. Trade Quality
5. Historical Trend
6. Diagnostics

## Executive Summary

The page should show:

* Total sleeves
* Active sleeves
* Sleeves with open positions
* Sleeves with closed positions
* Best sleeve
* Worst sleeve
* Total P&L
* Realized P&L
* Unrealized P&L
* Data quality status

The summary should immediately communicate whether sleeve analytics is meaningful yet or still early-stage.

## Sleeve Scorecard

The primary table should compare sleeves using:

* Sleeve name
* Sleeve status
* Open positions
* Closed positions
* Market value
* Realized P&L
* Unrealized P&L
* Total P&L
* Return %
* Win rate
* Profit factor
* Expectancy
* Mark coverage
* Attribution coverage
* Data quality

Rows should sort by total P&L by default until better analytics are available.

## Sleeve Detail View

Each sleeve should have an expandable detail view or drawer.

Required sections:

### Current Exposure

* Open positions
* Market value
* Largest position
* Concentration

### Performance

* Realized P&L
* Unrealized P&L
* Total P&L
* Return %

### Trade Quality

* Closed trades
* Win rate
* Average winner
* Average loser
* Profit factor
* Expectancy
* Average hold time

### Evidence

* Best contributing trades
* Worst contributing trades
* Recent candidates
* Recent exits

### Data Quality

* Missing marks
* Missing sleeve attribution
* Stale inputs
* Null metric reasons

## Early-Stage Behavior

If there are no closed trades, the UI must say:

```text
Closed-trade analytics unavailable until exits are recorded.
```

Do not spam `Unavailable`.

Do not imply that a sleeve is good or bad from open unrealized P&L alone.

## Phasing

### Phase 1: Attribution Foundation

Already implemented or near implemented:

* Canonical sleeve analytics artifact
* Executive summary
* Scorecard
* Mark coverage
* Attribution coverage
* Diagnostics

### Phase 2: Trade Quality

Add:

* Closed trade count
* Win rate
* Average winner
* Average loser
* Profit factor
* Expectancy
* Average hold time

### Phase 3: Historical Analytics

Add:

* Sleeve equity curve
* Daily cumulative P&L
* Drawdown
* Rolling performance
* Sleeve comparison over time

### Phase 4: Allocation Evidence

Add evidence only, not recommendations:

* Current exposure by sleeve
* Historical performance by sleeve
* Risk-adjusted comparison
* Operator-reviewed allocation notes

## Explicit Non-Goals

Sleeve Analytics must not:

* recommend trades
* tell the operator to buy or sell
* automatically allocate capital
* auto-retire sleeves
* auto-promote sleeves
* hide weak data quality
* present open P&L as proven edge

## Terminology

Use these terms carefully:

* "Attribution" = where P&L came from
* "Analytics" = whether the sleeve appears effective
* "Evidence" = facts supporting operator judgment
* "Recommendation" = forbidden unless explicitly enabled by future advisory policy

Avoid "capital allocation recommendation."

Use:

```text
Allocation Evidence
```

## Required UX Acceptance Criteria

The operator should be able to answer within 10 seconds:

* Which sleeve is best today?
* Which sleeve is worst today?
* How much P&L is realized versus unrealized?
* How many trades are closed?
* Is the sleeve history sufficient to evaluate?
* Is the data quality good enough to trust?

## Implementation Boundary

The UI must continue to read from:

```text
aegis_sleeve_analytics_v1
```

The UI must not compute sleeve analytics from raw source artifacts.

## Auditability

Every visible metric must be explainable through the canonical artifact.

The UI should provide access to:

* source artifact
* generated_at
* as_of
* data quality
* null reasons

## Governance Rule

No new Sleeve Analytics feature may be added unless it supports one of the operator decisions listed in this product requirements document.
