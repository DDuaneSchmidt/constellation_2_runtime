# Aegis Performance Screen Specification

## Purpose

Performance answers:

```text
How are we doing, what is working, what is not working, and what trends matter?
```

Performance is the operator surface for returns, P&L, attribution, benchmarks, strategy/sleeve performance, and performance trends.

Performance is not:

* candidate readiness
* candidate actionability
* candidate capture
* research progress
* research findings
* system readiness
* operational health
* repair workflow

## Primary Operator Questions

### How are we doing?

Required data:

* requested day
* source day
* total paper P&L when canonical or explicitly partial
* realized P&L
* unrealized P&L
* portfolio return if capital basis is known
* open and closed position counts as performance context only
* data completeness statement

Source system:

* paper P&L report
* daily paper performance report
* paper position ledger as performance input
* canonical market marks
* semantic invariant report
* operator surface contract or operator-ready performance envelope

Update frequency:

* after mark generation
* after P&L report generation
* after daily performance report generation
* after exits or position ledger updates

Empty state:

```text
No performance data is available for this day.
```

Blocked state:

```text
Performance cannot be shown because required marks, P&L, or attribution evidence is missing or inconsistent.
```

Degraded state:

```text
Performance is partially available, but some marks, attribution, benchmark, or history inputs are incomplete.
```

User action state:

* No trade, candidate, or repair action belongs in Performance.
* The operator may open evidence or System Health when performance is blocked.

### Is performance data complete?

Required data:

* mark coverage
* missing mark count
* attribution coverage
* missing sleeve attribution count
* benchmark availability
* realized/unrealized P&L availability
* semantic contradiction status

Source system:

* market data coverage
* paper P&L report
* sleeve analytics
* semantic invariants
* surface readiness / operator surface contract

Update frequency:

* after mark coverage rebuild
* after P&L rebuild
* after semantic invariant checks

Empty state:

```text
No completeness check is available for this day.
```

Blocked state:

```text
Performance completeness cannot be evaluated from current evidence.
```

Degraded state:

```text
Performance data is readable with limitations.
```

User action state:

* No primary action unless evidence details are opened.

### What is working?

Required data:

* positive contributors
* best sleeve/strategy by total P&L when trustworthy
* best position contributors
* realized vs unrealized contribution split
* benchmark-relative result if available

Source system:

* daily paper performance report
* position attribution
* sleeve analytics
* sleeve performance truth
* benchmark report if available

Update frequency:

* after daily performance generation
* after sleeve analytics generation

Empty state:

```text
No winners can be identified yet.
```

Blocked state:

```text
Working contributors cannot be ranked because performance inputs are unavailable.
```

Degraded state:

```text
Contributor ranking is partial because some marks or attribution are missing.
```

User action state:

* None. Performance is explanatory.

### What is not working?

Required data:

* negative contributors
* worst sleeve/strategy by total P&L when trustworthy
* positions with negative P&L
* benchmark-relative underperformance if available
* data-quality caveats for apparent losers

Source system:

* daily paper performance report
* position attribution
* sleeve analytics
* benchmark report if available

Update frequency:

* after daily performance generation

Empty state:

```text
No underperforming contributors can be identified yet.
```

Blocked state:

```text
Underperforming contributors cannot be ranked because performance inputs are unavailable.
```

Degraded state:

```text
Underperformance analysis is partial because some marks or attribution are missing.
```

User action state:

* None. Do not show buy/sell/exit/increase/reduce language.

### What trends matter?

Required data:

* daily P&L history if available
* sleeve trend history if available
* closed-trade history if available
* benchmark trend if available
* data history coverage

Source system:

* daily paper performance history
* sleeve analytics history
* paper trade outcome / exit receipts
* benchmark history

Update frequency:

* daily after close or after paper performance pipeline runs

Empty state:

```text
No performance trend history is available yet.
```

Blocked state:

```text
Performance trends cannot be shown because history inputs are missing or inconsistent.
```

Degraded state:

```text
Trend history is partial.
```

User action state:

* None.

## Required Screen Sections

### Section 1: Performance Summary

Purpose: give the first visible answer to how the portfolio performed.

Operator question answered: How are we doing?

Required data:

* total P&L
* realized P&L
* unrealized P&L
* data completeness state
* one-sentence trust statement

Value score: 10

Classification: NEW

Justification: current Performance has shown contradictory cards. The first section must synthesize performance and trust in one operator sentence.

### Section 2: Data Completeness

Purpose: explain whether visible performance can be trusted.

Operator question answered: Is performance data complete?

Required data:

* mark coverage
* missing marks
* attribution coverage
* benchmark availability
* semantic contradiction state

Value score: 10

Classification: IMPROVE

Justification: prior failures included `NOT_CANONICAL` mixed with complete-looking metrics. Completeness must be visible without raw invariant language.

### Section 3: Contributors

Purpose: show what drove the result.

Operator question answered: What is working and what is not working?

Required data:

* top positive contributors
* top negative contributors
* contribution amounts
* whether each contribution is realized/unrealized
* data-quality caveat if partial

Value score: 9

Classification: NEW

Justification: attribution is more useful than raw aggregate cards when evaluating daily operation.

### Section 4: Strategy / Sleeve Performance

Purpose: summarize strategy performance without replacing Sleeve Analytics.

Operator question answered: Which strategies appear to be working or lagging?

Required data:

* sleeve name
* total P&L
* realized/unrealized split
* open positions count
* mark/attribution coverage

Value score: 8

Classification: IMPROVE

Justification: useful for performance, but detailed sleeve analytics remain separate.

### Section 5: Trend Summary

Purpose: show whether performance direction matters beyond the day.

Operator question answered: What trends matter?

Required data:

* recent P&L trend if available
* benchmark-relative trend if available
* history coverage

Value score: 7

Classification: NEW

Justification: trend context prevents overreading a single day.

### Section 6: Collapsed Evidence

Purpose: provide verification without making raw artifacts primary.

Operator question answered: Why should I trust this?

Required data:

* source day
* generated timestamp
* source evidence references
* completeness inputs

Value score: 6

Classification: IMPROVE

Justification: evidence is necessary but belongs below the operator answer.

## State Model

### NORMAL

Visible message:

```text
Performance is available for this day.
```

Operator expectation: Review results and contributors. No operational action is required.

Components shown: Performance Summary, Data Completeness, Contributors, Strategy / Sleeve Performance, Trend Summary if available, Collapsed Evidence.

Components hidden: system repair workflow, candidate workflow, research detail, raw contract panels.

### NO_DATA

Visible message:

```text
No performance data is available for this day.
```

Operator expectation: Nothing can be evaluated yet. Check next scheduled performance run or System Health if this is unexpected.

Components shown: empty Performance Summary, reason, next expected update if known, Collapsed Evidence if available.

Components hidden: metric cards, contributors, trend charts, sleeve rankings.

### PARTIAL_DATA

Visible message:

```text
Performance is partially available.
```

Operator expectation: Use visible values only with the stated limitation.

Components shown: Performance Summary with partial label, Data Completeness, partial Contributors, Collapsed Evidence.

Components hidden: complete-looking rankings or canonical total returns when coverage is incomplete.

### DEGRADED

Visible message:

```text
Performance is degraded and should not be treated as complete.
```

Operator expectation: Read the reason and use System Health if a repair is needed.

Components shown: degraded explanation, impact, next step, limited metrics only when clearly labeled.

Components hidden: canonical-looking total P&L, benchmark rankings, complete analytics cards.

## Components That Must Never Appear On Performance

* candidate readiness or capture workflow
* open holdings table as ownership workflow
* research hypothesis cards
* system health repair queue as primary content
* raw surface contract JSON
* raw semantic invariant names as headline content
* broker/live/autonomous trading controls
* trade advice or execution recommendations

## Screenshot Acceptance

A Performance screenshot passes only if a non-engineer can answer:

* How are we doing?
* Is performance data complete?
* What is working?
* What is not working?

without reading raw diagnostics or artifact names.
