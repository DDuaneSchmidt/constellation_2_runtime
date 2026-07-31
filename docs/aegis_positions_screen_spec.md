# Aegis Positions Screen Product Specification

## Purpose

The Positions screen answers one question:

```text
What do we currently own?
```

It is the operator surface for current paper holdings, exposure, mark quality, and position-level monitoring. It is not a candidate workflow surface.

The Today / Command Center screen may summarize open positions and link here, but detailed position ownership belongs here.

## Product Boundary

Positions must include:

* open positions
* holdings
* exposure
* entry dates
* current value
* P&L if available
* risk and concentration
* stale or incomplete price warnings

Positions must not include:

* candidate workflow
* candidate qualification
* candidate capture
* research readiness
* manual IB capture workflow
* candidate blockers

## Operator Questions

### What positions are open?

Required data:

* open position count
* open position rows
* requested day
* source day

Source system:

* `aegis_paper_position_ledger_v1`
* prebuilt positions read model
* operator surface readiness / operator state as gating evidence

Update frequency:

* after position ledger generation or command processing
* after current marks refresh

Empty state:

```text
No open paper positions are recorded for this day.
```

Blocked state:

```text
Open position state is unavailable because the position ledger is missing, stale, or for a different day.
```

Degraded state:

```text
Open positions are visible, but some marks, P&L, or sleeve attribution are incomplete.
```

User action state:

* View position details.
* View position review if a review exists.
* Open evidence.
* No capture, submit, buy, sell, execute, or broker action.

### What do we own and where is exposure concentrated?

Required data:

* symbol
* sleeve
* quantity
* entry price
* entry timestamp
* current mark
* current value
* market value by sleeve
* concentration by symbol and sleeve

Source system:

* paper position ledger
* paper entry receipts
* canonical market marks
* sleeve attribution / sleeve analytics

Update frequency:

* mark refresh
* ledger update
* sleeve attribution rebuild

Empty state:

* show no exposure recorded.

Blocked state:

* show unavailable if ledger cannot be read.

Degraded state:

* show exposure counts but clearly mark missing current value if marks are incomplete.

User action state:

* none beyond review/detail navigation.

### Are current prices and P&L trustworthy?

Required data:

* mark coverage
* stale mark count
* missing mark count
* current value
* unrealized P&L if available
* realized P&L summary if available

Source system:

* canonical market data marks
* paper P&L report
* performance mark coverage diagnostics

Update frequency:

* after mark generation
* after P&L report generation

Empty state:

* if no open positions exist, no mark coverage warning is needed.

Blocked state:

```text
Position values are unavailable because current marks are missing.
```

Degraded state:

```text
Some positions are visible without certified current marks.
```

User action state:

* open evidence or System Health; do not ask for candidate action.

### What should be monitored?

Required data:

* concentration warnings
* mark quality warnings
* stale price warnings
* position review availability
* sleeve attribution quality

Source system:

* positions read model
* sleeve analytics
* position review summary
* surface readiness / evidence status

Update frequency:

* after positions, marks, sleeve analytics, or review rebuild

Empty state:

* no monitoring items if no positions exist.

Blocked state:

* show exact unavailable input.

Degraded state:

* show monitoring items that are trustworthy; show limitations separately.

User action state:

* review/details/evidence only.

## Proposed Components

### Component: Positions Status Summary

Purpose: Give the first visible answer to whether Aegis has open positions and whether position data is trustworthy.

Operator question answered: What do we currently own, and can I trust the position view?

Data source: positions read model, position ledger, mark coverage, sleeve attribution, surface readiness.

Empty state: No open paper positions are recorded for this day.

Blocked state: Position state is unavailable with exact missing or wrong-day input.

Degraded state: Positions are visible, but marks, attribution, or P&L are incomplete.

User action state: Link to evidence or position review only.

Classification: NEW

Value score: 10

Justification: The current Positions page buries the ownership answer under mixed workflow content.

### Component: Open Positions Table

Purpose: Show each open holding in a compact operator-readable table.

Operator question answered: What positions are active today?

Data source: `aegis_paper_position_ledger_v1`, paper entry receipts, current marks, P&L report, sleeve attribution.

Empty state: Table is replaced by the no-open-positions state.

Blocked state: Table does not render if position ledger truth is unavailable or wrong-day.

Degraded state: Rows render only trustworthy fields; missing marks/P&L show row-level warnings.

User action state: View Details, View Review, Open Evidence.

Classification: KEEP / IMPROVE

Value score: 10

Justification: The open positions table is the highest-value current Positions component, but it must be stripped of candidate workflow and raw state clutter.

### Component: Holdings and Exposure Summary

Purpose: Summarize market value, sleeve exposure, and concentration without becoming a performance dashboard.

Operator question answered: Where is current exposure concentrated?

Data source: position ledger, current marks, sleeve attribution, sleeve analytics.

Empty state: No exposure recorded.

Blocked state: Hidden when open position state is unavailable.

Degraded state: Show counts and exposure only for marked positions, with coverage warning.

User action state: View full table or evidence.

Classification: NEW

Value score: 9

Justification: Exposure is core ownership information and should not require opening Performance.

### Component: Mark and P&L Quality Banner

Purpose: Explain whether current values and P&L are complete, partial, stale, or unavailable.

Operator question answered: Can I trust current value and P&L?

Data source: market marks, paper P&L report, performance mark coverage diagnostics.

Empty state: Hidden when no open positions exist.

Blocked state: Show exact missing mark or P&L source.

Degraded state: Show coverage and affected count.

User action state: Open evidence/System Health.

Classification: NEW

Value score: 9

Justification: Previous UI showed unavailable fields without a clear trust explanation.

### Component: Risk and Concentration Summary

Purpose: Surface large single-symbol, sleeve, or stale-mark concentration risks in operator language.

Operator question answered: What current ownership risk should I notice?

Data source: positions read model, current marks, sleeve attribution.

Empty state: No concentration summary when no positions exist.

Blocked state: Hidden if values cannot be calculated.

Degraded state: Show concentration only across covered positions.

User action state: View positions/evidence.

Classification: NEW

Value score: 8

Justification: Risk/concentration belongs to current ownership and helps the operator monitor paper mode without advice.

### Component: Position Detail Drawer

Purpose: Show row-level evidence, receipt, entry, mark, and review links after the operator asks for detail.

Operator question answered: Why does this position row say that?

Data source: row-local ledger entry, entry receipt, market mark, position review availability, evidence references.

Empty state: No drawer without a selected row.

Blocked state: Drawer may show row evidence unavailable.

Degraded state: Drawer shows missing fields plainly.

User action state: View Review, Open Evidence.

Classification: IMPROVE

Value score: 8

Justification: Details are useful only after the ownership table has answered the main question.

### Component: Position Review Link

Purpose: Let the operator open the separate Position Review surface when available.

Operator question answered: What matters most for this position?

Data source: position review artifact availability and matching position id.

Empty state: No review available yet.

Blocked state: Position Review unavailable with reason.

Degraded state: Review exists but data quality is limited.

User action state: View Position Review.

Classification: NEW / IMPROVE

Value score: 7

Justification: Position Review is valuable, but its thesis content must not become the Positions primary view.

### Component: Positions Evidence Drawer

Purpose: Keep audit evidence available without making raw artifacts the primary UI.

Operator question answered: Why should I trust this position state?

Data source: source artifact refs and hashes from the positions read model and gating evidence.

Empty state: Hidden if no evidence exists.

Blocked state: Shows missing evidence reason.

Degraded state: Shows partial evidence limitations.

User action state: Expand evidence.

Classification: KEEP

Value score: 6

Justification: Evidence improves trust when collapsed by default.

### Component: Candidate Capture Panel on Positions

Purpose: Candidate capture and review workflow.

Operator question answered: None for Positions.

Data source: candidate state, paper review queue, candidate contracts, construction readiness.

Empty state: Not applicable.

Blocked state: Not applicable.

Degraded state: Not applicable.

User action state: Not allowed on Positions.

Classification: REMOVE

Value score: 1

Justification: Candidate capture belongs only on Candidates. Its presence on Positions makes the ownership screen answer the wrong question.

### Component: Today's Candidates Table on Positions

Purpose: Candidate workflow display.

Operator question answered: None for Positions.

Data source: candidate pipeline/readiness artifacts.

Empty state: Not applicable.

Blocked state: Not applicable.

Degraded state: Not applicable.

User action state: Not allowed on Positions.

Classification: REMOVE

Value score: 1

Justification: It duplicates Candidate Pipeline and violates the product boundary.

## Screenshot Acceptance

A Positions screenshot passes only if visible browser output proves:

1. The first meaningful content answers what is currently owned.
2. Open positions, holdings, exposure, entry date, current value, and P&L if available are visible or explicitly unavailable.
3. Stale or incomplete price warnings are visible when relevant.
4. Candidate workflow, candidate capture, candidate qualification, manual IB capture, research readiness, and candidate blockers do not appear as primary content.
5. No trade advice, broker submit, live trading, autonomous trading, buy, sell, or execute action appears.
6. Empty, blocked, and degraded states use plain ownership language.
7. Evidence is available only behind a secondary/collapsed detail affordance.
