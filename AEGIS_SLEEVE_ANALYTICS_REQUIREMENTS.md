# Aegis Sleeve Analytics Requirements

## Purpose

Define the long-term design for Sleeve Analytics as a canonical, deterministic, read-only analytics report.

Sleeve Analytics must not be implemented as UI-side calculations over multiple raw sources.

## Core Principle

Sleeve Analytics is a canonical read model, not a UI feature.

The UI must read one artifact:

```text
aegis_sleeve_analytics_v1
```

The UI must not calculate sleeve analytics directly from multiple sources.

## Goal

Sleeve Analytics should answer:

* Which sleeves are working?
* Which sleeves are losing money?
* Which sleeves have enough trade history to evaluate?
* Which sleeves have open exposure?
* Which sleeves have data-quality problems?
* Which sleeves deserve further review?

It must not provide automated trade advice or capital allocation instructions.

## Non-Goals

Sleeve Analytics must not:

* recommend trades
* submit orders
* transmit broker instructions
* automatically change allocations
* produce advisory language
* hide incomplete data quality
* calculate metrics in the browser

## Canonical Artifact

Create/document:

```text
truth/reports/aegis_sleeve_analytics_v1/<day>/sleeve_analytics.v1.json
```

Required top-level fields:

```json
{
  "day_utc": "...",
  "as_of": "...",
  "generated_at": "...",
  "status": "CANONICAL|PARTIAL|NOT_CANONICAL",
  "data_quality": {},
  "summary": {},
  "sleeves": [],
  "diagnostics": []
}
```

## Required Inputs

The report may consume:

* `aegis_paper_position_ledger_v1`
* `aegis_paper_entry_receipts_v1`
* `aegis_paper_exit_receipts_v1`
* `aegis_paper_pnl_report_v1`
* `aegis_daily_paper_performance_v1`
* `aegis_sleeve_performance_truth_v1`
* canonical market marks
* candidate/contract/signal lineage for sleeve attribution

All consumed sources must be listed in the report with:

* path
* generated_at
* freshness status
* input row count

## Required Summary Metrics

Top-level summary must include:

* total_sleeves
* active_sleeves
* sleeves_with_open_positions
* sleeves_with_closed_positions
* total_open_positions
* total_closed_positions
* total_realized_pnl
* total_unrealized_pnl
* total_pnl
* mark_coverage_pct
* sleeve_attribution_coverage_pct
* data_quality_status

## Sleeve Scorecard

Each sleeve row must include:

* sleeve_id
* sleeve_name
* status
* open_positions
* closed_positions
* market_value
* realized_pnl
* unrealized_pnl
* total_pnl
* return_pct
* win_rate
* average_winner
* average_loser
* profit_factor
* expectancy
* average_hold_days
* mark_coverage_pct
* attribution_coverage_pct
* data_quality_status
* diagnostics_count

## Required Formulas

Define formulas explicitly:

```text
total_pnl = realized_pnl + unrealized_pnl

win_rate = winning_closed_trades / closed_trades

average_winner = gross_profit / winning_closed_trades

average_loser = gross_loss / losing_closed_trades

profit_factor = gross_profit / abs(gross_loss)

expectancy = (win_rate * average_winner) - ((1 - win_rate) * abs(average_loser))

return_pct = total_pnl / capital_basis
```

If denominator is zero or unavailable, field must be null with a reason.

## Data Quality Rules

The report must expose:

* missing_mark_count
* missing_sleeve_assignment_count
* missing_entry_receipt_count
* missing_exit_receipt_count
* missing_realized_pnl_count
* stale_mark_count
* partial_history_count

If any required source is missing or stale, status must be:

```text
PARTIAL
```

or:

```text
NOT_CANONICAL
```

with a plain-English reason.

## Unknown Sleeve Handling

UNKNOWN sleeve bucket is allowed only when sleeve attribution cannot be recovered from:

* position ledger
* entry receipt
* candidate lifecycle projection
* candidate contract
* signal evidence
* output intent lineage

If UNKNOWN exists, report must include:

* affected symbols
* affected position IDs
* attempted recovery sources
* reason attribution failed

## UI Requirements

Create a Sleeve Analytics page or section with:

1. Executive Summary
2. Sleeve Scorecard
3. Sleeve Detail Drawer
4. Sleeve Diagnostics

Do not mix raw diagnostics into the main scorecard.

Diagnostics must be collapsed by default.

## Executive Summary

Show:

* Total Sleeves
* Active Sleeves
* Total P&L
* Realized P&L
* Unrealized P&L
* Best Sleeve by Total P&L
* Worst Sleeve by Total P&L
* Mark Coverage
* Attribution Coverage
* Data Quality

## Sleeve Scorecard

Default columns:

* Sleeve
* Status
* Open Positions
* Closed Positions
* Market Value
* Realized P&L
* Unrealized P&L
* Total P&L
* Return %
* Win Rate
* Profit Factor
* Data Quality

Do not add a composite 0-100 sleeve score in Phase 1.

## Sleeve Detail Drawer

For each sleeve, show:

### Current Exposure

* open positions
* market value
* largest position
* concentration

### Performance

* realized P&L
* unrealized P&L
* total P&L
* return %

### Trade Quality

* closed trades
* win rate
* average winner
* average loser
* profit factor
* expectancy
* average hold days

### Data Quality

* mark coverage
* attribution coverage
* missing fields
* stale inputs

## Phasing

### Phase 1

Implement:

* canonical `aegis_sleeve_analytics_v1` report
* executive summary
* sleeve scorecard
* data quality
* no composite score

### Phase 2

Implement:

* closed-trade analytics
* win rate
* average winner/loss
* profit factor
* expectancy

### Phase 3

Implement:

* equity curves
* drawdown
* sleeve historical comparison

### Phase 4

Implement:

* allocation evidence
* not recommendations

## Auditability Requirements

Every metric must include:

* source artifact
* source row count
* formula
* excluded row count
* null reason if unavailable
* generated_at
* as_of

## Acceptance Tests

Add tests proving:

* UI reads `aegis_sleeve_analytics_v1`, not raw source artifacts.
* Sleeve analytics artifact includes all active sleeves.
* total_pnl equals realized_pnl + unrealized_pnl.
* win_rate formula is deterministic.
* profit_factor formula is deterministic.
* UNKNOWN sleeve bucket appears only when attribution recovery fails.
* diagnostics are collapsed by default.
* no composite sleeve score appears in Phase 1.
* missing data produces PARTIAL or NOT_CANONICAL with reason.
* browser does not compute sleeve analytics.

## Required Commands

Add command:

```bash
npm run aegis:sleeve-analytics
```

Add self-check command:

```bash
npm run aegis:sleeve-analytics-self-check
```

The self-check must fail if:

* UI/API fields disagree with the canonical artifact
* total_pnl formula is inconsistent
* missing attribution is hidden
* browser-side analytics calculations are detected
* diagnostics appear uncollapsed on the main scorecard

## Governance Rule

No Sleeve Analytics UI may be implemented until `aegis_sleeve_analytics_v1` exists and passes self-checks.

This document is the authority for Sleeve Analytics product behavior. If implementation conflicts with this document, the document wins unless a higher-level Aegis workflow or safety requirement is changed first.
