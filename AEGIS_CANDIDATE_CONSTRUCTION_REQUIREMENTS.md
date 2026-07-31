# Aegis Candidate Construction Requirements

## Core Principle

Candidate capture and candidate construction are separate stages.

A captured candidate is only a raw opportunity until Aegis successfully constructs:

- planned entry
- planned stop
- quantity
- risk
- construction status

Only constructed candidates may become `READY_FOR_OPERATOR_REVIEW`.

Candidate construction must be evidence-backed. Aegis must not infer stop, quantity, risk, or market-data readiness in the UI.

## Runtime Context

Current runtime showed:

- 25 candidates captured
- 2 ready for operator review
- 23 incomplete
- `paper_trade_construction_v1` constructed only `SPY` and `QQQ`
- `paper_review_queue_v1` carried the broader 25-candidate set
- 23 missing `planned_stop`
- 20 missing `quantity`
- 24 symbols blocked by `PROVIDER_NO_DATA` from `YAHOO_CHART`

This means candidate capture worked, but candidate construction did not complete for most rows.

## Candidate Pipeline Stages

### 1. CAPTURED_RAW

Signal/candidate exists.

Not yet constructed.

### 2. MARKET_DATA_BOUND

Required market data was found and attached.

### 3. ENTRY_CONSTRUCTED

Planned entry exists.

### 4. STOP_CONSTRUCTED

Planned stop exists.

### 5. QUANTITY_SIZED

Quantity exists.

### 6. RISK_VALIDATED

Risk amount/percent is valid.

### 7. READY_FOR_OPERATOR_REVIEW

Candidate has all required construction fields.

### 8. REJECTED_BY_CONSTRUCTION

Candidate failed construction rules.

### 9. CONSTRUCTION_BLOCKED

Candidate cannot be constructed because upstream data is missing.

## Required Construction Fields

A candidate is fully constructed only if it has valid:

- `candidate_id`
- `candidate_contract_id`
- `paper_session_id`
- `symbol`
- `direction`
- `planned_entry`
- `planned_stop`
- `quantity`
- `risk_amount`
- `construction_status`
- `market_data_timestamp`
- `construction_source_artifact`

Recommended fields:

- `target_price`
- `reward_risk_ratio`
- `sleeve`
- `strategy`
- `notional_value`
- `risk_percent`
- `market_data_provider`
- `market_data_error_code`

## Construction Readiness Rule

A candidate may not become `READY_FOR_OPERATOR_REVIEW` unless:

```text
planned_entry is valid
planned_stop is valid
quantity is valid
risk_amount is valid
market data is current
```

If any required field is missing, the candidate must be marked:

```text
CONSTRUCTION_BLOCKED
```

or:

```text
INCOMPLETE_CANDIDATE
```

with explicit missing fields.

## Required Diagnostics

For every non-constructed candidate, Aegis must record:

- symbol
- missing fields
- failed construction stage
- upstream artifact expected
- upstream artifact found/missing
- market data provider
- market data status
- market data error code
- repair recommendation

Example:

```json
{
  "symbol": "AMT",
  "construction_status": "CONSTRUCTION_BLOCKED",
  "failed_stage": "MARKET_DATA_BOUND",
  "missing_required_fields": ["planned_stop", "quantity"],
  "market_data_provider": "YAHOO_CHART",
  "market_data_error_code": "PROVIDER_NO_DATA",
  "repair_action": "Repair market data binding before stop/quantity construction."
}
```

## Required Construction Report

Aegis must create or document:

```text
truth/reports/aegis_candidate_construction_report_v1/<day>/candidate_construction_report.v1.json
```

It must include:

- captured count
- market-data-bound count
- entry-constructed count
- stop-constructed count
- quantity-sized count
- risk-validated count
- ready-for-review count
- blocked count
- rejected count
- missing fields summary
- provider error summary
- construction by sleeve/strategy
- construction by source artifact

## Required Command

Aegis must create or document:

```bash
npm run aegis:candidate-construction-status
```

It should print:

```text
Candidate construction for PAPER-YYYY-MM-DD-0950:
- captured: 25
- ready_for_operator_review: 2
- construction_blocked: 23
- missing planned_stop: 23
- missing quantity: 20
- provider errors:
  - YAHOO_CHART / PROVIDER_NO_DATA: 24
- top repair action: repair market-data binding
```

Until this command exists, construction status may be derived from `aegis_candidate_lifecycle_projection_v1`, `paper_trade_construction_v1`, `aegis_paper_review_queue_v1`, and `aegis_market_data_coverage_v1`.

## UI Relationship

Positions UI must consume candidate construction status but must not perform construction.

Positions UI may show:

```text
25 captured
2 ready for review
23 construction blocked
```

Incomplete rows must show Details only.

The main Positions page must not promote `CAPTURED_RAW`, `CONSTRUCTION_BLOCKED`, or `INCOMPLETE_CANDIDATE` rows into operator disposition actions.

## Acceptance Tests

Tests must prove:

- Raw captured candidate is not reviewable.
- Candidate missing market data is `CONSTRUCTION_BLOCKED`.
- Candidate missing stop is not reviewable.
- Candidate missing quantity is not reviewable.
- Candidate with entry/stop/quantity/risk is `READY_FOR_OPERATOR_REVIEW`.
- Construction report summarizes missing fields.
- Construction report summarizes provider errors.
- Positions page shows ready vs blocked counts correctly.
- Incomplete candidates cannot be confirmed as captured.

## Investigation Requirement

After creating this document, produce a repair plan for the current 23 incomplete candidates.

The repair plan must answer:

- Why did `YAHOO_CHART` return `PROVIDER_NO_DATA` for 24 symbols?
- Is the provider symbol format wrong?
- Is the provider request window wrong?
- Is market data stale or missing?
- Are candidates being generated before market data is available?
- Is stop construction dependent on unavailable intraday data?
- Is quantity sizing skipped because stop is missing?
- Should incomplete candidates remain visible or move to diagnostics?

## Governance Rule

No future Aegis pipeline may label a candidate as operator-reviewable unless it passes candidate construction readiness.

Candidate capture, candidate construction, and operator readiness must remain separate concepts.

If implementation behavior conflicts with this document, this document wins and the implementation must be corrected.
