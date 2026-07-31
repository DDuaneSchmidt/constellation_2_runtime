# Aegis Macro Calendar Data Readiness Requirements v1

## Scope

This package defines a research-only readiness path for generated hypotheses that require a governed macro event calendar. It does not change broker execution, live trading, trade advice, real-capital allocation, autonomous execution, order management, hypothesis scoring rules, or paper lifecycle behavior.

## Required Outcome

Aegis must produce `aegis_macro_calendar_data_readiness_v1` for each target day. The artifact is the authoritative source for whether Macro Calendar has a governed event-calendar dataset and whether that dataset satisfies the minimum contract for shadow validation eligibility.

## Data Contract

Each macro calendar event row must include:

- `event_name`
- `event_type`
- `release_datetime`
- `actual`
- `consensus`
- `prior`
- `importance`
- `affected_assets`
- `source`
- `source_timestamp`
- `timezone`
- `data_quality_status`

Supported `event_type` values are `CPI`, `FOMC`, `NFP`, `GDP`, `Retail Sales`, `ISM`, `PPI`, `Jobless Claims`, and `Other`.

## States

- `NEEDS_SOURCE`: no governed macro calendar source exists.
- `SOURCE_INCOMPLETE`: a governed source exists but required fields or values are missing or invalid.
- `READY`: a governed source exists and all required fields are valid.

## Required Behavior

- Missing source reports `david_action_required: true` with buttons `Connect Source`, `Upload Dataset`, `Mark Not Available`, and `Defer`.
- Incomplete source reports deterministic `missing_fields` and remains blocked from shadow validation.
- Ready source reports `macro_calendar_ready: true`.
- Ready source allows downstream read models to stop treating Macro Calendar as missing the macro event calendar.
- Readiness must not fabricate macro events or infer `actual`, `consensus`, or `prior`.
- Readiness must not force shadow validation to pass.

## Safety

All outputs are research-only. The readiness path must explicitly preserve no broker execution, no live trading, no trade advice, no real capital, no autonomous execution, no order management, and no safety gate changes.
