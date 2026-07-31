# Aegis Data Action Routing Requirements v1

## Intent

Clarify why some `MISSING_DATA` blockers require David action while others do not.

## Scope

This package is read-only classification, documentation, read-model, and UI clarity. It must not change candidate generation, producer logic, paper lifecycle, trading behavior, broker behavior, or safety gates.

## Required Classifications

Every `MISSING_DATA` blocker must classify to exactly one of:

- `OPERATOR_PROVIDED_DATA_REQUIRED`
- `SYSTEM_DATA_PIPELINE_REQUIRED`
- `WAITING_FOR_MARKET_DATA`
- `DATA_NOT_AVAILABLE`

## Required Rows

Macro Calendar:

- `blocker_code`: `MISSING_DATA`
- `data_action_classification`: `OPERATOR_PROVIDED_DATA_REQUIRED`
- `david_action_required`: `true`
- `owner`: `DAVID`
- message: `Macro Calendar needs a macro event calendar source.`
- buttons: `Connect Source`, `Upload Dataset`, `Mark Not Available`, `Defer`

Oil Shock:

- `blocker_code`: `MISSING_DATA`
- explicit classification required
- `david_action_required`: `false` unless Aegis creates an explicit data-source action
- owner should be `AEGIS_SYSTEM` or `MARKET_CONDITIONS` when no David action exists
- message must explain that no David action is required

## Safety

The artifact and UI are read-only. Safety gates, trading permissions, broker execution, candidate generation, producer logic, and paper lifecycle behavior must remain unchanged.
