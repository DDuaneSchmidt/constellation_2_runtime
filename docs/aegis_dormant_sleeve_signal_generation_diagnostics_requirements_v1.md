# AEGIS Dormant Sleeve Signal Generation Diagnostics Requirements V1

## Purpose

Explain why each T01 `NO_SIGNALS_GENERATED` sleeve produced zero raw signals for a target day.

## Scope

This package is diagnostics-only and read-only. It must not repair producers, change sleeve logic, alter thresholds, mutate market data, fabricate signals, force candidates, change research quality, or change allocation logic.

## Required Artifact

`reports/aegis_dormant_sleeve_signal_generation_diagnostics_v1/{day}/dormant_sleeve_signal_generation_diagnostics.v1.json`

## Required Classifications

Each dormant sleeve is classified into exactly one deterministic reason code:

- `VALID_NO_SIGNAL_CONDITIONS`
- `PRODUCER_NOT_REGISTERED`
- `PRODUCER_NOT_INVOKED`
- `PRODUCER_DISABLED`
- `PRODUCER_RUNTIME_ERROR`
- `MISSING_MARKET_DATA`
- `MISSING_EVENT_DATA`
- `MISSING_TRIGGER_POLICY`
- `TRIGGER_THRESHOLDS_NOT_MET`
- `UNSUPPORTED_HYPOTHESIS_TYPE`
- `LINEAGE_MISMATCH`
- `UNKNOWN_DETERMINISTIC_BLOCKER`

## Required Answer

The artifact must distinguish healthy selectivity, data starvation, wiring failure, logic/configuration failure, runtime failure, and unsupported sleeve types. Zero signals must not be treated as automatically bad.
