# AEGIS Missing Market Data Requirement Resolver Requirements V1

## Scope

`aegis_missing_market_data_requirement_resolver_v1` explains T02 dormant sleeves whose deterministic reason is `MISSING_MARKET_DATA`. It is diagnostics-only.

## Required Questions

For each affected sleeve, the resolver must identify the required market data items, required fields, expected source, source existence, ingestion status, freshness, sleeve routing, quality status, owner, and next deterministic action.

## Forbidden Actions

The resolver must not change sleeve strategy logic, thresholds, candidate generation, research quality, allocation logic, market data, generated signals, or candidate outputs. Declarative evidence links are allowed.

## Resolution Types

The resolver uses only deterministic resolution types: `SOURCE_AVAILABLE_ROUTING_MISSING`, `SOURCE_AVAILABLE_INGESTION_MISSING`, `SOURCE_AVAILABLE_BUT_STALE`, `SOURCE_AVAILABLE_BUT_INCOMPLETE`, `SOURCE_NOT_CONFIGURED`, `SOURCE_NOT_AVAILABLE`, `DATA_REQUIREMENT_UNDECLARED`, `UNSUPPORTED_DATA_DEPENDENCY`, `NO_ACTION_REQUIRED`, and `UNKNOWN_DETERMINISTIC_BLOCKER`.
