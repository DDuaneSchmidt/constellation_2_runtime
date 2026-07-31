# AEGIS Missing Market Data Repair Spec v1

## Artifact

Family: `aegis_missing_market_data_repair_v1`

Filename: `missing_market_data_repair.v1.json`

## Repair Status Values

- `REPAIRED`
- `NOT_REPAIRED_SOURCE_TRULY_MISSING`
- `NOT_REPAIRED_UNSUPPORTED_DEPENDENCY`
- `NOT_REPAIRED_AMBIGUOUS_SOURCE`
- `NOT_REPAIRED_RUNTIME_ERROR`
- `NO_REPAIR_REQUIRED`
- `UNKNOWN_DETERMINISTIC_BLOCKER`

## Repair Type Values

- `ROUTING_REPAIR`
- `INGESTION_COMPLETENESS_REPAIR`
- `PATH_ALIAS_REPAIR`
- `DATE_KEY_REPAIR`
- `MANIFEST_REPAIR`
- `NO_REPAIR`
- `UNKNOWN`

## Deterministic Rules

`C2_DEFENSIVE_TAIL_V1` can be repaired only when canonical TLT historical JSONL for the target day exists and runtime NAV/positions evidence exists or bootstrap evidence explicitly permits empty positions.

`C2_EVENT_DISLOCATION_V1` can be repaired only when `historical_bar.GLD.<TARGET_DAY>` exists in canonical historical JSONL or a governed raw source row exists under `aegis_market_data_v1/<TARGET_DAY>/raw/STOOQ/GLD.quote.csv`.

If GLD source evidence does not exist, T04 must not create the historical bar and must emit `NOT_REPAIRED_SOURCE_TRULY_MISSING`.

## Ownership

Owner is `AEGIS_SYSTEM` for remaining code/routing/manifest/completeness work. Owner is `DAVID` only when David must provide an external source, vendor, file, calendar, API credential, or subscription. Owner is `NONE` when no repair action remains.

