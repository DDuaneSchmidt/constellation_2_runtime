# AEGIS Missing Market Data Repair Requirements v1

## Purpose

Package T04 repairs the two AEGIS-owned missing-market-data blockers identified by T03 for the target day without changing strategy behavior.

## Target Sleeves

- `C2_DEFENSIVE_TAIL_V1`: repair missing runtime routing for existing fresh TLT data and required defensive-tail runtime compatibility inputs.
- `C2_EVENT_DISLOCATION_V1`: repair GLD historical day-bar completeness only when governed source evidence exists.

## Allowed Changes

- Market-data routing repair.
- Runtime path or manifest repair.
- Historical bar completeness repair from governed source evidence.
- Diagnostic artifact regeneration and post-repair proof.

## Prohibited Changes

- Strategy logic, trigger thresholds, candidate scoring, research quality, allocation, safety gates, live trading, broker execution, fabricated signals, fabricated candidates, or fabricated market data.

## Required Artifact

`truth/reports/aegis_missing_market_data_repair_v1/<TARGET_DAY>/missing_market_data_repair.v1.json`

The artifact must report original T03 status, attempted repair, source path, routed path, post-repair market-data status, post-repair T01/T02/T03 evidence, remaining blocker, owner, and David action requirement for each targeted sleeve.

