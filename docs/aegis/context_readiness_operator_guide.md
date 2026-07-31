# Aegis Context Readiness Operator Guide

## Breadth drop location

Required path:

`/home/node/constellation_runtime_data/manual_drops/market_context/{DAY}/breadth.csv`

Template command:

`TARGET_DAY=YYYY-MM-DD npm run aegis:create-breadth-template`

This creates:

`/home/node/constellation_runtime_data/manual_drops/market_context/{DAY}/breadth.csv.template`

It does not create a certified `breadth.csv`.

Breadth is on an explicit manual operator path. Aegis does not infer or fabricate breadth.

## Required CSV format

Headers:

`day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc`

Example:

```csv
day_utc,advance_decline_delta,breadth_down_pct,source,timestamp_utc
2026-05-26,1234,42.7,MANUAL_OPERATOR_DROP,2026-05-26T20:00:00Z
```

Rules:
- `day_utc` must match `TARGET_DAY`
- `advance_decline_delta` must be numeric
- `breadth_down_pct` must be numeric
- `breadth_down_pct` must be between `0` and `100`
- `source` is required
- `timestamp_utc` is required

## Validation and ingest

Validate:

`TARGET_DAY=YYYY-MM-DD npm run aegis:validate-breadth-drop`

Ingest:

`TARGET_DAY=YYYY-MM-DD npm run aegis:ingest-breadth-drop`

Expected success shape:

```json
{
  "file_found": true,
  "schema_valid": true,
  "certification_status": "CERTIFIED"
}
```

Expected failure examples:

```json
{
  "file_found": false,
  "certification_status": "BLOCKED",
  "failure_reason": "BREADTH_DROP_MISSING: ..."
}
```

```json
{
  "file_found": true,
  "schema_valid": false,
  "certification_status": "BLOCKED",
  "failure_reason": "BREADTH_DROP_BREADTH_DOWN_PCT_RANGE_INVALID: ..."
}
```


## VIX drop location

Required manual fallback path:

`/home/node/constellation_runtime_data/manual_drops/market_context/{DAY}/vix.csv`

Template command:

`TARGET_DAY=YYYY-MM-DD npm run aegis:create-vix-template`

This creates:

`/home/node/constellation_runtime_data/manual_drops/market_context/{DAY}/vix.csv.template`

It does not create a certified `vix.csv`.

Required CSV format:

`day_utc,vix_level,source,timestamp_utc`

Example:

```csv
day_utc,vix_level,source,timestamp_utc
2026-05-26,18.25,MANUAL_OPERATOR_DROP,2026-05-26T20:00:00Z
```

Rules:
- `day_utc` must match `TARGET_DAY`
- `vix_level` must be numeric
- `vix_level` must be non-negative
- `source` is required
- `timestamp_utc` is required

Validate:

`TARGET_DAY=YYYY-MM-DD npm run aegis:validate-vix-drop`

Ingest:

`TARGET_DAY=YYYY-MM-DD npm run aegis:ingest-vix-drop`

## VIX verification

Run:

`TARGET_DAY=YYYY-MM-DD npm run aegis:verify-vix-source`

The report checks:
- `MANUAL_CSV_DROP`
- `LOCAL_CACHE`
- `CBOE`
- `STOOQ`
- `FINAL_EOD_ARTIFACT`

It reports:
- provider attempted
- provider status
- returned session date
- value
- freshness status
- certification status
- rejection reason
- next repair option

If all providers fail, the output remains blocked with:

`VIX_CURRENT_SOURCE_UNAVAILABLE`

Manual VIX is the governed operator fallback. Aegis does not synthesize VIX.

## Context repair

Run:

`TARGET_DAY=YYYY-MM-DD npm run aegis:repair-context-readiness`

This runs:
1. breadth validation
2. VIX verification
3. provider health
4. market context demand
5. event market snapshot
6. runtime truth kernel
7. verified runtime graph
8. hydrate
9. audit handoff

Expected closeout summary:
- `breadth_status`
- `vix_status`
- `event_market_snapshot_status`
- `runtime_truth_classification`
- `trade_advice_allowed`
- `remaining_blockers`
- `next_operator_action`

## Full readiness check

After context repair:

`npm run aegis:audit`

Then confirm policy:

`npm run aegis:query -- "claim: trade advice allowed"`

## What remains blocked if VIX is stale

If breadth is valid but VIX is stale or unavailable, Aegis remains blocked on:
- `vix_level`
- `vix_change_pct`
- `event_market_snapshot`
- runtime truth `PARTIAL_CONTEXT / BLOCKED`
- `TRADE_ADVICE_ALLOWED=false`

## What missing breadth or VIX does not block

Missing breadth or VIX does not block candidate visibility.

Current candidate diagnostics, raw signals, rejected rows, signal evidence graph rows, and valid candidate contracts remain visible when they are supported by certified candidate evidence.

Missing breadth or VIX does block:
- `event_market_snapshot`
- runtime truth readiness
- promotion/advice paths that require certified runtime context
- `TRADE_ADVICE_ALLOWED`

No broker execution, autonomous execution, manual capture, promotion bypass, or advice enablement occurs from this workflow.
