# Aegis Market Context Layer v1

## Purpose

The Market Context Layer provides deterministic market-state evidence for Aegis Lite event monitoring, regime attribution, AI feedback, sleeve review, and risk/sizing review. It is an evidence artifact, not a trading authorization layer.

Data flow:

`Market Data Sources -> event_market_snapshot_v1 -> Regime Classifier -> Event Monitor -> AI Feedback / Sleeve Review`

The canonical artifact is:

`reports/event_market_snapshot_v1/<date>/event_market_snapshot.v1.json`

## Snapshot Model

`event_market_snapshot_v1` contains:

- SPY, QQQ, and VIX price/return context
- breadth metrics
- volatility metrics
- trend metrics
- scheduled macro-event context
- deterministic regime label
- event-monitor input fields
- stale/missing data reasons
- source lineage

If required inputs are absent, the snapshot is still writable but fails closed with `stale_data_status=MISSING_INPUT` and explicit `MISSING_INPUT:<field>` reasons.

## Canonical VIX Symbol Resolution

Aegis treats volatility input as canonical symbol `VIX`. Market data paths must normalize these accepted aliases back to canonical `VIX`:

- `VIX`
- `^VIX`
- `$VIX`
- `vix`
- `VIXCLS`

Provider-specific mappings are centralized in `ops/aegis/market_data/symbol_alias_registry_v1.py` and surfaced through `aegis_symbol_map_v1`:

- `LOCAL_CACHE`: checks canonical `VIX` first, then `^VIX`, `vix`, `VIXCLS`, and `$VIX` if present under `market_data_snapshot_v1/<alias>/<year>.jsonl`.
- `STOOQ`: uses `^VIX` first, with `vix` as the deterministic secondary key. If Stooq requires an API key or returns no real quote, Aegis fails closed.
- `FRED`: reserved mapping is `VIXCLS` when a FRED provider exists.
- `CBOE`: reserved mapping is provider-native `VIX` when a CBOE provider exists.

All downstream artifacts use canonical `VIX` in `requested_symbols`, `missing_symbols`, diagnostics, sleeve readiness, and blocked-sleeve explanations. Provider aliases may appear only as `provider_symbol` or provider attempt metadata.

Resolution order is deterministic:

1. Local cache canonical `VIX`.
2. Local cache aliases.
3. Configured primary provider.
4. Configured fallback provider aliases.
5. Fail closed with a missing `VIX` explanation.

Aegis never fabricates, synthesizes, or substitutes VIX. If VIX is unavailable, `C2_VOL_INCOME_DEFINED_RISK_V1` remains blocked, readiness remains `READY_PARTIAL` when other sleeves can run, and operator review remains advisory-only.

To add real local-cache VIX data, write a real source row to one of the accepted local-cache alias paths, for example:

`/home/node/constellation_runtime_data/truth/market_data_snapshot_v1/VIX/2026.jsonl`

or:

`/home/node/constellation_runtime_data/truth/market_data_snapshot_v1/^VIX/2026.jsonl`

Rows must contain a real source symbol and timestamp, such as `symbol`, `timestamp_utc`, `open`, `high`, `low`, `close`, and optional `volume`. Do not manually invent a value.

Verify resolution with:

- `npm run aegis:refresh-market-data`
- `npm run aegis:data-registry`
- `npm run aegis:sleeve-readiness`
- `npm run aegis:candidate-diagnostics`
- `npm run aegis:noon-preflight`

## Volatility Model

Ruleset: `aegis_market_context_rules.v1`

- `PANIC_VOL`: VIX >= 35, VIX change >= 25%, realized volatility >= 35, or volatility expansion ratio >= 2.0
- `HIGH_VOL`: VIX >= 24, VIX change >= 10%, realized volatility >= 24, or expansion ratio >= 1.35
- `LOW_VOL`: VIX < 14, realized volatility <= 12, and expansion ratio < 0.9
- `NORMAL_VOL`: volatility inputs exist and no higher/lower threshold applies
- `UNKNOWN`: all volatility inputs are missing

## Breadth Model

- `BREADTH_COLLAPSE`: breadth down percentage >= 75 or advance-decline delta <= -1000
- `WEAK_BREADTH`: breadth down percentage >= 60 or participation percentage < 45
- `STRONG_BREADTH`: breadth down percentage <= 42 and advance-decline delta >= 500
- `MIXED_BREADTH`: breadth inputs exist and no other threshold applies
- `UNKNOWN`: breadth inputs are missing

## Macro Event Model

Supported scheduled macro events:

- `FOMC`
- `CPI`
- `JOBS_REPORT`
- `MAJOR_MACRO`

The snapshot records whether a macro event is scheduled for `day_utc`, the event type, and the highest declared risk level across that day’s events. No scraping is performed. Operators provide a simple JSON calendar or market-data payload.

## Regime Model

Initial deterministic regimes:

- `PANIC`
- `DEFENSIVE_ROTATION`
- `HIGH_VOLATILITY`
- `RECOVERY`
- `TRENDING_UP`
- `TRENDING_DOWN`
- `RANGE_BOUND`
- `UNKNOWN`

Rules are precedence ordered. Panic and defensive rotation are evaluated before broad trend labels. Missing or insufficient inputs produce `UNKNOWN`.

## Event Integration

The event monitor consumes `event_market_snapshot_v1` through its existing snapshot input path:

`reports/event_market_snapshot_v1/<date>/event_market_snapshot.v1.json`

The monitor uses the snapshot `inputs` map for event rules and includes `market_context` plus `market_snapshot_freshness_status` in `event_monitoring_status.v1.json`.

If the snapshot is missing, stale, or has missing inputs, event evaluation fails closed and records explicit reason codes. It does not mutate EOD state and does not submit broker orders.

## AI Feedback Integration

AI feedback reviews accept market context snapshots as deterministic attribution evidence. Reviews can state that results happened during regimes such as `HIGH_VOLATILITY`, `TRENDING_UP`, or macro-event days such as `CPI`/`FOMC`.

This context does not permit automatic research promotion, production mutation, or trade creation.

## Sleeve Review Integration

EOD/EOW sleeve reviews include `market_context_summary` so sleeve behavior can be compared against deterministic volatility, breadth, macro-event, and regime conditions.

## Safety Boundaries

The Market Context Layer:

- does not activate production
- does not touch broker/IB automation
- does not authorize autonomous trading
- does not create fake trades, fills, or outcomes
- is deterministic and replayable from explicit inputs
- fails closed when inputs are missing or stale
