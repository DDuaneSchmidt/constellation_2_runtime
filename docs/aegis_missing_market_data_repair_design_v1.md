# AEGIS Missing Market Data Repair Design v1

## Flow

1. Read original T03 missing-market-data resolver evidence.
2. Repair `C2_DEFENSIVE_TAIL_V1` routing by materializing required runtime files from existing governed evidence.
3. Repair `C2_EVENT_DISLOCATION_V1` GLD completeness by appending the missing historical day bar only from governed STOOQ raw source evidence.
4. Rebuild in-memory T01, T02, and T03 diagnostics for post-repair proof.
5. Emit T04 before/after artifact.

## Defensive Tail Routing

The defensive tail producer requires:

- `truth_sleeves/PRIMARY/PAPER/market_data_snapshot_v1/snapshots/<DAY>/TLT.market_data_snapshot.v1.json`
- `truth_sleeves/PRIMARY/PAPER/accounting_v1/nav/<DAY>/nav_snapshot.v1.json`
- `truth_sleeves/PRIMARY/PAPER/positions_snapshot_v2/snapshots/<DAY>/positions_snapshot.v2.json`

T04 creates compatibility routing files only from existing canonical TLT JSONL, NAV v2 evidence, positions v5 evidence, or explicit bootstrap failure evidence.

## Event Dislocation GLD Completeness

The event dislocation producer validates `market_data_snapshot_v1/dataset_manifest.json` and requires `GLD/2026.jsonl` to contain the target-day bar. T04 repairs the JSONL and updates the manifest hash only when the governed STOOQ raw CSV contains the exact target day.

## Safety

T04 does not invoke broker/live trading, create signals, create candidates, change thresholds, mutate sleeve logic, or alter allocation. It only repairs data availability artifacts that T03 already attributed to `AEGIS_SYSTEM`.

