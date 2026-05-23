# Aegis Research Lab Foundation

Packet 1 establishes the storage boundary for Aegis research work.

## Ownership Boundary

Aegis does not store canonical historical prices, OHLCV bars, universe data, or large result artifacts in the app database.

Research Store owns:

- historical OHLCV snapshots
- universe snapshots
- data quality reports
- evidence packages
- large result artifacts
- derived/event tables

Aegis owns:

- hypotheses
- research plans
- dataset snapshot references
- evidence package references
- lifecycle state
- human decisions
- audit trail
- journal/performance links

The Aegis-side model stubs are references only. They are not market-data tables and they do not contain canonical OHLCV rows.

## Immutable Snapshots

Research Store snapshots live under `research_lab/research_store/`.

Dataset snapshots are immutable. A created snapshot directory is never overwritten. If the same provider, date range, universe, and bar policy are rebuilt with different content, a new content-hash-suffixed snapshot ID must be created.

Evidence packages are also immutable. Packet 1 defines the evidence manifest contract and registry layout; future packets can write sealed evidence packages after research runners exist.

Registry JSONL files are metadata truth for snapshot identity, quality status, storage URI, provider, universe, date range, bar policy, and content hash.

DuckDB, Parquet, or other query engines are data/query tools only. Registry manifests remain the source of truth for snapshot metadata.

## Commands

Create a universe snapshot:

```bash
python -m research_lab.cli create-universe-snapshot \
  --name liquid_etf_core \
  --version v1 \
  --input universes/liquid_etf_core_v1.yaml
```

List universe snapshots:

```bash
python -m research_lab.cli list-universe-snapshots
```

Validate a universe snapshot:

```bash
python -m research_lab.cli validate-universe-snapshot \
  --universe-snapshot-id us_liquid_etf_core_v1_HASH
```

Create a placeholder dataset snapshot envelope:

```bash
python -m research_lab.cli create-empty-dataset-snapshot \
  --dataset-type ohlcv \
  --provider placeholder \
  --interval 1d \
  --bar-policy bp_v1 \
  --universe-snapshot-id us_liquid_etf_core_v1_HASH \
  --start 2005-01-01 \
  --end 2026-05-18
```

List dataset snapshots:

```bash
python -m research_lab.cli list-dataset-snapshots
```

Validate a dataset snapshot:

```bash
python -m research_lab.cli validate-dataset-snapshot \
  --dataset-snapshot-id ds_ohlcv_1d_liquid_etf_core_20050101_20260518_HASH
```

Every create and validate command appends an audit event to:

```text
research_lab/research_store/audit_log/audit_events.jsonl
```

## Packet 1 Non-Goals

Packet 1 does not fetch historical prices, ingest provider data, build event studies, run backtests, create charts, implement paper-trial logic, or create broker/order code.

`create-empty-dataset-snapshot` creates only a metadata envelope with `quality_status=pending`, a placeholder quality report, and an empty `data/` directory. It does not store OHLCV rows.

## Daily OHLCV Dataset Snapshots

Packet 2 adds daily OHLCV ingestion plumbing:

- provider abstraction
- yfinance daily provider adapter
- canonical daily OHLCV bar builder
- explicit `bp_daily_ohlcv_v1` bar policy
- immutable dataset snapshot builder
- raw per-symbol files
- canonical `daily_ohlcv.parquet`
- quality reports
- manifest hashes
- registry and audit updates
- dataset query helpers

Build a real OHLCV snapshot:

```bash
python -m research_lab.cli build-ohlcv-dataset \
  --dataset-type ohlcv \
  --provider yfinance \
  --interval 1d \
  --bar-policy bp_daily_ohlcv_v1 \
  --universe-snapshot-id us_liquid_etf_core_v1_HASH \
  --start 2005-01-01 \
  --end 2026-05-18
```

Query a built dataset:

```bash
python -m research_lab.cli dataset-head \
  --dataset-snapshot-id ds_ohlcv_1d_liquid_etf_core_20050101_20260518_HASH \
  --limit 20

python -m research_lab.cli dataset-symbol-summary \
  --dataset-snapshot-id ds_ohlcv_1d_liquid_etf_core_20050101_20260518_HASH
```

The live yfinance build requires `yfinance`, `pandas`, and a Parquet engine such as `pyarrow` or `fastparquet`. If those dependencies are unavailable, the build command fails closed and appends failed build audit events. It does not create a successful dataset snapshot from fake or unavailable data.

## Research Runtime Dependencies

Packet 2B pins the optional live research runtime in:

```text
research_lab/requirements-research.txt
research_lab/requirements-research.lock
```

Install the research extras in an isolated local virtual environment:

```bash
python3 -m venv research_lab/.venv
research_lab/.venv/bin/python -m pip install -r research_lab/requirements-research.lock
```

Check dependency health:

```bash
research_lab/.venv/bin/python -m research_lab.cli dependency-health
```

Inspect provider diagnostics:

```bash
research_lab/.venv/bin/python -m research_lab.cli provider-diagnostics --provider yfinance
research_lab/.venv/bin/python -m research_lab.cli provider-diagnostics --provider stooq
research_lab/.venv/bin/python -m research_lab.cli provider-diagnostics --provider local_csv
```

## Provider Credential Setup

API-key providers use local environment variables or `research_lab/.env.local`. Do not commit API keys.

Create a local secret template:

```bash
research_lab/.venv/bin/python -m research_lab.cli create-provider-env-template \
  --output research_lab/.env.local
```

Add one or more provider keys to `research_lab/.env.local`, then check readiness:

```bash
research_lab/.venv/bin/python -m research_lab.cli provider-config-status \
  --env-file research_lab/.env.local
```

Provider status masks keys and reports only whether each provider is ready. OS environment variables override values in `.env.local`.

Check a specific API provider:

```bash
research_lab/.venv/bin/python -m research_lab.cli first-api-dataset-status \
  --provider tiingo \
  --universe local_etf_minimum_viable_v1 \
  --env-file research_lab/.env.local
```

Build the first API-backed dataset and evidence package:

```bash
research_lab/.venv/bin/python -m research_lab.cli build-first-api-evidence \
  --provider tiingo \
  --universe local_etf_minimum_viable_v1 \
  --start 2015-01-01 \
  --end 2025-12-31 \
  --threshold -0.02 \
  --forward-windows 1,2,5,10 \
  --env-file research_lab/.env.local
```

Run a live provider smoke test without creating a dataset snapshot:

```bash
research_lab/.venv/bin/python -m research_lab.cli live-ohlcv-smoke-test \
  --provider yfinance \
  --symbols SPY QQQ IWM \
  --start 2024-01-02 \
  --end 2024-02-01
```

Stooq can be used as a second daily provider. If Stooq returns an API-key prompt, set `STOOQ_API_KEY` in the environment and rerun:

```bash
export STOOQ_API_KEY=...

research_lab/.venv/bin/python -m research_lab.cli live-ohlcv-smoke-test \
  --provider stooq \
  --symbols SPY QQQ IWM \
  --start 2024-01-02 \
  --end 2024-02-01
```

Create the small smoke universe snapshot:

```bash
research_lab/.venv/bin/python -m research_lab.cli create-universe-snapshot \
  --name smoke_etf_core \
  --version v1 \
  --input universes/smoke_etf_core_v1.yaml
```

Build a real immutable OHLCV snapshot for that small universe:

```bash
research_lab/.venv/bin/python -m research_lab.cli build-ohlcv-dataset \
  --dataset-type ohlcv \
  --provider yfinance \
  --interval 1d \
  --bar-policy bp_daily_ohlcv_v1 \
  --universe-snapshot-id us_smoke_etf_core_v1_HASH \
  --start 2024-01-02 \
  --end 2024-02-01
```

For Stooq snapshots use the Stooq-specific bar policy, which explicitly records that `adj_close` is derived from `close` unless Stooq provides adjusted data:

```bash
research_lab/.venv/bin/python -m research_lab.cli build-ohlcv-dataset \
  --dataset-type ohlcv \
  --provider stooq \
  --interval 1d \
  --bar-policy bp_daily_ohlcv_stooq_v1 \
  --universe-snapshot-id us_smoke_etf_core_v1_HASH \
  --start 2024-01-02 \
  --end 2024-02-01
```

Local CSV snapshots use user-supplied files and do not depend on live provider access. By default `local_csv` reads from:

```text
research_lab/local_data/ohlcv/
```

Set `LOCAL_CSV_OHLCV_ROOT` to point at another directory:

```bash
LOCAL_CSV_OHLCV_ROOT=/path/to/ohlcv \
research_lab/.venv/bin/python -m research_lab.cli provider-diagnostics --provider local_csv
```

Build from local CSV:

```bash
LOCAL_CSV_OHLCV_ROOT=/path/to/ohlcv \
research_lab/.venv/bin/python -m research_lab.cli build-ohlcv-dataset \
  --dataset-type ohlcv \
  --provider local_csv \
  --interval 1d \
  --bar-policy bp_daily_ohlcv_local_csv_v1 \
  --universe-snapshot-id us_local_spy_sample_v1_HASH \
  --start 2017-01-03 \
  --end 2017-01-17
```

Do not move to event studies until at least one real immutable OHLCV dataset snapshot has been built and validated.
