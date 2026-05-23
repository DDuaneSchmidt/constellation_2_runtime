# Local OHLCV Input Directory

This folder is for operator-supplied historical daily OHLCV CSV files.
Research Lab uses these files to build immutable DatasetSnapshots in the
Research Store. Aegis does not store canonical historical prices in the app
database.

## Expected Naming

Use one CSV per symbol:

```text
research_lab/local_data/ohlcv/SPY.csv
research_lab/local_data/ohlcv/QQQ.csv
research_lab/local_data/ohlcv/IWM.csv
```

Template files must use:

```text
*_template.csv
```

Template files are ignored by readiness checks and dataset builds.

## Accepted Columns

Preferred:

```text
date,open,high,low,close,adj_close,volume
```

Yahoo-style headers are accepted:

```text
Date,Open,High,Low,Close,Adj Close,Volume
```

Timestamp-style files are accepted if they contain:

```text
timestamp,open,high,low,close,volume
```

If `adj_close` / `Adj Close` is missing, Research Lab sets
`adj_close = close` and records `unadjusted_close_as_adj_close` as a warning
in readiness, manifest, dataset snapshot, and quality metadata.

## Manual Acquisition Notes

Yahoo Finance supports manual historical CSV download from a quote page's
Historical Data view.

Stooq historical data pages and Stooq bulk historical downloads can provide
CSV-style files, subject to their site access requirements.

The operator is responsible for data-source licensing, usage rights, and
retaining provenance notes outside the CSV when needed.

## Safety Rules

- Do not edit old CSV files after they have been used in a DatasetSnapshot.
- Corrections should create new CSV files and new DatasetSnapshots.
- Do not place template files as real data.
- Do not fabricate missing rows.
- Run `validate-local-csvs` before building an immutable dataset snapshot.

## Typical Workflow

```bash
research_lab/.venv/bin/python -m research_lab.cli expected-csv-files \
  --universe-snapshot-id us_local_etf_research_core_v1_1f7b96 \
  --csv-root research_lab/local_data/ohlcv

research_lab/.venv/bin/python -m research_lab.cli validate-local-csvs \
  --universe-snapshot-id us_local_etf_research_core_v1_1f7b96 \
  --csv-root research_lab/local_data/ohlcv \
  --start 2015-01-01 \
  --end 2025-12-31 \
  --allow-missing-symbols true

research_lab/.venv/bin/python -m research_lab.cli build-ohlcv-dataset \
  --provider local_csv \
  --interval 1d \
  --bar-policy bp_daily_ohlcv_local_csv_v1 \
  --universe-snapshot-id us_local_etf_research_core_v1_1f7b96 \
  --start 2015-01-01 \
  --end 2025-12-31 \
  --allow-missing-symbols true \
  --require-readiness-report true \
  --readiness-report-path research_lab/local_data/ohlcv/csv_readiness_report.json
```
