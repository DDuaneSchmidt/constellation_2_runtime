# Manual CSV Acquisition

Research Lab uses operator-supplied CSV files for local historical daily OHLCV
ingestion. It does not scrape around CAPTCHA, API-key, rate-limit, or site
access restrictions.

## Minimum Viable ETF Set

Need at least 3 real CSV files from:

- SPY
- QQQ
- IWM
- TLT
- GLD

Preferred full set:

```text
SPY.csv
QQQ.csv
IWM.csv
TLT.csv
GLD.csv
```

Place files in:

```text
research_lab/local_data/ohlcv/
```

## Yahoo Finance

Yahoo Finance data should be downloaded manually from the symbol's Historical
Data page when available to the operator. Save the downloaded file using the
expected `SYMBOL.csv` filename. Research Lab does not implement automated Yahoo
scraping.

Symbol hints:

- SPY: Yahoo symbol `SPY`
- QQQ: Yahoo symbol `QQQ`
- IWM: Yahoo symbol `IWM`
- TLT: Yahoo symbol `TLT`
- GLD: Yahoo symbol `GLD`

## Stooq

Stooq provides historical data pages and CSV download links, but access can
depend on route/environment and may require CAPTCHA/API-key handling. Stooq also
has historical data download and bulk historical market data pages.

Research Lab does not bypass CAPTCHA, API-key, or site access limits.

Symbol hints:

- SPY: Stooq symbol `spy.us`
- QQQ: Stooq symbol `qqq.us`
- IWM: Stooq symbol `iwm.us`
- TLT: Stooq symbol `tlt.us`
- GLD: Stooq symbol `gld.us`

## Verification

After placing files, run:

```bash
research_lab/.venv/bin/python -m research_lab.cli verify-downloaded-csvs \
  --csv-root research_lab/local_data/ohlcv \
  --universe local_etf_minimum_viable_v1
```

Then build only if the verifier says `ready_for_dataset_build=true`.
