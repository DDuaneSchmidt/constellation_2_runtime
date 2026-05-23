# Minimum Viable ETF CSV Checklist

Need at least 3 real CSV files.

- [ ] SPY.csv
- [ ] QQQ.csv
- [ ] IWM.csv
- [ ] TLT.csv
- [ ] GLD.csv

Place files in:
research_lab/local_data/ohlcv

Then run:
research_lab/.venv/bin/python -m research_lab.cli first-dataset-status --csv-root research_lab/local_data/ohlcv
