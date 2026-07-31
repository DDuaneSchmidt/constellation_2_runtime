# BACKTEST_001 Data and Baseline Specification

# IMP_001 — SPY 200DMA Trend Following

## Purpose

Freeze the final unresolved assumptions before BACKTEST_001:

```text
Data source
Adjusted price policy
Cost application
Random timing baseline
Output reproducibility requirements
```

## Data Source

Primary data source: Yahoo Finance via yfinance or equivalent local adapter.

```text
Ticker: SPY
Frequency: Daily
Start date: 2000-01-01
End date: latest available full trading day at execution time
Price field: adjusted close for signal calculations unless execution-open adjusted series is available and documented.
```

If adjusted open prices are unavailable, execution using next open must document the chosen open-price proxy and classify the run as preliminary until a fully adjusted OHLC source is available.

## Corporate Action Policy

```text
Dividend-adjusted and split-adjusted prices must be used for long-horizon return calculations.
Splits and dividends must not be ignored.
Any mismatch between signal price series and execution price series must be recorded.
```

## Cost Application

```text
10 bps round-trip cost.
5 bps applied on entry.
5 bps applied on exit.
Costs deducted from strategy returns at each trade event.
```

Stress tests:

```text
20 bps round-trip
50 bps round-trip
1-day execution delay
```

## Random Timing Baseline

```text
Create randomized timing portfolios that preserve the same approximate time-in-market as IMP_001.
Use a fixed random seed.
Run at least 1,000 randomized simulations.
Report median, 5th percentile, and 95th percentile outcomes for required metrics.
IMP_001 must be compared against this distribution.
```

Required random baseline outputs:

```text
Median random CAGR
Median random max drawdown
Median random Sharpe
IMP_001 percentile rank versus random distribution
```

## Exposure Baseline

```text
50% SPY / 50% cash, rebalanced daily or monthly.
The rebalance frequency must be explicitly recorded before execution.
```

Preferred for BACKTEST_001:

```text
Monthly rebalance.
```

## Cash Return Policy

```text
Initial version may assume cash return = 0%.
This must be explicitly labeled conservative/simplified.
Future versions may use Treasury bill returns, but that requires a new data specification version.
```

## Output Reproducibility

Backtest output must record:

```text
Data vendor
Data download timestamp
Ticker
Date range
Price fields used
Adjustment policy
Cost model version
Random seed
Number of random simulations
Code version or script path
Run timestamp
```

## Authorization Rule

BACKTEST_001 may be authorized only after this specification exists.

No results should be generated in this task.

## Completion Criteria

File exists:

```text
docs/aegis/technical_strategy_factory/07_backtest_001_data_and_baseline_spec.md
```
