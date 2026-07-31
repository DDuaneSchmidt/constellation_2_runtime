# TEST_PLAN_002

# IMP_002 — 52-Week Breakout

## Purpose

Pre-register the evaluation criteria before BACKTEST_002.

## Research Question

The test is not asking:

```text
Does SPY rise after all-time highs?
```

The test is asking:

```text
Does a 52-week breakout signal improve outcomes versus appropriate baselines after realistic costs?
```

## Development Period

```text
2000-01-01 through 2012-12-31
```

Purpose:

```text
Behavior observation only.
No parameter optimization.
```

## Validation Period

```text
2013-01-01 through latest available full trading day.
```

Purpose:

```text
Out-of-sample evaluation.
```

## Parameter Freeze Rule

The following are frozen:

```text
Breakout lookback: 252 trading days
Exit average: 100 trading days
Execution: next market open
Cost: 10 bps round-trip
```

Changing any of these creates a new implementation or stress test, not a modification to IMP_002.

## Required Baselines

Use the same baseline framework as IMP_001:

```text
SPY Buy and Hold
50% SPY / 50% Cash
Random Timing Model preserving approximate time-in-market
```

## Required Metrics

```text
total_return
cagr
annual_volatility
sharpe
sortino
maximum_drawdown
ulcer_index
time_in_market
trade_count
turnover
average_holding_period_days
```

## Required Subperiod Review

```text
2000-2003
2008
2020
2022
Development period
Validation period
```

## Required Stress Tests

```text
20 bps round-trip cost
50 bps round-trip cost
1-day execution delay
Breakout perturbation: 200-day breakout
Breakout perturbation: 300-day breakout
Exit perturbation: 50DMA exit
Exit perturbation: 150DMA exit
```

Perturbations must be reported separately and must not modify IMP_002.

## Possible Outcomes

```text
SUPPORTED
WEAK_SUPPORT
INSUFFICIENT_EVIDENCE
FALSIFIED
```

## Claim-Level Rule

A successful IMP_002 result cannot certify CLAIM_001 by itself.

