# TEST_PLAN_003

# IMP_003 — Turtle Breakout

## Purpose

Pre-register the evaluation criteria before BACKTEST_003.

## Research Question

The test is not asking:

```text
Does SPY rise after breakouts?
```

The test is asking:

```text
Does a 55-day breakout with a 20-day breakdown exit improve outcomes versus appropriate baselines after realistic costs?
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
Entry breakout lookback: 55 trading days
Exit breakdown lookback: 20 trading days
Execution: next market open
Cost: 10 bps round-trip
```

Changing any of these creates a new implementation or stress test, not a modification to IMP_003.

## Required Baselines

Use the same baseline framework as IMP_001 and IMP_002:

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
Entry breakout perturbation: 40-day breakout
Entry breakout perturbation: 70-day breakout
Exit breakdown perturbation: 10-day low exit
Exit breakdown perturbation: 30-day low exit
```

Perturbations must be reported separately and must not modify IMP_003.

## Hostile Review Requirements

Hostile review must evaluate:

```text
Higher costs
Execution delay
Entry lookback perturbation
Exit lookback perturbation
Subperiod stress
Validation-period survival
Exposure baseline comparison
Random timing comparison
Turnover and tax burden
Hidden market beta exposure
```

## Evidence Requirements

BACKTEST_003 must produce the same evidence artifact classes used for BACKTEST_001 and BACKTEST_002:

```text
Summary
Metrics
Random baseline
Subperiods
Stress tests
Run manifest
Evidence report
```

## Possible Outcomes

```text
SUPPORTED
WEAK_SUPPORT
INSUFFICIENT_EVIDENCE
FALSIFIED
```

## Claim-Level Rule

A successful IMP_003 result cannot certify CLAIM_001 by itself.
