# TEST_PLAN_004

# IMP_004 — Dual Momentum

## Purpose

Pre-register the evaluation criteria before BACKTEST_004.

## Research Question

The test is not asking:

```text
Does SPY have a positive long-term return?
```

The test is asking:

```text
Does a 12-month absolute momentum filter improve outcomes versus appropriate baselines after realistic costs?
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
Absolute momentum lookback: 12 months
Evaluation frequency: monthly
Execution: next market open after signal period
Cost: 10 bps round-trip
```

Changing any of these creates a new implementation or stress test, not a modification to IMP_004.

## Required Baselines

Use the same baseline framework as IMP_001, IMP_002, and IMP_003:

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
Lookback perturbation: 9-month absolute momentum
Lookback perturbation: 15-month absolute momentum
Evaluation perturbation: weekly evaluation
Evaluation perturbation: quarterly evaluation
```

Perturbations must be reported separately and must not modify IMP_004.

## Hostile Review Requirements

Hostile review must evaluate:

```text
Higher costs
Execution delay
Lookback perturbation
Evaluation-frequency perturbation
Subperiod stress
Validation-period survival
Exposure baseline comparison
Random timing comparison
Turnover and tax burden
Hidden market beta exposure
```

## Evidence Requirements

BACKTEST_004 must produce the same evidence artifact classes used for BACKTEST_001, BACKTEST_002, and BACKTEST_003:

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

A successful IMP_004 result cannot certify CLAIM_001 by itself.
