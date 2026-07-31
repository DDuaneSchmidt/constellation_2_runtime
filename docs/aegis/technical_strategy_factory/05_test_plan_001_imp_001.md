# TEST_PLAN_001

# IMP_001 — SPY 200DMA Trend Following

## Purpose

Determine whether the following implementation provides evidence supporting CLAIM_001.

Implementation:

```text
Universe: SPY

Signal:
SPY Close > 200-Day Simple Moving Average

Entry:
Enter long if SPY closes above 200DMA while in cash

Exit:
Exit to cash if SPY closes below 200DMA while long

Position Size:
100% SPY or 100% cash

Signal Evaluation:
Daily after close

Execution:
Next market open

Initial Cost Assumption:
10 bps round-trip
```

## Research Question

The test is NOT attempting to determine:

```text
Does SPY go up?
```

The test IS attempting to determine:

```text
Does the 200DMA signal improve outcomes
relative to appropriate alternatives?
```

## Development Period

```text
2000-01-01
through
2012-12-31
```

Purpose:

```text
Behavior observation only.

Not parameter optimization.
```

## Validation Period

```text
2013-01-01
through
present
```

Purpose:

```text
Out-of-sample evaluation.
```

## Parameter Freeze Rule

```text
The 200DMA parameter is frozen.

Changing the parameter creates a new implementation.

It does not modify IMP_001.
```

Examples:

```text
180DMA → new implementation

220DMA → new implementation
```

## Baseline Set

### Baseline A

```text
SPY Buy and Hold
```

Purpose:

```text
Determine whether timing adds value.
```

### Baseline B

```text
50% SPY / 50% Cash
```

Purpose:

```text
Determine whether improvements
are simply lower exposure.
```

### Baseline C

```text
Random Timing Model
```

Purpose:

```text
Determine whether timing signal
beats randomness.
```

Do not specify implementation details for the random model yet. Only register the requirement.

## Required Metrics

```text
Total Return
CAGR
Annual Volatility
Sharpe Ratio
Sortino Ratio
Maximum Drawdown
Ulcer Index
Time In Market
Trade Count
Turnover
Average Holding Period
```

## Primary Evaluation Questions

Question 1:

```text
Does IMP_001 improve
risk-adjusted outcomes
relative to SPY Buy and Hold?
```

Question 2:

```text
Does IMP_001 materially reduce drawdowns?
```

Question 3:

```text
Does IMP_001 survive
the validation period?
```

Question 4:

```text
Does IMP_001 survive
hostile review?
```

## Pre-Registered Expected Failure Modes

```text
Whipsaw during sideways markets

Late exits during sharp reversals

Underperformance during strong bull markets

Lower returns despite lower drawdowns
```

These observations must be recorded before results exist.

## Hostile Review Requirements

Register mandatory stress tests:

### Stress Test 1

Higher transaction costs.

Example:

```text
2x initial assumption
```

### Stress Test 2

Execution degradation.

Example:

```text
1-day delayed execution
```

### Stress Test 3

Parameter perturbation review.

Purpose:

```text
Determine whether nearby parameters
behave similarly.
```

Note:

```text
Nearby parameters are reviewed.

They do not modify IMP_001.
```

### Stress Test 4

Subperiod review.

Explicitly review:

```text
2000-2003

2008

2020

2022
```

## Replication Requirement

```text
IMP_001 cannot be supported
from a single run.

Independent replication is required.
```

## Paper Validation Requirement

```text
Minimum:
6 months

Preferred:
12 months
```

Signals must be recorded automatically.

## Possible Outcomes

Only:

```text
SUPPORTED

WEAK_SUPPORT

INSUFFICIENT_EVIDENCE

FALSIFIED
```

No capital allocation decisions.

## Important Claim-Level Rule

```text
A successful IMP_001 result
does not support CLAIM_001 by itself.

Multiple implementations must survive
before CLAIM_001 may become SUPPORTED.
```
