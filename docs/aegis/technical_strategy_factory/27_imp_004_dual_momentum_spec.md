# IMP_004 — Dual Momentum Specification

## Purpose

Define a fourth implementation of CLAIM_001.

This implementation tests whether a simple SPY absolute momentum filter contains exploitable trend-persistence information.

## Claim Reference

```text
CLAIM_001 — Trend Persistence
```

## Implementation Definition

Use SPY only for the pilot implementation.

```text
Universe: SPY

Absolute momentum filter:
SPY 12-month total return > 0

Signal:
SPY adjusted close is above its adjusted close 12 months prior.

Important:
The 12-month return must use only data available at the signal date.
The current monthly signal must not use future prices.

Entry:
Long SPY when the absolute momentum filter is positive while in cash.

Exit:
Exit to cash when the absolute momentum filter is not positive while long.

Position size:
100% SPY or 100% cash.

Signal evaluation:
Monthly after the signal period close.

Execution:
Next market open after signal period.

Initial cost:
10 bps round-trip = 5 bps on entry + 5 bps on exit.

Cash return:
0%.

No leverage.
No partial allocation.
No discretionary overrides.
```

## Rationale

Dual momentum is a known technical momentum implementation family. This pilot tests only the absolute momentum component using SPY and cash. It is not assumed valid. It is being tested as one implementation under CLAIM_001.

## Pre-Registered Expected Failure Modes

```text
Late exits after sharp reversals
Late re-entry after fast recoveries
Underperformance during strong bull markets
Whipsaw around flat 12-month returns
Low signal frequency causing small sample risk
Possible similarity to slower trend-following implementations rather than independent claim evidence
Possible return reduction despite drawdown improvement
```

## Important Rule

```text
IMP_004 may update CLAIM_001 only as one implementation.
It cannot certify CLAIM_001 by itself.
```
