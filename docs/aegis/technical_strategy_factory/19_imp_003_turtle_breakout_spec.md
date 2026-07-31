# IMP_003 — Turtle Breakout Specification

## Purpose

Define a third implementation of CLAIM_001.

This implementation tests whether a shorter Donchian/Turtle-style breakout with a separate trailing breakdown exit contains exploitable trend-persistence information.

## Claim Reference

```text
CLAIM_001 — Trend Persistence
```

## Implementation Definition

Use SPY only for the pilot implementation.

```text
Universe: SPY

Entry breakout lookback:
55 trading days

Signal:
SPY adjusted close > prior 55-trading-day highest adjusted close

Important:
The breakout threshold must use only prior data.
The current day close must not be included in the 55-day high used for that day’s signal.

Entry:
If SPY closes above the prior 55-trading-day high while in cash, enter long.

Exit:
Exit to cash if SPY adjusted close is below the prior 20-trading-day lowest adjusted close.

Important:
The exit threshold must use only prior data.
The current day close must not be included in the 20-day low used for that day’s exit signal.

Position size:
100% SPY or 100% cash.

Signal evaluation:
Daily after close.

Execution:
Next market open.

Initial cost:
10 bps round-trip = 5 bps on entry + 5 bps on exit.

Cash return:
0%.

No leverage.
No partial allocation.
No discretionary overrides.
```

## Rationale

Turtle-style breakout logic is a known technical trend-following implementation family. It is not assumed valid. It is being tested as one implementation under CLAIM_001.

## Pre-Registered Expected Failure Modes

```text
False breakouts
Whipsaw after short-lived breakouts
Late exits after sharp reversals
Underperformance during range-bound markets
Higher turnover than slower trend-following rules
Possible similarity to IMP_002 rather than independent claim evidence
Possible return reduction despite drawdown improvement
```

## Important Rule

```text
IMP_003 may update CLAIM_001 only as one implementation.
It cannot certify CLAIM_001 by itself.
```
