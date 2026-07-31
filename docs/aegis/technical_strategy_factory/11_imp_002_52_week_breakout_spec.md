# IMP_002 — 52-Week Breakout Specification

## Purpose

Define a second implementation of CLAIM_001.

This implementation tests whether a new 52-week high breakout contains exploitable trend-persistence information.

## Claim Reference

```text
CLAIM_001 — Trend Persistence
```

## Implementation Definition

Use SPY only for the pilot implementation.

```text
Universe: SPY

Breakout lookback:
252 trading days

Signal:
SPY adjusted close > prior 252-trading-day highest adjusted close

Important:
The breakout threshold must use only prior data.
The current day close must not be included in the 252-day high used for that day’s signal.

Entry:
If SPY closes above the prior 252-trading-day high while in cash, enter long.

Exit:
Exit to cash if SPY closes below its 100-day simple moving average.

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

The 52-week breakout is related to technical breakout and momentum behavior. It is not assumed valid. It is being tested as one implementation under CLAIM_001.

Prior literature, including George and Hwang’s 2004 work on the 52-week high reference point, and Donchian-style breakout logic are useful context only. They are not certification.

## Pre-Registered Expected Failure Modes

```text
False breakouts
Whipsaw after marginal new highs
Late exits after trend reversals
Underperformance when breakouts occur late in bull markets
Low signal frequency causing small sample risk
Possible similarity to IMP_001 rather than independent claim evidence
```

## Important Rule

```text
IMP_002 may update CLAIM_001 only as one implementation.
It cannot certify CLAIM_001 by itself.
```
