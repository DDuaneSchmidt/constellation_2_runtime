# CLAIM_001 — Trend Persistence

## Status

`UNDER_INVESTIGATION`

## Claim Statement

Past price trends contain information about future price direction that can be harvested systematically after realistic implementation costs.

## Scope

- Technical price behavior only.
- No fundamentals.
- No macro forecasts.
- No news.
- No alternative data.
- No AI forecasts.

## Competing Hypotheses

H0: Past price movement contains no useful information about future price movement.

H1: Past price movement contains exploitable information.

H2: Information exists but is too weak to exploit after costs.

H3: Information existed historically but has decayed.

H4: Apparent persistence is explained by market beta, sector beta, or other known exposure.

## Registered Implementations

- `IMP_001` - SPY 200DMA Trend Following
- `IMP_002` - 52-Week Breakout
- `IMP_003` - Turtle Breakout
- `IMP_004` - Dual Momentum

## Initial Implementation Priority

`IMP_001` first, because it is simple, auditable, low parameter count, and deployable.

## IMP_001 Draft Specification

Universe: SPY

Signal: SPY close > 200-day simple moving average

Entry: If SPY closes above 200DMA while in cash, enter long

Exit: If SPY closes below 200DMA while long, exit to cash

Position size: 100% SPY or 100% cash

Signal evaluation: daily after close

Execution: next market open

Initial cost assumption: 10 bps round-trip, with higher-cost stress tests required

## Pre-Registered Expected Failure Modes

- Whipsaw in sideways markets
- Late exits during sharp reversals
- Long underperformance during strong bull markets
- Possible reduction in return despite drawdown improvement

## Initial Claim Confidence

`UNDER_INVESTIGATION`

## Important Rule

`IMP_001` results may update implementation evidence but cannot by themselves certify `CLAIM_001`.

## Next Required Artifact

`TEST_PLAN_001` for `IMP_001`.

Do not run or invent backtest results in this task.
