# Mechanism Classification

Mechanism classification happens before handoff.

Allowed families:

- `OPENING_RANGE`
- `BREAKOUT`
- `MEAN_REVERSION`
- `TREND_CONTINUATION`
- `LIQUIDITY_SWEEP`
- `VOLATILITY_EXPANSION`
- `PULLBACK`
- `MOMENTUM`
- `VWAP_OR_AVERAGE_RECLAIM`
- `ATR_FILTER`
- `SESSION_TIMING`
- `UNKNOWN`

The extractor must prefer `UNKNOWN` over guessing. Channel reputation, video title popularity, or public search results cannot fill missing mechanism detail.
