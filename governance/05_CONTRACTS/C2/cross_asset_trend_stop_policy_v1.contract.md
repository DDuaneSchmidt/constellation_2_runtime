# Cross-Asset Trend Stop / Invalidation Policy v1

Contract ID: CROSS_ASSET_TREND_STOP_POLICY_V1
Scope: C2_CROSS_ASSET_TREND_V1 and successor cross-asset trend sleeves when explicitly registered.
Mode today: PAPER manual-capture construction only.

## Purpose

This contract defines a deterministic, governed stop and invalidation framework for cross-asset trend exposures. It is suitable for ETFs, broad index products, macro proxies, and other governed cross-asset trend instruments. It is not symbol-specific and must not encode ticker-specific behavior.

A selected exposure is not a trade. A Cross-Asset Trend selected exposure may become a manual paper-capture candidate only after paper trade construction has resolved current market data, capital authority, entry, quantity, notional, stop or invalidation, and risk estimate.

## Hard Stop Policy

For a LONG exposure:

`stop_price = entry_reference_price * (1 - stop_loss_bps / 10000)`

For a SHORT exposure:

`stop_price = entry_reference_price * (1 + stop_loss_bps / 10000)`

Initial governed default:

- `stop_loss_bps`: 1000
- effective hard stop distance: 10 percent
- rounding: currency price rounded to cents with half-up rounding

The default is deliberately wider than short-term tactical stops because the sleeve target holding period is trend-oriented. Future sleeve or asset-class overrides may narrow or widen this value only through governed registry/configuration changes.

## Invalidation Policy

The hard stop is the initial executable risk bound. The construction artifact must also carry invalidation metadata so future governed automation can reason about non-price exits without front-end inference.

Initial invalidation rules:

- `trend_signal_reversal`: invalidate when the source trend signal reverses against the position.
- `moving_average_regime_failure`: invalidate when the instrument fails the governed moving-average regime used by the sleeve.
- `time_based_invalidation`: review or invalidate when the exposure exceeds the governed expected holding period without continued trend confirmation. Initial expected holding period is 60 days.
- `volatility_regime_invalidation`: review or invalidate when volatility regime evidence invalidates the original trend construction assumptions.

These rules are metadata for paper/manual capture today. They do not authorize broker execution, order routing, automated paper submit, or live allocation.

## Risk Estimate

Paper trade construction must compute:

- `risk_per_share = abs(entry_reference_price - stop_price)` when a hard stop exists.
- `max_loss_estimate = suggested_quantity * risk_per_share`.
- `estimated_notional_risk_pct = max_loss_estimate / suggested_notional` when suggested notional is positive.

If a future construction uses only an invalidation level, the risk estimate must still be explicit and deterministic before ManualCaptureDomain can become READY.

## Readiness Semantics

StopRiskDomain is READY only when a stop price or invalidation level is present and risk-per-share plus max-loss estimate are present.

ManualCaptureDomain may become READY when MarketDataDomain, TradeConstructionDomain, and StopRiskDomain are READY. Manual capture readiness does not require PaperSubmitDomain or ExecutionDomain readiness unless a future governed rule explicitly changes that boundary.

PaperSubmitDomain remains separate and must continue to enforce submit-boundary governance. ExecutionDomain remains disabled unless explicitly enabled by a separate governed production authorization.

## Safety Boundaries

This contract does not authorize broker execution, live orders, real capital allocation, automated paper submit, candidate mutation, or governance bypass. For 2026-05-20 operational use, it is consumed only to construct PAPER manual-capture tickets and readiness-domain artifacts.
