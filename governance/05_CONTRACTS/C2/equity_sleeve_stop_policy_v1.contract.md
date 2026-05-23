# Equity Sleeve Stop/Invalidation Policy V1

This contract defines governed stop, invalidation, and paper trade risk-estimate semantics for active C2 equity and multi-asset sleeves that do not require a more specific contract. It is production-oriented risk control, but current runtime use remains PAPER ONLY. It does not authorize broker execution, live orders, real capital allocation, or automated paper submit.

## Hard Stop

For LONG exposures:

`stop_price = entry_reference_price * (1 - stop_loss_bps / 10000)`

For SHORT exposures:

`stop_price = entry_reference_price * (1 + stop_loss_bps / 10000)`

The registry supplies sleeve-specific `stop_loss_bps_default`. Implementations round stop prices to currency cents using half-up rounding.

## Invalidation

Each policy row must include invalidation metadata appropriate to the sleeve, such as signal decay, thesis invalidation, hedge failure, volatility regime invalidation, or time-based review. Invalidation metadata is allowed to support manual paper capture even before full automated exit execution exists.

## Risk Estimate

Risk construction computes:

- `risk_per_share = abs(entry_reference_price - stop_price)` when a hard stop exists.
- `max_loss_estimate = risk_per_share * suggested_quantity`.
- `estimated_notional_risk_pct = max_loss_estimate / suggested_notional`.

If quantity, entry, and stop/invalidation cannot produce a deterministic risk estimate, construction must fail closed with explicit blockers.

## Safety Boundary

This contract only supports construction of paper/manual trade tickets and manual capture annotations. Paper submit and execution readiness remain separate domains and are not enabled by this contract.
