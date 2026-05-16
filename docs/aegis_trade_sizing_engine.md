# Aegis Trade Sizing Engine v1

## Purpose

`aegis_trade_sizing_engine_v1` adds conservative, deterministic sizing guidance to Aegis Lite manual trade packets. It is advisory/manual only.

It does not:

- submit broker orders
- connect to IB or TWS
- auto-execute trades
- allow AI to choose size
- activate production

## Flow

`Sleeve Signal -> Trade Eligibility Gate -> Risk Budget Engine -> Overlap / Concentration Engine -> Regime Risk Adjustment -> Sizing Engine -> Manual Trade Packet -> Receipt / Outcome Review`

## Risk Tiers

| Tier | Rule |
| --- | --- |
| `SMOKE_TEST` | 1 share |
| `VERY_SMALL` | 0.10% portfolio risk |
| `NORMAL` | 0.25% portfolio risk |
| `STRONG` | 0.50% portfolio risk |
| `MAX` | 1.00% portfolio risk |

Early paper mode caps percentage sizing at 0.25% portfolio risk. Initial manual paper trading should use `SMOKE_TEST` or `VERY_SMALL`.

## Formula

`allowed_dollar_risk / abs(entry_reference_price - stop_price) = suggested_quantity`

Quantity is floored to whole shares. `SMOKE_TEST` always returns one share when the packet is otherwise eligible.

## Blocking Rules

Sizing is blocked for:

- `DEMO_ONLY`
- `DRY_RUN_ONLY`
- stale packets
- unpromoted sleeves
- missing symbol or side
- missing entry
- missing stop price or stop logic
- missing risk rules
- invalid runtime truth
- invalid stop distance
- concentration cap blocks

Blocked sizing makes the manual trade packet non-actionable.

## Manual Packet Fields

Each packet candidate includes:

- `portfolio_value_used`
- `sizing_tier`
- `risk_pct_used`
- `allowed_dollar_risk`
- `entry_reference_price`
- `stop_price`
- `risk_per_share`
- `suggested_quantity`
- `estimated_position_value`
- `max_loss_if_stopped`
- `overlap_adjustment`
- `regime_adjustment`
- `concentration_adjustment`
- `sizing_blockers`
- `sizing_reason_codes`
- `ai_selected_size=false`
- `operator_can_override=true`

## Receipt / Outcome Review

Manual execution receipts record suggested quantity, actual quantity, quantity override status, expected risk, actual risk, override reason, and sizing quality.

Outcome ledger rows preserve the same sizing comparison so sleeve performance can review:

- suggested quantity vs actual quantity
- expected risk vs actual risk
- operator override reason
- stop behavior
- sizing quality

## Safety Boundary

Sizing guidance is not authorization to trade. The operator must still verify the packet, confirm all gates are pass/ready, enter manually, place the protective stop, and record the receipt.
