# operator_execution_queue.v1

`operator_execution_queue.v1` turns Aegis Lite candidates into an operator-safe manual execution queue.

Initial supported trade classes:

- `LONG_EQUITY`
- `SHORT_EQUITY`
- `LONG_CALL_OPTION`
- `LONG_PUT_OPTION`
- `ETF_ROTATION_PAIR`

Unsupported structures fail closed as `UNSUPPORTED_MANUAL_EXECUTION`. This includes multi-leg option spreads, futures combos, advanced linked orders, autonomous IB routing, and broker-managed execution lifecycle.

The queue includes execution order, priority rank, trade class, manual recipe, required orders, stop requirement, skip flag, overlap group, edge cluster, risk bucket, and operator confirmations.

## Queue Readiness

A queue item is `READY_FOR_MANUAL_ENTRY` only when the trade class is supported and the candidate has a positive quantity, symbol, direction, instrument type, entry reference, stop, risk, no candidate blockers, and a complete manual recipe.

Blocked or unsupported queue items prevent the EOD report from emitting `READY_FOR_MANUAL_ENTRY`.

Executable recipes must state account/mode when available, side, symbol, quantity, order type, entry instruction, stop order type, stop price, stop quantity, sequence, price-moved instruction, and operator confirmations. The required sequence is:

1. Enter position.
2. Immediately enter protective stop.
3. Confirm stop accepted.
