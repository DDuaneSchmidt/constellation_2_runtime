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
