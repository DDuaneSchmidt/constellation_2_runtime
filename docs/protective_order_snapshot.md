# protective_order_snapshot.v1

`protective_order_snapshot.v1` confirms whether manually entered positions have protective stops.

Protection statuses are:

- `PROTECTED`
- `PARTIALLY_PROTECTED`
- `UNPROTECTED`
- `UNKNOWN`

Any open manual position without a confirmed stop produces a `MISSING_PROTECTIVE_STOP` warning in the next Aegis Lite EOD report. This is evidence for operator action, not an automated order-management loop.

## Protection Requirements

`PROTECTED` requires:

- `stop_exists=true`
- non-empty `stop_price`
- `stop_quantity > 0`
- `stop_quantity >= abs(position quantity)`

`PARTIALLY_PROTECTED` means a valid stop exists but does not cover the full position quantity. `UNPROTECTED` means no valid stop exists. `UNKNOWN` means source data is incomplete. Any status other than `PROTECTED` blocks `READY_FOR_MANUAL_ENTRY`.
