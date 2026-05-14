# protective_order_snapshot.v1

`protective_order_snapshot.v1` confirms whether manually entered positions have protective stops.

Protection statuses are:

- `PROTECTED`
- `PARTIALLY_PROTECTED`
- `UNPROTECTED`
- `UNKNOWN`

Any open manual position without a confirmed stop produces a `MISSING_PROTECTIVE_STOP` warning in the next Aegis Lite EOD report. This is evidence for operator action, not an automated order-management loop.
