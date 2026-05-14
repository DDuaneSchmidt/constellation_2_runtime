# portfolio_position_snapshot.v1

`portfolio_position_snapshot.v1` records current portfolio state after manual entries.

Sources are:

- `MANUAL`
- `CSV_IMPORT`
- `READ_ONLY_IB_FUTURE`

The artifact contains open positions, cash, net liquidation, gross exposure, net exposure, and exposure grouped by symbol, edge cluster, and sleeve. It is used by the next EOD report to surface concentration and portfolio context.
