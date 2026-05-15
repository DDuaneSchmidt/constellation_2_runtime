# Aegis Lite Operator UI

Primary screen: `Aegis Lite Queue` at `/aegis-lite`.

Primary API: `/api/aegis/lite-execution-queue`.

The screen reads current `aegis_lite_operating_status.v1`, then the current Lite EOD report and execution queue referenced by that status. It does not use legacy `aegis_operator_state.v1` for readiness or executable trade display.

If active release integrity reports a mismatch, the UI downgrades readiness to advisory and disables executable trade display.

Executable cards show the fields required for manual IB paper entry:

- symbol, side, direction, class, quantity
- entry reference
- stop price, stop quantity, stop order type
- risk per trade and sleeve owner
- edge cluster and governance recommendation
- manual IB recipe
- required operator steps
- source sleeve, promotion status, edge cluster, and report timestamp
- confidence badges such as `GOVERNED_READY`, `ADVISORY_ONLY`, `BLOCKED`, `DEMO_ONLY`, and `DRY_RUN_ONLY`

Blocked/advisory candidates are visually separate and marked do-not-trade. If no current Lite report or queue exists, the UI shows `NOT_READY / NO_CURRENT_LITE_REPORT` or `NOT_READY / NO_CURRENT_LITE_QUEUE`.
