# First Supervised Paper Trade

This workflow is supervised-only and capped at 1 trade/day.

Do not use broker automation. Aegis Lite produces a manual checklist and queue only.

## Preconditions

- `aegis_release_integrity_status.v1` reports `release_match_status=MATCH`.
- `aegis_lite_operating_status.v1` reports `manual_execution_only=true`.
- `ib_automation_status=DEFERRED`.
- The `/aegis-lite` UI has no active release mismatch warning.
- The candidate is in the `Executable Manual Trades` section, not `Blocked / Advisory Candidates`.

## Run Lite EOD

```bash
python3 ops/tools/check_aegis_release_integrity_v1.py
python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --day_utc YYYY-MM-DD --environment PAPER --manual-only --allow-not-ready-exit-zero
```

For UI/operator dry-run proof only:

```bash
python3 ops/tools/run_aegis_lite_eod_pipeline_v1.py --day_utc YYYY-MM-DD --environment PAPER --manual-only --demo-promoted-candidates --allow-not-ready-exit-zero
```

Dry-run candidates are marked `DEMO_ONLY` and `DRY_RUN_ONLY`. They prove the UI and checklist, not live decision quality.

## Operator Checklist

1. Open `/aegis-lite`.
2. Confirm the banner says this is not broker automation.
3. Confirm release match is `MATCH`.
4. Confirm `Broker submit=false` and `IB automation=DEFERRED`.
5. Pick at most one card from `Executable Manual Trades`.
6. Verify symbol, side, quantity, entry reference, stop price, stop quantity, and stop order type.
7. Manually enter the position in IB paper.
8. Immediately enter the protective stop.
9. Confirm the stop was accepted in IB.
10. Record `ENTERED`, `SKIPPED`, or `MODIFIED` using manual feedback artifacts.

## Stop Checklist

- Stop price must be present before entry.
- Stop quantity must match intended position quantity unless explicitly modified.
- Stop order type must be shown on the card.
- If the stop cannot be entered immediately, mark the trade skipped or modified and stop trading for the day.

## Feedback Artifacts

After manual action, record:

- `manual_operator_decision.v1`
- `manual_execution_event.v1` if entered
- `portfolio_position_snapshot.v1`
- `protective_order_snapshot.v1`
- `trade_outcome_attribution.v1` later

If a protective stop is missing, the next Lite report must surface a missing-stop warning.
