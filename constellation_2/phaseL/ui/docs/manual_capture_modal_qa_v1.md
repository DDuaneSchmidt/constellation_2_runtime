# Manual Capture Modal QA v1

Scope: verify the Aegis manual capture workflow records operator evidence only. Do not use this script to submit, route, transmit, or create broker orders.

## Preconditions

- Truth root: `/home/node/constellation_runtime_data/truth`
- Day: `2026-05-20`
- DOW / `C2_MEAN_REVERSION_EQ_V1` ticket is `ACTIVE_CURRENT`
- Submit-boundary is `VALIDATED`
- Broker submit/transmit is disabled
- Autonomous execution is disabled
- Trade advice is disabled unless RuntimeEvaluation explicitly allows it

## Viewport Checks

Run the workflow at these viewport sizes:

- `1366x768`
- `1440x900`
- `1600x900`
- `1920x1080`

For each size:

1. Load the Aegis operator UI.
2. Open the current DOW manual capture ticket.
3. Click `Record capture`.
4. Confirm the `Record Manual Capture` modal opens centered.
5. Confirm `Record capture` is visible immediately without page scrolling.
6. Confirm the page behind the modal does not scroll.
7. Confirm there is no horizontal scrollbar.
8. Confirm no technical evidence drawer is shown in the operator modal.

## Payload Checks

1. Enter a fill price, fill time, quantity, stop price, operator ID, and optional notes.
2. Before saving, inspect the outgoing browser request.
3. Confirm the payload includes:
   - `ticket_id`
   - `runtime_evaluation_hash`
   - `ticket_lineage_hash`
   - `submit_boundary_hash`
   - `capture_status`
   - `fill_price`
   - `fill_time_local`
   - `fill_time_utc`
   - `quantity`
   - `stop_price`
   - `operator_id`
   - `notes`
4. Confirm `fill_time_utc` is an ISO UTC timestamp derived from the local fill time.

## Save Checks

Only save with real operator-provided values. For dry-run validation, stop after the payload/precheck inspection and do not append evidence.

If saving is authorized:

1. Click `Record capture`.
2. Confirm success text: `Manual capture recorded. No broker action was taken.`
3. Confirm record id, event ids, and timestamp are shown.
4. Confirm the ticket becomes read-only/captured after refresh.
5. Confirm no broker submit/transmit event was created.
