# Aegis Event Monitoring v1

Event Monitoring is the non-canonical intraday awareness path for Aegis Lite:

`event_rules_registry.v1 -> event monitor -> event_awareness_ledger.v1 -> tactical_review_gate.v1 -> event_tactical_packet.v1 -> event_validity_gate.v1 -> trade_capture_alert_gate.v1 -> alert ledger -> manual receipt -> outcome ledger -> offline Research Lab feedback`

Canonical EOD remains the official daily decision state. Event runs cannot overwrite `manual_trade_packet.v1`, `aegis_lite_eod_report.v1`, or the operator execution queue.

## Command

Run the monitor only with an explicit safe truth root:

```bash
python3 ops/tools/run_aegis_event_monitor_v1.py \
  --truth_root /tmp/aegis_event_monitor_truth \
  --day_utc YYYY-MM-DD \
  --market_snapshot_json /path/to/event_market_snapshot.json
```

The market snapshot must include `generated_at_utc`, input values required by registry rules, optional `current_prices`, optional `promoted_sleeves`, and optional tactical packet defaults. Missing or stale data blocks evaluation.

## Outputs

The monitor writes:

- `event_rules_registry.v1` snapshot
- `event_monitoring_status.v1`
- `event_awareness_ledger.v1`
- `tactical_review_gate.v1` when a rule triggers
- `event_tactical_packet.v1` only when tactical review is allowed and the rule requests a packet
- `event_validity_gate.v1`
- `trade_capture_alert_gate.v1`
- `trade_capture_alert_ledger.v1`

All artifacts carry `broker_submit_required=false`, `canonical_eod_state_mutated=false`, and manual-only semantics.

## Operator Surface

The read-only UI/API surface is:

- page: `/aegis-events`
- API: `/api/aegis/event-monitoring`
- CLI: `python3 ops/tools/read_aegis_event_monitoring_surface_v1.py --truth_root <path> --day_utc <YYYY-MM-DD>`

It shows rules, monitor status, event ledger, actionable packet details, alert delivery status, and Research learning boundaries.

## Alert Transport

Current email/SMS transport status is `GATE_ONLY_NO_TRANSPORT`.

The alert gate and ledger produce message bodies and record `DRY_RUN_MESSAGE_BODY_ONLY` when an actionable email would be eligible, but no real email/SMS is sent. The operator-facing transport status is `GATE_ONLY_NO_TRANSPORT`. Live transport must be added later behind `trade_capture_alert_gate.v1`; it must not create trades.

## Demo / Dry-Run Guardrail

`event_tactical_packet.v1` includes `runtime_truth_classification`:

- `REAL_RUNTIME`
- `DEMO_ONLY`
- `DRY_RUN_ONLY`

Only `REAL_RUNTIME` packets may become actionable. `DEMO_ONLY` and `DRY_RUN_ONLY` packets are visibly labeled in the UI and are blocked by both the event validity gate and trade capture alert gate.

## Safety

- No broker or IB dependency.
- No submit/transmit/fill automation.
- No production activation.
- No EOD state mutation from event runs.
- No Research Lab mutation except later explicit offline learning tasks.
- `EXTREME` sensitivity is blocked.
- `ACTIONABLE` requires validity gate `PASS` and alert gate `ACTIONABLE_TRADE` or `URGENT_ACTIONABLE_TRADE`.
