# Aegis Lite Event Awareness

Aegis Lite remains EOD-centered. The official daily decision state is produced by the canonical 15:50 ET EOD run.

The Event Awareness Layer is non-canonical. It reads `event_rules_registry.v1`, may surface unusual intraday conditions, request operator review, write `event_monitoring_status.v1` and `event_awareness_ledger.v1`, and produce an optional `event_tactical_packet.v1`.

Event awareness cannot:
- overwrite EOD state
- submit broker orders
- enable IB automation
- create promoted sleeves
- promote research
- mutate Research Lab artifacts directly

## Alert Levels

- `INFO` and `WATCH`: awareness only.
- `TACTICAL`: suggests manual tactical review.
- `ACTIONABLE`: allowed only after `event_validity_gate.v1` returns `PASS`.
- `BLOCKED`: event was detected but is not safe/manual executable.

## Event Rules And Monitor

The operator-readable rule source is `governance/02_REGISTRIES/C2_EVENT_RULES_REGISTRY_V1.json`.

Run manually with an explicit truth root:

```bash
python3 ops/tools/run_aegis_event_monitor_v1.py --truth_root <safe_truth_root> --day_utc YYYY-MM-DD --market_snapshot_json <snapshot.json>
```

Rules are snapshotted into runtime truth for replay. Missing rules, disabled rules, stale data, missing inputs, research-only rules, no promoted sleeve, and low-confidence events fail closed.

Read-only operator surfaces:

- `/aegis-events`
- `/api/aegis/event-monitoring`
- `python3 ops/tools/read_aegis_event_monitoring_surface_v1.py --truth_root <safe_truth_root> --day_utc YYYY-MM-DD`

## Event Validity Gate

The validity gate blocks tactical packets when required manual-capture fields are missing, the event is stale, price has moved beyond max slippage, or execution sensitivity is `EXTREME`.

`HIGH` sensitivity can pass only with an explicit warning. `EXTREME` is not suitable for manual Aegis Lite capture.

## Manual Execution

If David manually acts on an event packet, `manual_execution_receipt.v1` records `source_packet_type=EVENT_TACTICAL_PACKET`, `event_id`, `event_run_id`, fill timing, entry deviation, valid-until compliance, slippage compliance, stop entry, and operator notes.

Event tactical packets are separate from `manual_trade_packet.v1` and never overwrite it.

## Trade Capture Alerts

`trade_capture_alert_gate.v1` decides whether a packet is eligible to interrupt David. In current v1, this is a **gate and ledger only**. It writes SMS/email message bodies and records `DRY_RUN_MESSAGE_BODY_ONLY` or `NOT_SENT`, but no real SMS/email transport is proven or wired.

The gate can mark a packet delivery-eligible only when:

- the event validity gate is `PASS`
- the source event tactical packet exists
- symbol, side, entry, sizing, stop/risk, max slippage, and valid-until fields are complete
- execution sensitivity is not `EXTREME`
- enough time remains before `valid_until`

Minimum time remaining:

- `LOW`: 30 minutes
- `MEDIUM`: 15 minutes
- `HIGH`: 5 minutes
- `EXTREME`: always blocked

Only `ACTIONABLE_TRADE` and `URGENT_ACTIONABLE_TRADE` may become delivery-eligible. `INFO`, `WATCH`, `TACTICAL`, `BLOCKED`, `EXPIRED`, `INVALID`, and `MISSED_VALIDITY_WINDOW` are never delivery-eligible.

`trade_capture_alert_ledger.v1` records every alert attempt, including blocked no-alert decisions. Duplicate SMS alerts for the same unchanged packet are suppressed at the gate/ledger layer. Until a transport is explicitly added and validated, the operator-facing transport status is `GATE_ONLY_NO_TRANSPORT`; delivery rows use `DRY_RUN_MESSAGE_BODY_ONLY` for message bodies that would have been eligible for delivery.

Event tactical packets carry `runtime_truth_classification` with values `REAL_RUNTIME`, `DEMO_ONLY`, or `DRY_RUN_ONLY`. Demo and dry-run packets are never actionable and cannot pass the event validity or trade-capture alert gates.

## Outcome And Research

`outcome_ledger.v1` can attribute outcomes to either EOD manual packets or event tactical packets. Event outcomes may create offline Research Lab follow-up tasks such as event failure, success, stale-entry, false-positive, or overlap reviews. These tasks cannot create trades or production changes.
