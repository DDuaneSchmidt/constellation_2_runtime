# Aegis Lite Event Awareness

Aegis Lite remains EOD-centered. The official daily decision state is produced by the canonical 15:50 ET EOD run.

The Event Awareness Layer is non-canonical. It may surface unusual intraday conditions, request operator review, write `event_awareness_ledger.v1`, and produce an optional `event_tactical_packet.v1`.

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

## Event Validity Gate

The validity gate blocks tactical packets when required manual-capture fields are missing, the event is stale, price has moved beyond max slippage, or execution sensitivity is `EXTREME`.

`HIGH` sensitivity can pass only with an explicit warning. `EXTREME` is not suitable for manual Aegis Lite capture.

## Manual Execution

If David manually acts on an event packet, `manual_execution_receipt.v1` records `source_packet_type=EVENT_TACTICAL_PACKET`, `event_id`, `event_run_id`, fill timing, entry deviation, valid-until compliance, slippage compliance, stop entry, and operator notes.

Event tactical packets are separate from `manual_trade_packet.v1` and never overwrite it.

## Trade Capture Alerts

`trade_capture_alert_gate.v1` decides whether David should be interrupted by SMS/email. It can alert only when:

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

Only `ACTIONABLE_TRADE` and `URGENT_ACTIONABLE_TRADE` may send email/SMS. `INFO`, `WATCH`, `TACTICAL`, `BLOCKED`, `EXPIRED`, `INVALID`, and `MISSED_VALIDITY_WINDOW` never send.

`trade_capture_alert_ledger.v1` records every alert attempt, including blocked no-alert decisions. Duplicate SMS alerts for the same unchanged packet are suppressed.

## Outcome And Research

`outcome_ledger.v1` can attribute outcomes to either EOD manual packets or event tactical packets. Event outcomes may create offline Research Lab follow-up tasks such as event failure, success, stale-entry, false-positive, or overlap reviews. These tasks cannot create trades or production changes.
