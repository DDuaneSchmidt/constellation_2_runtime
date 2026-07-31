# Aegis Operator Action Event Log Spec V1

## Artifact

`reports/aegis_operator_action_event_log_v1/{day}/operator_action_events.v1.jsonl`

## Contract

The log is append-only. Each UI action appends one immutable event with:

- `event_id`
- `action_id`
- `hypothesis_id`
- `actor`
- `action_type`
- `button_clicked`
- `prior_state`
- `new_state`
- `source_state_hash`
- `event_timestamp_utc`
- `safety_statement`
- `no_broker_execution`
- `no_trade_advice`
- `no_live_trading`
- `no_real_capital`

## Phase 1 Events

Supported buttons are `APPROVE_PAPER_TEST`, `REJECT_PAPER_TEST`, `DEFER_PAPER_TEST`, `DEFER_DATA_SOURCE`, and `MARK_DATA_NOT_AVAILABLE`. Events record operator intent only; they do not execute trades, create orders, allocate real capital, or enable live systems.
