# Aegis Data Action Routing Design v1

## Design

`aegis_data_action_routing_v1` reads existing missing-data evidence and emits a compact routing table for UI and scorecard consumers.

The operator action queue is authoritative for David-owned missing data. A queue item with `PROVIDE_DATA_SOURCE` maps to `OPERATOR_PROVIDED_DATA_REQUIRED`, `owner=DAVID`, and exposes the exact buttons from the action queue.

The Oil Shock candidate-flow artifact is authoritative for Oil Shock. If Oil Shock reports `MISSING_DATA` and no explicit David action exists, the row maps to `WAITING_FOR_MARKET_DATA` when the blocker is market-data/evidence availability, or `SYSTEM_DATA_PIPELINE_REQUIRED` when the system must create or repair a data pipeline. In both cases `david_action_required=false`.

## Consumer Contract

Research Daily Scorecard attaches routing fields to generated-hypothesis rows when available. Command Center renders blocker, classification, owner, David action yes/no, and next step. The UI must not display multiple `MISSING_DATA` rows without an explanation of why only one is actionable.

## Safety

The producer only reads and writes JSON evidence. It does not call broker APIs, generate candidates, change paper tracking, mutate safety policy, or create trading advice.
