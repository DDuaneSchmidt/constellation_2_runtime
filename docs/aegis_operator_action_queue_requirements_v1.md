# Aegis Operator Action Queue Requirements V1

## Scope

Create one canonical David action queue for hypothesis workflow actions. The queue contains only concrete actions David needs to take and excludes vague review rows.

## Requirements

1. Produce `aegis_operator_action_queue_v1`.
2. Include only allowed action types: `APPROVE_PAPER_TEST`, `PROVIDE_DATA_SOURCE`, `REVIEW_RETIREMENT`, and `REVIEW_CAPITAL`.
3. Each action item must include action id, hypothesis identity, action type, urgency, reason, exact buttons, consequence of each button, safety statement, source state hash, source paths, and created timestamp.
4. If David does not need to act, the queue is empty.
5. The UI must render buttons only from `exact_buttons`.
6. No queue action may imply broker execution, live trading, trade advice, real capital, autonomous execution, order management, or automatic real-world position management.
