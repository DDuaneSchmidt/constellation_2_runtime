# decision_plan_v1

Defines the governed review-ready plan artifact assembled from compiled actions.

Required fields:
- `plan_id`
- `planning_snapshot_id`
- `advisory_packet_id`
- `actions`
- `blocked_actions`
- `assumptions_used`
- `constraints_used`
- `replan_triggers`
- `version`

Rules:
- built deterministically from typed inputs
- blocked actions must remain explicit
- no execution or trading logic in this phase
