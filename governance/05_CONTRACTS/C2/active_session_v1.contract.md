# active_session_v1

`active_session_v1` is the single runtime authority for the active session day.

Canonical output:
- `/home/node/constellation_runtime_data/truth/active_session_v1/current.json`

Rules:
- it must be derived from binding `target_day_admission_v1` and the explicit `session_promotion_decision_v1`
- it must never activate a blocked target day
- it must retain traceability to both the active-day admission artifact and the active-day build artifact
- when present, `promotion_state` and `promotion_decision_ref` must identify the promotion decision that governed the current-state write
- when the target day is blocked:
  - `rollover_status` must be `ROLLOVER_WITHHELD`
  - `rollover_reason_code` must use the stable blocker taxonomy
  - `rollover_reason_summary` must remain operator-readable
  - `blocked_target_day` and `blocked_admission_ref` must be populated
- wrappers and execution entrypoints must read `active_session_v1/current.json` instead of deriving day ownership independently
- the prior admitted active day may be retained only as historical context while rollover is withheld
