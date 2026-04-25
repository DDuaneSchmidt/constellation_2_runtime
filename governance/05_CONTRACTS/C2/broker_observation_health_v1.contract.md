# broker_observation_health_v1.contract

owner: broker_observation_health_v1
scope: downstream reliance posture for Core 1

Health contract rules:
- TRUSTED permits normal downstream consumption.
- DEGRADED permits consumption only under explicit degraded-posture allowances.
- BLOCKED requires fail-closed downstream behavior.

First-class states:
- replay_status: NOT_OBSERVED, REPLAYING, REPLAY_COMPLETE, REPLAY_UNCERTAIN
- reconnect_status: NONE, RECONNECT_IN_PROGRESS, RECONNECT_RECOVERED
- gap_status: NONE, GAP_SUSPECTED, GAP_UNRESOLVED
- attribution_status: ATTRIBUTED, PARTIAL, AMBIGUOUS, FOREIGN, UNRESOLVED

Required outputs:
- downstream_consumption_posture
- may_consume_normally
- may_consume_with_degraded_posture
- must_fail_closed
- blocker_codes
- degraded_codes
