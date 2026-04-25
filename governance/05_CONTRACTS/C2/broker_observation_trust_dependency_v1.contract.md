# broker_observation_trust_dependency_v1.contract

owner: broker_observation_trust_dependency_v1
scope: machine-readable downstream dependency contract for Core 1

Downstream obligations:
- Core 2 may consume only the canonical Core 1 fact ledgers plus broker_observation_health.v1 and broker_observation_trust_dependency.v1.
- Raw broker evidence journal is provenance only and is forbidden as direct canonical Core 2 input.
- NORMAL_CONSUMPTION means Core 2 may consume normally.
- DEGRADED_ONLY means Core 2 may consume only under an explicit degraded posture.
- FAIL_CLOSED means Core 2 must not consume Core 1 for reconciled trade truth or action use.

Consumption matrix:
- trust_verdict=TRUSTED and replay_status in {REPLAY_COMPLETE, NOT_OBSERVED} and reconnect_status=NONE and gap_status=NONE and attribution_status=ATTRIBUTED => core2_consumption_mode=NORMAL_ONLY.
- trust_verdict=DEGRADED and no blocker codes => core2_consumption_mode=DEGRADED_ALLOWED.
- trust_verdict=BLOCKED => core2_consumption_mode=FAIL_CLOSED.
- replay_status in {REPLAYING, REPLAY_UNCERTAIN} => core2_consumption_mode=FAIL_CLOSED.
- reconnect_status=RECONNECT_IN_PROGRESS => core2_consumption_mode=FAIL_CLOSED.
- gap_status=GAP_UNRESOLVED => core2_consumption_mode=FAIL_CLOSED.
- attribution_status in {AMBIGUOUS, FOREIGN, UNRESOLVED} => core2_consumption_mode=FAIL_CLOSED.
- attribution_status=PARTIAL may only remain DEGRADED_ALLOWED; it must not silently promote to normal consumption.

Required machine-readable fields:
- trust_verdict
- downstream_consumption_posture
- may_consume_normally
- may_consume_with_degraded_posture
- must_fail_closed
- core2_consumption_mode
- replay_status
- reconnect_status
- gap_status
- attribution_status
- core2_allowed_artifact_families[]
- core2_forbidden_artifact_families[]
- blocker_codes[]
- degraded_codes[]
