# Aegis Event Dislocation Position State Freshness Repair Spec v1

Artifact family: `aegis_event_dislocation_position_state_freshness_repair_v1`

Filename: `event_dislocation_position_state_freshness_repair.v1.json`

Target sleeve: `C2_EVENT_DISLOCATION_V1`

Freshness rule:

The position-state payload must have `day_utc` or `as_of_day_utc` equal to the target day, status must not be `BLOCKED`, `ERROR`, `FAILED`, or `STALE`, `positions.items` must be present as a list, and `input_manifest` must be present.

Repair rule:

If the governed source state under `truth_sleeves/PRIMARY/PAPER/positions_snapshot_v2/snapshots/<TARGET_DAY>/positions_snapshot.v2.json` is fresh and lineaged, T05 may route that exact payload to `truth_sleeves/PRIMARY/PAPER/positions_v1/snapshots/<TARGET_DAY>/positions_snapshot.v2.json` for consumers that only read the `positions_v1` contract path.

Fail-closed rule:

Missing, stale, malformed, or lineage-missing source state is not repaired. T05 reports the exact blocker and leaves consumer state unrouted.
