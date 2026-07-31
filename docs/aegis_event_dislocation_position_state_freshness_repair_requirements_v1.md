# Aegis Event Dislocation Position State Freshness Repair Requirements v1

Package T05 repairs or precisely explains the `C2_EVENT_DISLOCATION_V1` `POSITION_STATE_STALE` blocker that remained after T04 market-data repair.

The repair is limited to AEGIS-owned position-state freshness, routing, deterministic artifact generation, timestamp validation, lineage validation, and post-repair diagnostics. It must not change strategy logic, trigger thresholds, candidate scoring, risk policy, allocation, broker/live/autonomous behavior, or fabricate position state, signals, or candidates.

Required output:

`truth/reports/aegis_event_dislocation_position_state_freshness_repair_v1/<TARGET_DAY>/event_dislocation_position_state_freshness_repair.v1.json`

The artifact must show the original T04 blocker, the governed position-state source, freshness/lineage/routing status before and after repair, post-repair T01/T02 status, remaining blocker ownership, and safety gates.
