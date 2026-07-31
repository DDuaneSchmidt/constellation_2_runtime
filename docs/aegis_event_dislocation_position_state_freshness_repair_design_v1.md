# Aegis Event Dislocation Position State Freshness Repair Design v1

T05 treats T04 as prior evidence and does not rewrite market data. It reads the T04 row for `C2_EVENT_DISLOCATION_V1`, checks the governed position snapshot created from existing runtime/bootstrap evidence, and routes it only when the source already satisfies freshness and lineage rules.

The main consumer gap is that intent lifecycle state reads `positions_v1/snapshots`, while T04 created the governed compatibility state under `positions_snapshot_v2/snapshots`. T05 bridges that path without modifying the payload, then regenerates position lifecycle, intent lifecycle, T01, T02, T03, and T04 evidence so the post-repair blocker reflects current runtime truth.

Safety boundaries are explicit in the artifact: no strategy, threshold, scoring, risk policy, allocation, broker/live/autonomous, position-state fabrication, signal fabrication, or candidate fabrication changes.
