# Aegis Outcome Performance Metrics Design v1

## Design Summary

`aegis_outcome_performance_metrics_v1` is a deterministic rollup layer between raw outcome evidence and research policy consumers. It lets consumers use outcome performance directly while preserving the runtime truth invariant: consumers query verified evidence and do not invent truth.

## Data Flow

1. Read closed outcomes from the outcome registry.
2. Read included and excluded validation samples.
3. Join evidence by `hypothesis_id`, optional `sleeve_id`, and linked `outcome_id`.
4. Read statistical sufficiency state and minimum sample requirements.
5. Emit hypothesis-level and sleeve-level metric rows.
6. Research quality consumes metric rows directly.
7. Decision policy consumes quality rows and the embedded metric snapshot.
8. Allocation recommendations consume decisions and embedded metric snapshots.

## Sufficiency Preservation

Outcome metrics are directional evidence before sufficiency, not maturity evidence. Underpowered rows may influence attention level but cannot produce capital review. Retirement remains reserved for sufficient poor evidence or explicit duplicate/invalid policy.

## Auditability

Each metric row carries source ids, source artifact paths, source artifact hashes, computed timestamp, scoring version, performance policy version, and reason codes. This allows downstream artifacts and audits to prove whether decisions used closed outcomes and validation samples.

## Safety Model

The implementation is read-only. It writes only the metrics artifact. It does not mutate allocation, candidates, outcomes, validation samples, broker state, live trading state, order state, exit rules, statistical thresholds, or safety gates.
