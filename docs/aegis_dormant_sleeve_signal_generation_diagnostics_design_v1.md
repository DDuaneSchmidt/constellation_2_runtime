# AEGIS Dormant Sleeve Signal Generation Diagnostics Design V1

## Design

T02 is a read-only join over T01 dormant rows and existing producer/runtime evidence. T01 remains the authority for which sleeves are in scope. T02 explains only the signal-generation stage.

## Classification Order

1. Detect lineage mismatch if signal evidence exists despite T01 zero-signal status.
2. Check producer registration, disabled state, and invocation evidence.
3. Check runtime errors.
4. Check event/calendar blockers.
5. Check market/account/position input blockers.
6. Check trigger policy and threshold evidence.
7. Classify no-intent/no-signal declarations as healthy selectivity.
8. Fall back to unsupported or unknown deterministic blocker.

## Safety

The builder writes one diagnostics artifact. It does not execute sleeve producers, request missing data, mutate thresholds, create signals, create candidates, or change downstream AEGIS readiness.
