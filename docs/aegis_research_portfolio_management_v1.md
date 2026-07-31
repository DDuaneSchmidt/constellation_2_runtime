# Aegis Research Portfolio Management v1

## Daily Workflow

1. Build thesis registry.
2. Build hypothesis registry with sleeve, candidate, position, outcome, sample, and validation links.
3. Apply deterministic state machines.
4. Score research allocation.
5. Emit research portfolio artifact.
6. Run self-check and fail visibly on orphaned or conflicting mappings.

## Portfolio Questions

The artifact answers:
- Which theses exist?
- Which hypotheses exist?
- Which hypotheses are gathering evidence?
- Which hypotheses are validation-ready?
- Which hypotheses are validated or disproven?
- Which sleeves map to which hypotheses?
- Which sleeves implement the same thesis?
- Where should research effort go next?
- Which hypotheses are underpowered, stalled, degraded, or over-resourced?

## Decision Semantics

Recommendations are research allocation recommendations only. They are not trade advice, broker instructions, or portfolio allocation orders.

## Source of Truth

The portfolio manager consumes existing runtime truth artifacts and emits canonical read models. Consumers must read the generated artifacts instead of inventing hypothesis truth.
