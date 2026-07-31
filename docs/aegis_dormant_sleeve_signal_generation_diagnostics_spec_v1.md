# AEGIS Dormant Sleeve Signal Generation Diagnostics Spec V1

## Inputs

- T01 sleeve throughput diagnostics
- Candidate generation diagnostics
- Sleeve evaluation kernel rollup
- Sleeve input contracts
- Scheduled run reconciliation when present
- Macro calendar data readiness when event/calendar evidence is referenced

## Output Rows

Each row includes sleeve identity, producer presence, registration and invocation status, raw signal count, expected signal conditions, observed market conditions, market data status, trigger evaluation status, threshold evidence, event/calendar dependency status, missing inputs, furthest stage, dormant reason code, owner, and David action requirement.

## Determinism

The classifier uses only persisted truth artifacts. It emits `UNKNOWN_DETERMINISTIC_BLOCKER` when available evidence is insufficient for a more specific category.

## T01 Linkage

T01 rows with `DORMANT` and `NO_SIGNALS_GENERATED` include a downstream diagnostics link to this artifact. The link does not alter T01 throughput status, counts, ranking, or blocker classification.
