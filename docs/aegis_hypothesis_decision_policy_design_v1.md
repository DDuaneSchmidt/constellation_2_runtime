# Aegis Hypothesis Decision Policy Design v1

## Design

The policy consumes only `aegis_research_quality_engine_v1`. It does not inspect raw evidence except through the quality artifact, preserving the consumer-does-not-invent-truth invariant.

Rows are evaluated in deterministic order by `hypothesis_id`. Hard gates are checked before soft grades. The first matching rule writes the recommendation, reason codes, confidence level, and David-review requirement.

## READY_FOR_CAPITAL_REVIEW Guard

`READY_FOR_CAPITAL_REVIEW` requires all of the following:

- No active hard gates.
- `validation_evidence` grade is `PASS`.
- `robustness` grade is `PASS`.
- Statistical sample sufficiency reason is present from backend evidence.

This is only a research control-point recommendation and not approval to deploy real capital.

## Safety Boundary

The policy emits research recommendations. It never mutates allocation, sends orders, enables trade advice, changes broker state, or performs real-world position management.
