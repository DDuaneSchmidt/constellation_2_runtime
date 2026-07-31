# Aegis Research Allocation Recommendation Requirements v1

## Purpose

The Research Allocation Recommendation artifact converts hypothesis decisions into research-attention recommendations. It never mutates actual allocation weights automatically.

## Required Output

Artifact: `aegis_research_allocation_recommendation_v1`.

For each hypothesis, output:

- `current_allocation_weight`
- `recommended_allocation_action`
- `recommended_weight_delta`
- `reason_codes`
- `confidence_level`
- `requires_david_review`

Allowed allocation actions:

- `HOLD`
- `INCREASE`
- `DECREASE`
- `PAUSE`
- `RETIRE_REVIEW`
- `CAPITAL_REVIEW`

## Required Behavior

- `NEEDS_DATA` maps to `PAUSE`.
- `REDESIGN` maps to `PAUSE`.
- `RETIRE_RECOMMENDED` maps to `RETIRE_REVIEW`.
- `READY_FOR_CAPITAL_REVIEW` maps to `CAPITAL_REVIEW` only when no hard gates are active.
- `INCREASE_ATTENTION` maps to `INCREASE`.
- `DECREASE_ATTENTION` maps to `DECREASE`.
- `CONTINUE` maps to `HOLD`.

## Non-Mutation

The artifact must report `current_allocation_weight` and `recommended_weight_delta` but must not write a new portfolio allocation, broker instruction, or real-capital instruction.
