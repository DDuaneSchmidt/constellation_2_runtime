# Aegis Hypothesis Decision Policy Requirements v1

## Purpose

The Hypothesis Decision Policy converts research quality into deterministic research recommendations. It is advisory for research attention only and cannot authorize trading, broker execution, live trading, or real-capital use.

## Required Output

Artifact: `aegis_hypothesis_decision_policy_v1`.

Allowed recommendation values:

- `CONTINUE`
- `INCREASE_ATTENTION`
- `DECREASE_ATTENTION`
- `REDESIGN`
- `NEEDS_DATA`
- `RETIRE_RECOMMENDED`
- `READY_FOR_CAPITAL_REVIEW`

## Required Rules

- `NEEDS_DATA` if data quality is `BLOCKED`.
- `REDESIGN` if implementation completeness is `FAIL` or paper path is missing.
- `CONTINUE` if underpowered but producing samples.
- `INCREASE_ATTENTION` if sample production is strong and validation evidence is improving.
- `DECREASE_ATTENTION` if sample production is weak but not broken.
- `RETIRE_RECOMMENDED` only when sufficient evidence exists or hypothesis is duplicate/invalid.
- `READY_FOR_CAPITAL_REVIEW` only when statistical sufficiency and validation quality pass.
- Never emit `READY_FOR_CAPITAL_REVIEW` while any hard gate is active.

## Auditability

Each recommendation must include reason codes, active hard gates, source artifact paths, input hashes, policy version, deterministic rerun id, confidence level, and safety flags.
