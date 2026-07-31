# Aegis Research Capital Allocation Spec v1

## Artifacts

- `reports/aegis_research_program_registry_v1/<day_utc>/research_program_registry.v1.json`
- `reports/aegis_research_capital_scoring_v1/<day_utc>/research_capital_scoring.v1.json`
- `reports/aegis_research_allocation_decisions_v1/<day_utc>/research_allocation_decisions.v1.json`
- `reports/aegis_research_allocation_event_log_v1/<day_utc>/research_allocation_event_log.v1.json`
- `reports/aegis_research_capital_allocation_self_check_v1/<day_utc>/self_check.v1.json`

## Scoring Model

Model version: `RESEARCH_CAPITAL_ALLOCATION_MODEL_V1`.

Components: candidate yield, validation progress, evidence quality, expected value signal, time-to-decision, diversification value, resource efficiency, staleness/dormancy penalty, duplication penalty, risk/drawdown penalty.

If closed outcomes are missing, expected value signal must be `INSUFFICIENT_OUTCOMES` and may not contribute positive proof.

## Recommendation Rules

Allowed recommendations: `INCREASE`, `MAINTAIN`, `REDUCE`, `PAUSE`, `RETIRE`, `INVESTIGATE_MORE`.

Rules are deterministic. `INCREASE` requires candidate yield, validation progress or near-term validation path, strong evidence quality, no critical blockers, not retired/disproven, not data blocked, and no outcome overclaim. `RETIRE` requires disproven/retired state or retirement threshold evidence. `PAUSE` is used for data blockers or non-actionable evidence. `INVESTIGATE_MORE` is used for promising but underpowered/under-specified programs.
