# Aegis Outcome Performance Metrics Requirements v1

## Intent

Aegis must convert closed outcomes and included validation samples into deterministic performance metrics by hypothesis and sleeve. These metrics inform research quality scoring, hypothesis decisions, and research allocation recommendations without bypassing statistical sufficiency gates.

## Scope

In scope:

- Closed-outcome performance metrics.
- Included and excluded validation sample counts.
- Win/loss, realized return, holding period, and exit reason rollups.
- Read-only integration into research quality, hypothesis decision, and allocation recommendation artifacts.
- Audit source ids, source paths, source hashes, policy versions, computed timestamps, and reason codes.

Out of scope:

- Broker execution.
- Live trading.
- Trade advice.
- Real-capital allocation.
- Autonomous execution.
- Order management.
- Forced exits.
- Exit rule changes.
- Statistical sufficiency threshold changes.
- Automatic retirement from underpowered samples.

## Required Metrics

Each hypothesis and sleeve row must include:

- `closed_outcome_count`
- `included_validation_sample_count`
- `excluded_validation_sample_count`
- `win_count`
- `loss_count`
- `win_rate`
- `average_realized_return`
- `median_realized_return`
- `best_realized_return`
- `worst_realized_return`
- `average_holding_period`
- `exit_reason_distribution`
- `take_profit_count`
- `stop_loss_count`
- `time_stop_count`
- `average_return_by_exit_reason`
- `sample_sufficiency_status`
- `minimum_required_samples`
- `distance_to_sufficiency`
- `performance_confidence`
- `underpowered`

## Policy Requirements

- Zero included validation samples keep validation evidence `UNDERPOWERED` or `NOT_APPLICABLE`.
- Below-threshold included samples may influence early direction, but confidence must be `LOW` and the hypothesis remains `UNDERPOWERED`.
- Poor early performance may produce `WATCH`, `DECREASE_ATTENTION`, or `REDESIGN_REVIEW` outcomes.
- Poor early underpowered performance must not automatically produce `RETIRE_RECOMMENDED`.
- Positive early underpowered performance must not produce `READY_FOR_CAPITAL_REVIEW`.
- Sufficient poor performance may produce `RETIRE_RECOMMENDED` or `REDESIGN` when hard gates allow.
- Sufficient strong performance may allow `READY_FOR_CAPITAL_REVIEW` only when statistical sufficiency and all hard gates pass.
- Candidate-heavy sample-poor hypotheses must remain underpowered and must not be rewarded solely for candidate count.

## Safety Requirements

The metrics artifact and all consumers are research-only. They must not mutate allocations, candidates, outcomes, validation samples, safety gates, broker state, live trading state, or order state.
