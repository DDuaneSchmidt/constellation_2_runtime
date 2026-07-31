# Aegis Outcome Validation Maturity Spec v1

## Artifacts

- `reports/aegis_outcome_registry_v1/<day_utc>/outcome_registry.v1.json`
- `reports/aegis_hypothesis_outcome_ledger_v1/<day_utc>/hypothesis_outcome_ledger.v1.json`
- `reports/aegis_validation_samples_v1/<day_utc>/validation_samples.v1.json`
- `reports/aegis_statistical_sufficiency_v1/<day_utc>/statistical_sufficiency.v1.json`
- `reports/aegis_outcome_validation_maturity_self_check_v1/<day_utc>/self_check.v1.json`

## Outcome Registry Fields

Each outcome row contains `outcome_id`, `thesis_id`, `hypothesis_id`, `sleeve_id`, `candidate_id`, `position_id`, `open_day_utc`, optional `close_day_utc`, `outcome_state`, `entry_mark`, `latest_mark`, optional `exit_mark`, optional `realized_return`, optional `unrealized_return`, `holding_period_days`, `source_artifacts`, `source_hashes`, `blocker_reasons`, and `generated_at`.

## Outcome Price And Timestamp Authority

Validation samples are reproducible only when the outcome row binds every return input to explicit evidence. Outcome builders must not infer prices from UI text, portfolio summaries, or non-hash-bound transient state.

- `entry_mark` is sourced from the paper position ledger entry reference price that was admitted from a candidate contract with `entry_reference_price_certification_status = CERTIFIED`. The outcome row must preserve the entry price, entry price timestamp, source artifact, and source hash from the candidate-to-paper lineage.
- `exit_mark` is sourced only from a governed exit/closure evidence artifact or deterministic exit recommendation/closure artifact that records the exit price, source artifact, source hash, and price timestamp. A review-only exit recommendation is not a realized exit unless a governed closure artifact records the resolved exit.
- `latest_mark` may be used for open-position unrealized context, but it must not become `exit_mark` or realized return evidence without a governed closure artifact.
- `trigger_timestamp` is the timestamp at which the exit condition or closure trigger was evaluated, as recorded by the exit recommendation, exit strategy analysis, or closure authority artifact.
- `outcome_timestamp` is the timestamp at which the outcome state became resolved in the outcome registry. For open positions it is the registry evaluation timestamp, not a closure timestamp.
- `realized_return` may be populated only when entry and exit marks are both evidence-bound and the outcome is resolved. Open positions must not produce realized return samples.

## Replay And Return Determinism

Outcome replay is deterministic when the same target day, source artifacts, source hashes, entry mark, exit mark, and timestamp evidence produce byte-stable outcome rows after timestamp normalization. A rerun must produce the same `realized_return` for every resolved outcome. If a rerun changes realized return for unchanged source hashes, the outcome is invalid for validation sampling and must be blocked with a reproducibility reason code instead of counted as proof.

Minimum replay evidence for a resolved validation sample:

- entry price source artifact and hash
- exit price source artifact and hash
- trigger timestamp
- outcome timestamp
- deterministic return formula/version
- source hash list covering the candidate, paper position, exit/closure, and outcome artifacts

## Statistical Defaults v1

- `minimum_required_samples`: 30
- `validation_ready_samples`: 10
- `minimum_independent_sleeves_for_strong_independence`: 2
- `max_drawdown_limit`: -0.15
- `validated_min_expectancy`: 0.0
- `disproven_max_expectancy`: -0.02

Allowed sufficiency states: `UNDERPOWERED`, `ACCUMULATING`, `VALIDATION_READY`, `VALIDATED`, `DISPROVEN`, `DEGRADED`, `BLOCKED`.

## Promotion/Disproof Rules

A hypothesis can be `VALIDATION_READY` only when usable closed/resolved samples meet the validation-ready threshold and lineage is intact. A hypothesis can be `VALIDATED` only when usable samples meet the full minimum threshold, expectancy is positive, drawdown is acceptable, and excess return is non-negative. A hypothesis can be `DISPROVEN` when usable samples meet the full minimum threshold and expectancy or excess return is materially negative.
