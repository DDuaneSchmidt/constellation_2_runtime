# Aegis Outcome Performance Metrics Spec v1

## Artifact Contract

Artifact id: `aegis_outcome_performance_metrics_v1`

Canonical path:

`reports/aegis_outcome_performance_metrics_v1/{day}/outcome_performance_metrics.v1.json`

Producer command:

`TARGET_DAY={day} npm run aegis:outcome-performance-metrics`

## Inputs

- `aegis_outcome_registry_v1`
- `aegis_validation_samples_v1`
- `aegis_statistical_sufficiency_v1`
- `aegis_research_portfolio_v1`
- `aegis_hypothesis_registry_v1`

## Top-Level Fields

- `schema_id`: `aegis_outcome_performance_metrics`
- `schema_version`: `v1`
- `artifact_id`: `aegis_outcome_performance_metrics_v1`
- `day_utc`
- `scoring_version`
- `performance_policy_version`
- `input_artifact_hashes`
- `input_generated_at_utc`
- `computed_at_utc`
- `deterministic_rerun_id`
- `source_artifact_paths`
- `hypotheses`
- `sleeves`
- `summary`
- `content_hash`
- research-only safety flags

## Metric Row

Each hypothesis and sleeve row must include the required metrics from the requirements document plus:

- `hypothesis_id`
- `sleeve_id`
- `source_outcome_ids`
- `source_validation_sample_ids`
- `source_artifact_paths`
- `source_artifact_hashes`
- `computed_at_utc`
- `scoring_version`
- `performance_policy_version`
- `reason_codes`

## Calculation Rules

- Closed outcomes are outcomes whose state or lifecycle status indicates a terminal closed result.
- Included validation samples are samples with included/included-like inclusion state.
- Excluded validation samples are samples with excluded/rejected/invalid inclusion state.
- Win/loss is derived from explicit outcome state when available, otherwise from realized return sign.
- Realized return aggregates use numeric realized return fields from outcomes, falling back to validation sample return fields when needed.
- Exit reason distributions use exit reason/trigger fields and normalize missing values to `UNKNOWN_EXIT_REASON`.
- Sufficiency status is read from `aegis_statistical_sufficiency_v1`; the metrics artifact does not change thresholds.
- `performance_confidence` is `LOW` while sample count is below sufficiency, `MEDIUM` at sufficiency, and `HIGH` only when sample count materially exceeds the minimum.

## Safety

The artifact is read-only research evidence. It must not contain order instructions, execution instructions, trade advice, live trading enablement, trade sizing, real-capital allocation, or safety-gate mutations.
