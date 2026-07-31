# Estimator Metrics

EstimatorPerformanceReport records summarize estimator calibration across a period.

Required metrics:

- estimate_count: appended LearningEstimate count.
- evaluated_count: appended LearningEstimateEvaluation count.
- mean_learning_prediction_error: mean actual minus expected learning.
- mean_importance_weighted_error: mean importance-weighted prediction error.
- correlation_expected_actual: optional Pearson correlation when enough non-constant evaluated observations exist.
- behavior_change_rate: share of evaluations with observed behavior change.
- regret_reduction_signal: bounded signal for whether learning errors and regret indicate better attention targeting.

The report is a measurement artifact. It may guide future attention review, but it cannot validate an experiment or authorize downstream market actions.

## Run Identity And Duplicate Guard

Every estimator output record carries `estimator_run_id`. EstimatorPerformanceReport also records `source_ledger_root`, `source_record_counts`, and `input_fingerprint`. The fingerprint is computed from the source/input Atlas V2 records read by the estimator, not from prior estimator outputs.

Prior behavior was purely append-only: rerunning the estimator against the same ledger root appended a complete duplicate set of LearningEstimate, LearningEstimateEvaluation, AttentionSignal, and EstimatorPerformanceReport records. That history remains auditable, but new runs use duplicate detection to avoid accidental double-counting.

If the same `source_ledger_root`, `input_fingerprint`, and `estimator_version` already exists, the default run skips appending new estimator records. A forced run may append a new audit copy, but the new records include `duplicate_of_run_id` so analysis can separate intentional duplicate runs from primary estimator passes.
