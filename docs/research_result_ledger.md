# Research Result Ledger

`research_result_ledger.v1` preserves the outcome of every Research Lab task.

It records result rows with:

- `result_id`
- `hypothesis_id`
- `task_id`
- `result_status`
- `evidence_refs`
- `conclusion_summary`
- `confidence_before`
- `confidence_after`
- `confidence_change`
- `metrics_summary`
- `failure_mode_notes`
- `invalidation_notes`
- `next_recommended_task`
- `promotion_recommendation`
- `created_at_utc`
- `data_snapshot_refs`
- `methodology_version`
- `code_version`
- `artifact_lineage`
- `reason_codes`
- `reproducibility_notes`

Supported result statuses are:

- `INSUFFICIENT_DATA`
- `SUPPORTS_HYPOTHESIS`
- `WEAK_SUPPORT`
- `CONTRADICTS_HYPOTHESIS`
- `INVALIDATED`
- `NEEDS_MORE_RESEARCH`

Supported promotion recommendations are:

- `NONE`
- `CONTINUE_RESEARCH`
- `PROMOTION_CANDIDATE`
- `REJECT`
- `ARCHIVE`

Failed and invalidated hypotheses must be preserved. They are durable knowledge, not cleanup targets. An invalidated or contradicted result cannot be used to justify promotion.

The ledger is evidence bookkeeping only. It does not authorize trades, mutate Lite runtime, submit broker orders, or promote sleeves.

Metric conventions for new entries:

- `expectancy`: string decimal or descriptive status.
- `sample_size`: integer count.
- `hit_rate`: string decimal or percentage.
- `drawdown`: string decimal, percentage, or descriptive status.
- `MAE`: string decimal, percentage, or descriptive status.
- `MFE`: string decimal, percentage, or descriptive status.
- `regime_expectancy`: object keyed by regime.
- `forward_return_windows`: object keyed by holding window.
- `false_positive_rate`: string decimal or percentage.
- `edge_decay`: string decimal or descriptive status.
- `confidence_change`: explicit bounded step such as `+1`, `0`, or `-1`.

The transition engine uses result status, evidence refs, result lineage, and confidence fields to update `research_hypothesis.v1` deterministically. Result entries are immutable evidence memory; corrections should be represented by a later result or conclusion, not by deleting failed outcomes.
