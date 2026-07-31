# Conversion Rules

Each accepted `CONVERTED` `HistoricalExperienceRecord` appends one linked Atlas V2 chain:

- `AttentionDecision`
- `Prediction`
- `Outcome`
- `Regret`
- `CalibrationRecord`
- `ExperienceEvent`

The `Outcome.evidence_reference` is the original `source_artifact`. The `ExperienceEvent` carries `historical_record_id`, `source_artifact`, `source_type`, and `provenance_reference`.

Allowed `HistoricalExperienceRecord.status` values:

- `CONVERTED`
- `INCOMPLETE_PROVENANCE`
- `UNSUPPORTED_SOURCE`
- `DUPLICATE_SOURCE`
- `REJECTED_LOW_QUALITY`

Non-converted statuses append only the `HistoricalExperienceRecord`; they do not create predictions, outcomes, regrets, calibrations, or experience events. Matching is strict normalized text equality. `unknown`, `outcome_unknown`, and `pending_outcome` never become failed outcomes.

The factory rejects records that contain prohibited authority fields for trading, broker execution, autonomous execution, sleeves, candidates, paper positions, capital allocation, recommendations, or validation authority. Rejection is reported without mutating the source artifact.
