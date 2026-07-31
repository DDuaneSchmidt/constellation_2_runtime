# Forbidden Boundaries

Historical Experience Factory V1 is read-only and audit-only with respect to source artifacts and authority surfaces.

It must not create or mutate:

- discovery output
- hypotheses
- YouTube ingestion artifacts
- trading authority
- broker execution authority
- autonomous execution authority
- sleeves
- candidates
- paper positions
- capital allocations
- recommendations
- validation authority
- allocation surfaces

`HistoricalExperienceRecord`, `AttentionDecision`, `Prediction`, `Outcome`, `Regret`, `CalibrationRecord`, and `ExperienceEvent` are learning records only. A converted historical `ExperienceEvent` is not a candidate, not a sleeve, not a trade, not a recommendation, and not validation authority.

`INCOMPLETE_PROVENANCE`, `UNSUPPORTED_SOURCE`, `DUPLICATE_SOURCE`, and `REJECTED_LOW_QUALITY` records are terminal audit records for that conversion attempt unless a future append-only run supplies a new source record with complete provenance.
