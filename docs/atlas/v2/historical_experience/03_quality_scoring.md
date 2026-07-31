# Quality Scoring

`experience_quality_score` is a learning suitability score between 0 and 1. It is not investment validity and grants no validation authority.

The score considers:

- outcome clarity
- expectation clarity
- provenance strength
- regret informativeness
- calibration usefulness
- future decision relevance

Factory reports group historical record quality into `0.0-0.25`, `0.25-0.5`, `0.5-0.75`, and `0.75-1.0` buckets. Provenance coverage is the share of created `ExperienceEvent` records with `historical_record_id`, `source_artifact`, and `provenance_reference`.

Rows below the conversion threshold are preserved as `REJECTED_LOW_QUALITY` and do not emit downstream learning objects.
