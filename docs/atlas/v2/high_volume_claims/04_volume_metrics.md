# Volume Metrics

The pipeline reports:

- `total_claims`
- `unique_claims`
- `unique_mechanisms`
- `duplicate_ratio`
- `mechanism_distribution`
- `cheap_experiment_candidates`

Volume metrics:

- Claims ingested: `claims_ingested`
- Claims deduped: `claims_deduped`
- Mechanisms discovered: `mechanisms_discovered`
- Mechanisms reused: `mechanisms_reused`
- Contrarian coverage: `contrarian_coverage`
- Cheap experiment coverage: `cheap_experiment_coverage`

Cheap experiment candidates are bounded by unique mechanisms and the configured route cap. A 10,000-claim batch must not produce 10,000 independent experiments.
