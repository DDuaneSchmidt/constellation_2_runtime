# Object Contract

Generator V1 emits three object types only:

- `ResearchHypothesis`
- `HypothesisFalsificationPlan`
- `HypothesisGenerationRun`

`ResearchHypothesis` requires `hypothesis_id`, `created_at`, `source_claim_id`, `mechanism_id`, `mechanism_family`, `hypothesis_text`, `testable_condition`, `expected_direction`, `baseline_comparison`, `required_data`, `falsification_criteria`, `contrarian_inputs`, `confidence`, `authority_boundary_acknowledged`, and `status`.

Allowed `ResearchHypothesis` statuses are `GENERATED`, `INSUFFICIENT_DETAIL`, `DUPLICATE_HYPOTHESIS`, `UNSUPPORTED_MECHANISM`, and `REJECTED_LOW_SPECIFICITY`. `UNKNOWN` mechanisms may only become `UNSUPPORTED_MECHANISM` or `INSUFFICIENT_DETAIL`.

`HypothesisFalsificationPlan` requires `plan_id`, `hypothesis_id`, `failure_modes`, `required_tests`, `minimum_sample_requirement`, `baseline_requirement`, `falsification_threshold`, and `status`.

Allowed `HypothesisFalsificationPlan` statuses are `CREATED`, `INSUFFICIENT_DETAIL`, `REQUIRES_BASELINE`, and `REQUIRES_DATA`.

`HypothesisGenerationRun` records only run counts and optional source pointers. It never grants execution or validation authority.
