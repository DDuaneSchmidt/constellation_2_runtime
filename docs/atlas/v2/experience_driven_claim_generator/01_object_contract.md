# Object Contract

`GeneratedResearchClaim` requires `generated_claim_id`, `created_at`, `source_event_ids`, `source_mechanism_ids`, `source_type`, `mechanism_family`, `claim_text`, `rationale`, `novelty_basis`, `expected_learning_value`, `uncertainty_score`, `contrarian_prompt`, `intended_testability`, `authority_boundary_acknowledged`, and `status`.

Allowed source types are `HISTORICAL_FAILURE`, `HISTORICAL_CONTRADICTION`, `REGRET_SIGNAL`, `CALIBRATION_ERROR`, `MECHANISM_GAP`, `CONTRARIAN_GAP`, and `PRIOR_EXPERIENCE_EVENT`.

`ClaimGenerationRun` records `max_claims`, generated count, duplicate count, insufficient-basis count, mechanism distribution, and status. Default `max_claims` is 10 and the hard cap is 100.
