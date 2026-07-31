# Aegis Generated Hypothesis Candidate-to-Paper Spec v1

## Artifact

Family: `aegis_generated_hypothesis_candidate_to_paper_v1`

Path: `reports/aegis_generated_hypothesis_candidate_to_paper_v1/{day}/generated_hypothesis_candidate_to_paper.v1.json`

## Oil Shock Output Fields

The `oil_shock` row includes hypothesis, sleeve, raw signal, candidate id, candidate-contract status, entry certification status and price, paper construction status, auto-promotion status, paper position id, paper observation flag, outcome-row flag, validation-sample status, current stop stage, current stop reason, source paths, source hashes, and safety flags.

## Allowed Statuses

The proof emits the required lifecycle status vocabulary: `CANDIDATE_CONTRACT_VALID`, `CANDIDATE_CONTRACT_REJECTED`, `ENTRY_REFERENCE_CERTIFIED`, `ENTRY_REFERENCE_FAILED`, `PAPER_CONSTRUCTION_READY`, `PAPER_CONSTRUCTION_FAILED`, `AUTO_PROMOTED_TO_PAPER_TRACKING`, `AUTO_PROMOTION_BLOCKED`, `PAPER_OBSERVATION_CREATED`, `VALIDATION_SAMPLE_PENDING`, and `BLOCKED`.

`OUTCOME_ROW_CREATED` is represented by `outcome_row_created: true` and `outcome_id`.

## Consumer Rule

Generated-hypothesis validation proof may consume this artifact for UI-facing progress fields, but candidate contracts, paper construction, lifecycle, ledger, and outcome registry remain the authoritative lifecycle sources.
