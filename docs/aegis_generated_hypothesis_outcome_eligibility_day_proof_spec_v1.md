# AEGIS Generated Hypothesis Outcome Eligibility Day Proof Spec v1

## Artifact

Family: `aegis_generated_hypothesis_outcome_eligibility_day_proof_v1`

Filename: `generated_hypothesis_outcome_eligibility_day_proof.v1.json`

The artifact contains `summary` and `oil_shock` sections with the same required Package 020 proof fields.

## Readiness Classification

The proof classifies the Oil Shock generated-hypothesis paper observation in this order:

1. Missing paper position: `OUTCOME_BUILDER_LINEAGE_MISMATCH`.
2. Authoritative closed outcome row exists: `OUTCOME_CREATED`, `READY`, `OUTCOME_FLOW`, `NONE`.
3. Minimum holding period not elapsed: `HOLDING_PERIOD_NOT_ELAPSED`.
4. Missing close-rule or exit recommendation evidence: `CLOSE_RULE_MISSING`.
5. Missing certified current mark: `MARK_DATA_MISSING`.
6. Close condition exists but actionable exit price is absent: `EXIT_PRICE_MISSING`.
7. Holding period elapsed but deterministic close condition is not met: `CLOSE_CONDITION_NOT_MET`.
8. Unclassified deterministic state: `UNKNOWN_DETERMINISTIC_BLOCKER`.

## Success

Success requires an authoritative outcome row in `aegis_outcome_registry_v1` or Package 018 evidence that itself confirms `OUTCOME_CREATED`. The Package 020 artifact never creates that row.

## Fail-Closed

When no outcome row exists, the artifact keeps `outcome_status` as `OUTCOME_NOT_READY`, `outcome_row_created` as `false`, and `furthest_stage_reached` as `PAPER_OBSERVATION_FLOW` when the paper observation exists.
