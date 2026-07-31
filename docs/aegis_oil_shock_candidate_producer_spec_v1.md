# Aegis Oil Shock Candidate Producer Spec v1

## Producer ID
`C2_OIL_SHOCK_REVERSAL_V1`

## Command
`npm run aegis:oil-shock-candidate-producer`

## Outputs
- `reports/aegis_oil_shock_candidate_producer_v1/<TARGET_DAY>/oil_shock_candidate_producer.v1.json`
- `reports/sleeve_evaluation_kernel_v1/<TARGET_DAY>/C2_OIL_SHOCK_REVERSAL_V1/sleeve_evaluation.v1.json`
- Optional exposure-intent JSON under the existing paper intent truth root when status is `VALID_CANDIDATE_SIGNAL`.

## Evaluation Rules
1. Load the approved Oil Shock evidence packet and paper blueprint.
2. Confirm required evidence fields are present.
3. Confirm paper tracking setup and readiness certification are present and complete.
4. Confirm candidate construction policy requires valid candidate contracts and entry-price certification.
5. Load the original proposal event definition.
6. Require proposal event definitions for `daily_return_above_threshold` and/or `daily_range_percentile_above`.
7. Require OHLCV rows for approved universe symbols from the sleeve market data manifest.
8. Evaluate source symbols in the approved universe. `USO` and `XLE` are preferred event-source symbols when available.
9. `daily_return_above_threshold`: absolute close-to-close return must be greater than or equal to proposal threshold.
10. `daily_range_percentile_above`: current daily range percentage must be greater than or equal to the historical percentile threshold.
11. If no source symbol qualifies, status is `NO_MARKET_SETUP`.
12. If a source symbol qualifies, emit one deterministic long-equity research raw signal for that symbol using the existing exposure-intent schema.

## Candidate Path
Oil Shock Producer -> raw signal -> signal evidence graph -> candidate contract -> paper construction -> auto-promotion to paper tracking -> outcome registry -> validation sample eligibility.

## Non-Authority
The producer is not authoritative for candidate validity, paper observations, outcomes, validation samples, or allocation. Candidate contracts and downstream lifecycle artifacts remain authoritative.
