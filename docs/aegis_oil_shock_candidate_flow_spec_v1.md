# Aegis Oil Shock Candidate Flow Spec v1

## Artifact Contract
The day-scoped artifact path is `reports/aegis_oil_shock_candidate_flow_v1/{day}/oil_shock_candidate_flow.v1.json`.

Top-level fields include schema id/version, day, policy version, input artifact hashes, deterministic rerun id, source artifact paths, computed time, content hash, safety flags, and an `oil_shock` object.

The `oil_shock` object includes:
- `hypothesis_id`
- `hypothesis_name`
- `current_state`
- `paper_setup_status`
- `instrument_universe`
- `required_evidence_fields`
- `available_evidence_fields`
- `missing_evidence_fields`
- `candidate_generation_rules`
- `candidate_construction_readiness`
- `entry_price_certification_path`
- `exit_policy_path`
- `candidate_flow_status`
- `candidate_flow_started`
- `candidate_count`
- `paper_observation_count`
- `reason_no_candidates_generated_yet`
- `reason_codes`
- `blocker_classification`
- `next_expected_step`
- `david_action_required`
- `ai_root_cause_advisory`

## Deterministic Classification
Missing data produces `MISSING_DATA`. Missing candidate artifacts or producer evidence produces `PRODUCER_MISSING`. Missing construction requirements produces `CANDIDATE_CONSTRUCTION_INCOMPLETE`. Missing entry/exit policy evidence produces `POLICY_INCOMPLETE`. Complete policy and data with zero qualifying signals produces `NO_MARKET_SETUP` and `WAIT_FOR_MARKET_CONDITIONS`. Contradictory artifacts produce `IMPLEMENTATION_DEFECT`.

## Safety
The artifact is read-only. It must not create candidates, candidate contracts, paper observations, validation samples, trades, orders, allocation changes, or retirements.
