# Aegis Generated Hypothesis Signal-to-Candidate Spec v1

## Artifact

`reports/aegis_generated_hypothesis_signal_to_candidate_v1/{day}/generated_hypothesis_signal_to_candidate.v1.json`

## Inputs

- `aegis_oil_shock_candidate_producer_v1`
- `aegis_signal_evidence_graph_v1`
- `aegis_candidate_contracts_v1`
- `aegis_entry_reference_price_certification_v1`
- `aegis_oil_shock_candidate_construction_v1`
- `aegis_generated_hypothesis_governance_bridge_v1`
- `aegis_generated_hypothesis_paper_setup_bridge_v1`
- `aegis_candidate_to_paper_lifecycle_v1`

## Oil Shock Output Fields

- `raw_signal_id`
- `raw_signal_present`
- `raw_signal_source_artifact`
- `signal_schema_status`
- `signal_direction`
- `signal_instrument_type`
- `signal_symbol_or_basket`
- `governance_status`
- `candidate_construction_policy_id`
- `risk_policy_id`
- `exit_policy_id`
- `entry_reference_price_status`
- `entry_reference_price_certification_status`
- `candidate_contract_status`
- `candidate_contract_id`
- `signal_to_candidate_status`
- `rejection_reason_codes`
- `missing_fields`
- `candidate_flow_advanced`
- `paper_observation_flow_advanced`
- `remaining_blocker`
- `source_artifact_paths`
- `source_artifact_hashes`

## Status Rules

`CANDIDATE_CONTRACT_CREATED` requires an Oil Shock raw signal in the signal evidence graph and a valid matching row in `aegis_candidate_contracts_v1`.

`SIGNAL_SCHEMA_INCOMPLETE` is emitted when a matching raw signal is present but required fields such as direction, instrument type, symbol, governance status, or risk are missing.

`STALE_CANDIDATE_CONTRACTS` is a remaining blocker when the signal evidence graph has a valid generated-hypothesis signal but candidate contracts have neither a valid nor rejected matching row.

## Safety

Candidate contracts remain authoritative. This artifact only traces and classifies existing evidence.
