# Aegis Oil Shock Candidate Flow Enablement Spec v1

## Command
`TARGET_DAY=<day> npm run aegis:oil-shock-candidate-flow-enablement`

## Artifact
`reports/aegis_oil_shock_candidate_flow_enablement_v1/<day>/oil_shock_candidate_flow_enablement.v1.json`

## Inputs
- `aegis_oil_shock_candidate_producer_v1`
- `aegis_oil_shock_candidate_flow_v1`
- `aegis_candidate_generation_diagnostics_v1`
- `aegis_candidate_contracts_v1`
- `aegis_candidate_to_paper_lifecycle_v1`
- `aegis_entry_reference_price_certification_v1`
- `aegis_outcome_registry_v1`
- `aegis_validation_samples_v1`
- Oil Shock producer tool registration

## Output Contract
The top-level object and `oil_shock` row include:

- `hypothesis_id`
- `hypothesis_name`
- `producer_status`
- `producer_registered`
- `producer_run_status`
- `candidate_flow_status`
- `candidate_count`
- `raw_signal_count`
- `required_evidence_fields`
- `available_evidence_fields`
- `missing_evidence_fields`
- `blocker_code`
- `blocker_owner`
- `david_action_required`
- `next_expected_step`
- `source_artifact_paths`
- `source_artifact_hashes`
- `computed_at_utc`

Valid setup emits raw signal evidence only through the existing producer. Candidate contracts, entry-price certification, paper lifecycle, outcome registry, and validation sample eligibility remain authoritative downstream gates.
