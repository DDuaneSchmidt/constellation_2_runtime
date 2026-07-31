# AEGIS Event Dislocation Governance Trace Diagnostic Spec v1

Inputs:

- `aegis_signal_evidence_graph_v1`
- `aegis_candidate_contracts_v1`
- `aegis_event_dislocation_candidate_suppression_diagnostics_v1`
- `ENGINE_MODEL_REGISTRY_V1`
- `C2_EQUITY_STRUCTURE_POLICY_V1`
- `C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1`
- `C2_RISK_POLICY_REGISTRY_V1`

Output schema highlights:

- `sleeve_id`
- `sleeve_name`
- `target_day`
- `rejected_candidate_attempt_count`
- `classification`
- `classification_counts`
- `missing_field_counts`
- `representative_rejected_candidate`
- `candidate_traces`
- `answer`
- `root_cause_source`
- `owner`
- `david_action_required`

Allowed failure classifications:

- `FIELD_MISSING_FROM_SIGNAL`
- `FIELD_PRESENT_IN_SIGNAL_NOT_MAPPED_TO_CANDIDATE`
- `FIELD_PRESENT_IN_CANDIDATE_INVALID_VALUE`
- `INSTRUMENT_NOT_IN_GOVERNED_REGISTRY`
- `INSTRUMENT_TYPE_VALUE_NOT_GOVERNED`
- `GOVERNANCE_STATUS_VALUE_NOT_GOVERNED`
- `DIRECTION_VALUE_NOT_GOVERNED`
- `CONTRACT_SCHEMA_MISMATCH`
- `BUILDER_OUTPUT_CONTRACT_INPUT_MISMATCH`
- `UNKNOWN_DETERMINISTIC_BLOCKER`
