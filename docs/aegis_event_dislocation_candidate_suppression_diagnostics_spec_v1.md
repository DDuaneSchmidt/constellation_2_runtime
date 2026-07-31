# AEGIS Event Dislocation Candidate Suppression Diagnostics Spec v1

Artifact family: `aegis_event_dislocation_candidate_suppression_diagnostics_v1`

Filename: `event_dislocation_candidate_suppression_diagnostics.v1.json`

Target sleeve: `C2_EVENT_DISLOCATION_V1`

Primary inputs:

- `aegis_sleeve_throughput_diagnostics_v1`
- `aegis_event_dislocation_position_state_freshness_repair_v1`
- `aegis_signal_evidence_graph_v1`
- `aegis_candidate_generation_diagnostics_v1`
- `aegis_candidate_contracts_v1`
- `candidate_generation_manifest_v1`
- governance candidate/risk policy registries

The top-level artifact includes the requested T06 fields including raw signal ids, source paths, validation status, candidate invocation status, candidate counts, suppression/rejection counts, furthest stage, deterministic suppression code, rejected-by flags, missing fields, required inputs, owner, and `david_action_required`.

Valid post-diagnostic statuses are `FLOWING`, `VALID_NO_CANDIDATE_CONDITIONS`, `SIGNALS_PRESENT_BUT_NO_CANDIDATES`, `BLOCKED_BY_CANDIDATE_SUPPRESSION`, `BLOCKED_BY_POLICY`, `BLOCKED_BY_RUNTIME_ERROR`, and `UNKNOWN_BLOCKER`.
