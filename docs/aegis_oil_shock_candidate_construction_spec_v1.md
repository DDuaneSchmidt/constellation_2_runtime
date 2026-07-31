# Aegis Oil Shock Candidate Construction Spec v1

## Artifact

`aegis_oil_shock_candidate_construction_v1` writes:

`reports/aegis_oil_shock_candidate_construction_v1/{day}/oil_shock_candidate_construction.v1.json`

## Status Values

- `READY_FOR_MARKET_EVALUATION`: all governed construction fields are present; producer may evaluate market setup.
- `POLICY_INCOMPLETE`: governed construction fields, approval/setup state, or policy mappings are missing.
- `NO_MARKET_SETUP`: construction is ready but no market setup qualifies.

## Required Fields

The artifact emits `hypothesis_id`, `hypothesis_name`, `sleeve_id`, `generated_sleeve_id`, `signal_id`, `symbol`, `instrument_basket`, `direction`, `instrument_type`, `entry_logic`, `exit_logic`, `risk_policy_id`, `exit_policy_id`, `expected_holding_period`, `required_evidence_fields`, `governance_status`, `candidate_construction_status`, `missing_construction_fields`, `reason_codes`, `candidate_count`, `raw_signal_count`, `candidate_flow_started`, `david_action_required`, `source_artifact_paths`, and `source_artifact_hashes`.

## June 2 Expected Classification

For 2026-06-02 before lineage repair, Oil Shock had market-data coverage but lacked canonical approval-event lineage. After approval lineage repair, candidate construction consumes the paper setup bridge and may report `READY_FOR_MARKET_EVALUATION` when `approval_event` and `approval_event_hash` are present. This artifact still does not create candidates or paper observations; downstream producer and lifecycle gates remain the authorities for market setup, signal evidence, candidate contracts, and paper observations.


## Paper Setup Bridge Integration

Oil Shock candidate construction consumes `aegis_generated_hypothesis_paper_setup_bridge_v1`. When the bridge reports `PAPER_SETUP_BRIDGE_READY`, construction may use its governed `sleeve_id`, `risk_policy_id`, `exit_policy_id`, `approval_event_hash`, and paper setup status. When the bridge is blocked, construction remains fail-closed and must not create raw signals or candidates.


## Governance Bridge Integration

`aegis_generated_hypothesis_governance_bridge_v1` is the upstream authority for generated research sleeve and policy metadata. When it reports `GOVERNANCE_BRIDGE_READY`, the paper setup bridge can use its deterministic sleeve, risk policy, exit policy, candidate construction policy, validation plan, blueprint, readiness, and tracking setup IDs. When it is blocked, downstream candidate construction remains fail-closed and must not create raw signals or candidates.

## Approval Lineage Behavior

Oil Shock candidate construction consumes approval lineage indirectly through the paper setup bridge. When the bridge is `PAPER_SETUP_BRIDGE_READY`, construction must stop reporting `approval_event` and `approval_event_hash` as missing. Candidate construction may move to `READY_FOR_MARKET_EVALUATION`, but it still emits zero raw signals and zero candidates unless normal market setup and candidate-contract gates qualify.

## Signal-to-Candidate Proof

After a qualifying Oil Shock setup emits a raw signal, `aegis_generated_hypothesis_signal_to_candidate_v1` traces that signal through `aegis_signal_evidence_graph_v1` and `aegis_candidate_contracts_v1`. Candidate construction is not allowed to count a downstream contract unless the candidate-contract artifact contains a valid matching row or an explicit rejection row.
