# Aegis Data Action Routing Spec v1

## Artifact

`reports/aegis_data_action_routing_v1/<day_utc>/data_action_routing.v1.json`

## Required Output Fields

Each `routing_rows[]` item includes:

- `hypothesis_id`
- `hypothesis_name`
- `blocker_code`
- `data_action_classification`
- `david_action_required`
- `missing_data_description`
- `required_fields`
- `owner`
- `next_step`
- `source_artifact_paths`
- `source_artifact_hashes`
- `computed_at_utc`

## Ownership Semantics

- `DAVID`: operator-provided data source is required.
- `AEGIS_SYSTEM`: system implementation or data pipeline work is required.
- `MARKET_CONDITIONS`: system market evidence is not yet available.
- `UNAVAILABLE`: the required data is marked unavailable.

## Source Artifacts

Primary inputs:

- `aegis_operator_action_queue_v1`
- `aegis_generated_hypothesis_throughput_v1`
- `aegis_oil_shock_candidate_flow_v1`

The artifact is a classifier over existing truth. It does not modify source artifacts.
