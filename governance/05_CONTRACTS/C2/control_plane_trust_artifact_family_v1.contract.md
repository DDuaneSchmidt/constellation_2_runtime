# control_plane_trust_artifact_family_v1

This contract governs the durable trust-plane read-model artifacts for the certified control plane.

Artifact families:
- `control_plane_operator_status_v1`
- `control_plane_blocked_transition_view_v1`
- `advisory_truth_binding_status_v1`
- `transition_timeline_projection_v1`

Authority class:
- all Bundle 6 trust-plane artifacts are governed derived projections
- artifact-class registration MUST use `read_model`
- trust-plane artifacts MUST NOT claim authority beyond their projection contracts

Required fields for every trust-plane artifact:
- `authority_label`
- `projection_version`
- `generated_at_utc`
- governing stage refs where applicable
- governing transition refs where applicable
- governing certification refs where applicable
- `freshness_state`
- evidence refs
- deterministic human-facing fields only

Freshness law:
- projections MUST label one of:
  - `fresh`
  - `stale`
  - `superseded`
  - `uncertified`
  - `historical_only`
  - `unknown`

No-invention law:
- if required governing evidence is missing, the projection MUST emit explicit degraded or blocked state, or fail closed
- projections MUST NOT invent causal meaning, currentness, or certification not proven by certified refs

