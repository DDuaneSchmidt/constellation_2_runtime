# control_plane_trust_projection_v1

This contract governs the Bundle 6 certified trust plane for control-plane operator, advisory, and reporting surfaces.

Core law:
- `constellation_2.common.control_plane_trust_projection_kernel_v1` is the only legal semantic source for trust-plane projections over the certified control plane.
- dashboards, CLIs, reports, advisory views, and explanation renderers MUST NOT recompute trust-plane semantics outside that kernel.
- trust-plane surfaces are projections only; they MUST NOT mutate control-plane truth.

Allowed inputs:
- admitted stage artifacts:
  - `control_stage_day_admitted_v1`
  - `control_stage_context_admitted_v1`
  - `control_stage_session_admitted_v1`
  - `control_stage_execution_build_admitted_v1`
- stage certification artifacts:
  - `control_stage_day_certification_v1`
  - `control_stage_context_certification_v1`
  - `control_stage_session_certification_v1`
  - `control_stage_execution_build_certification_v1`
- `startup_chain_certification_v1`
- `control_stage_transition_record_v1`
- explicitly ratified current projections only when named by governance

Forbidden inputs:
- direct reasoning over raw runtime files outside the certified path
- derived or advisory surfaces as upstream truth
- UI- or CLI-local semantic recomputation
- direct truth-root resolution in operator or advisory surfaces

Required projection outputs:
- current operator status
- blocked transition view
- transition timeline view
- advisory truth-binding status
- recovery and supersession lineage view

Determinism law:
- the same certified truth basis MUST yield the same projection payload except for ratified volatile timestamps
- trust-plane projections MUST preserve exact governing refs, freshness state, authority labels, taxonomy codes, and projection version metadata

Observability law:
- trust-plane projections MUST emit structured semantic events derived from certified transition truth only
- `transition_start` in trust-plane observability means the transition request recorded by `control_stage_transition_record_v1`; Bundle 6 does not invent a separate unrecorded runtime-start event

