# subsystem_authority_manifest_model_v1

`subsystem_authority_manifest_model_v1` defines the shared manifest shape for governed subsystem authorities.

Canonical writer:
- `ops/tools/run_subsystem_authority_v1.py`

Canonical runtime schema:
- `governance/04_DATA/SCHEMAS/C2/REPORTS/subsystem_authority_manifest.v1.schema.json`

Rules:
- each subsystem manifest must declare exactly:
  - `subsystem_id`
  - `authority_owner`
  - `canonical_artifacts[]`
  - `trusted_upstream_authorities[]`
  - `precedence_rules[]`
  - `invalidation_rules[]`
  - `degraded_states[]`
  - `fail_closed_on_ambiguity`
  - `operator_summary_ref`
- when one canonical owner cannot be proven from governed contracts, the manifest must not guess; it must emit the explicit sentinel owner `AMBIGUOUS_UNPROVEN_CANONICAL_OWNER`
- runtime dossiers must fail closed whenever the manifest declares ambiguity for an execution-affecting concern
- legacy or diagnostic surfaces may remain materialized, but manifests must classify them explicitly and must not let them silently override canonical owners
