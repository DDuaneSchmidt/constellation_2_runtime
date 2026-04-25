# execution_authority_manifest_v1

`execution_authority_manifest_v1` governs execution-root ownership for Phase C, authorization, and execution evidence.

Canonical writer:
- `ops/tools/run_subsystem_authority_v1.py`

Canonical runtime output:
- `/home/node/constellation_runtime_data/truth/reports/execution_authority_manifest_v1/current.json`

Rules:
- the manifest must declare `authority_owner = sleeve_execution_root_v1`
- trusted upstream authorities must include, at minimum:
  - `execution_root_authority_v1`
  - `truth_partitioning_by_sleeve_v1`
  - `multi_account_topology_v1`
  - `C2_TRUE_EVIDENCE_SPINE_V2`
  - `trade_submit_readiness_c2_v1`
  - `startup_materialization_v1`
- precedence must explicitly state that sleeve-scoped execution root overrides any older global/shared execution-root references
- invalidation rules must fail closed on:
  - `EXECUTION_ROOT_SLEEVE_ID_MISSING`
  - `EXECUTION_ROOT_MODE_MISSING`
  - `EXECUTION_ROOT_PATH_UNRESOLVED`
  - `EXECUTION_ROOT_PATH_MISMATCH`
  - `EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN`
- execution runtime dossiers must emit precise execution-root blocker codes instead of silently choosing a root or falling back to the older generic ambiguity code
- no manifest or dossier in this family may authorize broker transmit
