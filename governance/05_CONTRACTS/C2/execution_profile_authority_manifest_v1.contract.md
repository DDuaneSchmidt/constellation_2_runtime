# execution_profile_authority_manifest_v1

`execution_profile_authority_manifest_v1` governs IB gateway host, port, and client-id ownership for runtime execution.

Canonical writer:
- `ops/tools/run_subsystem_authority_v1.py`

Canonical runtime output:
- `/home/node/constellation_runtime_data/truth/reports/execution_profile_authority_manifest_v1/current.json`

Rules:
- trusted upstream authorities must include, at minimum:
  - `sleeve_registry_v1`
  - `multi_account_topology_v1`
  - current orchestrator defaults in `ops/tools/run_c2_paper_day_orchestrator_v2.py`
  - current operator examples in `paper_day_readiness_runbook_v1`
- if the sleeve registry and runtime defaults disagree on the active governed profile, the manifest must fail closed with `EXECUTION_PROFILE_AUTHORITY_AMBIGUOUS`
- runtime dossiers must preserve both sides of the contradiction explicitly; they must not normalize the conflict into one guessed host/port/client-id profile
