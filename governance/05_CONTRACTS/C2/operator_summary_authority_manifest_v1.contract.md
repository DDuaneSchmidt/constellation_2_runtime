# operator_summary_authority_manifest_v1

`operator_summary_authority_manifest_v1` governs the operator-facing summary surface for day and execution readiness interpretation.

Canonical writer:
- `ops/tools/run_subsystem_authority_v1.py`

Canonical runtime output:
- `/home/node/constellation_runtime_data/truth/reports/operator_summary_authority_manifest_v1/current.json`

Rules:
- `session_authority_status_v1/current.json` is the canonical operator-summary owner for current runtime interpretation
- `operator_summary_dossier_v1/<DAY>/operator_summary_dossier.v1.json` is the per-day dossier surface for the same authority plane
- precedence must be explicit:
  - Session Authority owns active day and admission interpretation
  - `submit_boundary_status_v1` owns submission authorization interpretation
  - `execution_identity_dossier_v1` owns whether the resolved submit identity is governed and usable
  - `capability_state_v1`
  - `paper_policy_verdict_v1`
  - `gate_stack_verdict_v1`
  - legacy `operator_summary_v1`
    are advisory-only unless separately promoted by governance
- unresolved execution, execution-profile, execution-identity, or account-trading-policy ambiguity must block the operator summary fail closed
- precise execution identity blocker codes must also block the operator summary fail closed even when ambiguity state is `CLEAR`
- legacy diagnostic surfaces may remain materialized, but the manifest must say they are non-authoritative for operator interpretation
