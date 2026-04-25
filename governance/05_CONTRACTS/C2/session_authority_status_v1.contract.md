# session_authority_status_v1

`session_authority_status_v1` is the canonical current operator-summary surface for the Session Authority control plane.

Canonical output:
- `/home/node/constellation_runtime_data/truth/session_authority_status_v1/current.json`

Canonical writer:
- `ops/tools/run_session_authority_status_v1.py`

Rules:
- this surface is authoritative for operator-facing interpretation only
- it remains derived from governed upstream authorities and must not override Session Authority itself
- it must read canonical runtime truth only
- it must derive from `active_session_v1/current.json` plus the referenced `target_day_admission_v1` and `target_day_build_v1` artifacts
- in `PAPER` mode, it must treat `reports/paper_session_authority_v1/<DAY>/paper_session_authority.v1.json` as a constitutional upstream dependency for operator-facing open authority projection
- it must incorporate `market_calendar_coverage_status_v1/current.json` when present and may derive a read-only coverage view when the status artifact is absent
- it must not recompute day readiness independently of Session Authority
- it must bind the operator summary authority model declared by:
  - `operator_summary_authority_manifest_v1`
  - `operator_summary_dossier_v1`
- it must expose:
  - top-level `semantic_status` using `governance/05_CONTRACTS/C2/operator_semantic_taxonomy_v1.contract.md`
  - upstream intent-input convergence state
  - upstream startup-materialization-input convergence state
  - readiness bootstrap interpreter/import failure state when present in target-day build evidence
  - active day
  - next target day
  - blocked target day
  - rollover status and rollover reason code
  - target-day admission status
  - target-day build status
  - closure status
  - hidden dependency check result
  - market-calendar coverage status
  - market-calendar coverage severity
  - market-calendar required target day
  - market-calendar warning target day
  - market-calendar source coverage status
  - market-calendar runtime coverage status
  - whether source covers the required target day
  - whether runtime covers the required target day
  - market-calendar operator action code
  - submission authorization status
  - first real blocker code and summary
  - advisory-only legacy readiness signals
  - subsystem ambiguity state
  - operator summary authority manifest ref
  - operator summary dossier ref
  - traceability status
  - top blocker reason codes
  - semantic status
  - monitoring severity
  - required operator action
- it must verify that `active_session_v1 -> target_day_admission_v1 -> target_day_build_v1` is readable and internally consistent
- it must fail closed when subsystem authority dossiers report unresolved execution, execution-profile, execution-identity, or account-policy ambiguity
- it must also fail closed when the execution dossier is blocked by a precise execution-root owner failure, including:
  - `EXECUTION_ROOT_SLEEVE_ID_MISSING`
  - `EXECUTION_ROOT_MODE_MISSING`
  - `EXECUTION_ROOT_PATH_UNRESOLVED`
  - `EXECUTION_ROOT_PATH_MISMATCH`
  - `EXECUTION_ROOT_GLOBAL_REFERENCE_FORBIDDEN`
- it must also fail closed when the execution identity dossier is blocked by a precise submit-identity failure, including:
  - `EXECUTION_IDENTITY_SLEEVE_MISSING`
  - `EXECUTION_IDENTITY_ENVIRONMENT_MISSING`
  - `EXECUTION_IDENTITY_SLEEVE_UNREGISTERED`
  - `EXECUTION_IDENTITY_ACCOUNT_MISSING`
  - `EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISSING`
  - `EXECUTION_IDENTITY_ACCOUNT_AMBIGUOUS`
  - `EXECUTION_IDENTITY_CLIENT_ID_ORDERS_AMBIGUOUS`
  - `EXECUTION_IDENTITY_ACCOUNT_MISMATCH`
  - `EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH`
  - `EXECUTION_IDENTITY_FORBIDDEN_COMBINATION`
- legacy `operator_summary_v1`, `capability_state_v1`, `paper_policy_verdict_v1`, and `gate_stack_verdict_v1` may remain visible only as advisory or legacy diagnostic context
- missing or invalid Session Authority artifacts must fail closed with stable blocker reason codes
- severity mapping is governed as:
  - `INFO` for admitted healthy state
  - `WARNING` for withheld rollover or blocked admission with intact traceability when no subsystem authority ambiguity exists
  - `CRITICAL` for missing control-plane artifacts, malformed traceability, wrong authority path, schema failure, hidden dependency failure, blocked market-calendar coverage for the required target day, or subsystem authority ambiguity
- `semantic_status` and severity are separate:
  - `semantic_status` classifies operator-facing state using the shared taxonomy
  - `status_severity` continues to express escalation level without replacing semantic interpretation
- stable blocker reason codes are governed by `governance/02_REGISTRIES/C2_SESSION_AUTHORITY_BLOCKER_REASON_REGISTRY_V1.json`
