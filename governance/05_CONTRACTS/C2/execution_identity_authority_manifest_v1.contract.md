# execution_identity_authority_manifest_v1

`execution_identity_authority_manifest_v1` governs sleeve/account/orders-client ownership for submit-capable PAPER execution.

Canonical writer:
- `ops/tools/run_subsystem_authority_v1.py`

Canonical runtime output:
- `/home/node/constellation_runtime_data/truth/reports/execution_identity_authority_manifest_v1/current.json`

Rules:
- the manifest must declare `authority_owner = execution_identity_binding_v1`
- trusted upstream authorities must include, at minimum:
  - `execution_identity_binding_v1`
  - `execution_profile_authority_v1`
  - `C2_SLEEVE_REGISTRY_V1.json`
  - `C2_IB_ACCOUNT_REGISTRY_V1.json`
- precedence must explicitly state that sleeve registry owns the submit identity tuple and IB account registry only validates account eligibility plus allowed sleeve membership
- invalidation rules must fail closed on:
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
- runtime dossiers must preserve the resolved identity tuple explicitly and must not normalize mismatches into advisory warnings
