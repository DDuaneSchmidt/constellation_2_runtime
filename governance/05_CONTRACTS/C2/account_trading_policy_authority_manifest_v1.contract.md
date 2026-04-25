# account_trading_policy_authority_manifest_v1

`account_trading_policy_authority_manifest_v1` governs account eligibility versus tradable-symbol policy ownership for execution readiness.

Canonical writer:
- `ops/tools/run_subsystem_authority_v1.py`

Canonical runtime output:
- `/home/node/constellation_runtime_data/truth/reports/account_trading_policy_authority_manifest_v1/current.json`

Rules:
- the manifest must declare:
  - `trading_symbol_policy_authority_v1` as the canonical symbol-policy owner when the repo proves that `ENGINE_MODEL_REGISTRY_V1.allowed_symbols` is authoritative
  - `ib_account_registry_v1` as the account-eligibility owner only
- the manifest must explicitly classify `C2_IB_ACCOUNT_REGISTRY_V1.allowed_symbols` and `C2_SLEEVE_REGISTRY_V1.symbols` as non-authoritative for tradable-symbol policy whenever the repo contract layer says so
- if contracts or submit-boundary code disagree on those ownership boundaries, the manifest and dossier must fail closed with `ACCOUNT_TRADING_POLICY_AUTHORITY_AMBIGUOUS`
