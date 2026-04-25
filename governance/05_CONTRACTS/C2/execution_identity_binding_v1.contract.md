# execution_identity_binding_v1

`execution_identity_binding_v1` governs the submit-capable execution identity invariant for PAPER execution.

Canonical owner:
- `execution_identity_binding_v1`

Required identity tuple:
- `sleeve_id`
- `environment`
- `account_id`
- `client_id_orders`

Governed companion fields:
- `client_id_observer`
- `host`
- `port`

Canonical upstream authorities and precedence:
1. `C2_SLEEVE_REGISTRY_V1.json` owns sleeve-to-account binding and gateway profile binding for the active sleeve/environment tuple.
2. `ib_account_registry_v1` owns account eligibility and allowed sleeve bindings for the resolved account.
3. `execution_profile_authority_v1` must resolve the same account, orders client id, observer client id, host, and port as the governed registry identity.
4. `submit_boundary_paper_v4.py` must reject any runtime submit identity that differs from the governed tuple before any broker-facing action.

Invalidation conditions:
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

Hard-block rules:
- this contract is not advisory
- submit boundary must fail closed on any invalidation
- no broker submission path may proceed until the identity binding resolves to exactly one sleeve, one IB account, and one orders client id
- no submit-capable runtime may silently substitute a different account or orders client id

Proof basis:
- `governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json`
- `governance/05_CONTRACTS/C2/execution_profile_authority_v1.contract.md`
- `governance/05_CONTRACTS/C2/ib_account_registry_v1.contract.md`
- `governance/05_CONTRACTS/C2/execution_root_authority_v1.contract.md`
- `constellation_2/phaseD/lib/submit_boundary_paper_v4.py`
