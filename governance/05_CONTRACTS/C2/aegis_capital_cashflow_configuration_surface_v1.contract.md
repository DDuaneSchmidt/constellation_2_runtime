# aegis_capital_cashflow_configuration_surface_v1

Purpose:
- Define the minimal governed UI configuration boundary for `capital_cashflow` authority inputs.

## 1. Mutation Boundary

- Browser clients MUST write drafts only through:
  - `POST /api/configuration/drafts`
- Browser clients MUST NOT write governance registries or runtime truth files directly.
- Drafts are non-authoritative and MUST remain outside canonical activation state.
- Activation MUST run through `configuration_activation_authority_v1` and MUST produce:
  - `configuration_policy_snapshot_v1`
  - `configuration_validation_result_v1`
  - `configuration_review_diff_v1`
  - `configuration_activation_transaction_v1`
  - `configuration_state_v1/current.json`
- Runtime consumers MUST continue reading governed activation artifacts only.

## 2. Locked Safety Fields

The following fields are non-editable from the UI configuration surface:

- `kill_switch.state` (`LOCKED_BY_DESIGN`)
- `broker.transmit_arming` (`LOCKED_BY_DESIGN`)
- `submission_authorization_status` (`DERIVED_READONLY`)
- `trade_submit_readiness.attestation_outputs` (`DERIVED_READONLY`)

Any attempted UI mutation of these fields MUST fail closed.

## 3. Capital Cashflow Lifecycle

Allowed editable fields (only):
- `capital_cashflow.scenario`
- `capital_cashflow.include_inheritance`
- `capital_cashflow.horizon_months`
- `capital_cashflow.start_month`

Lifecycle:
- `DRAFT -> VALIDATED -> REVIEWED -> ACTIVATED -> SUPERSEDED/REJECTED`

Rules:
- Validation MUST run before review.
- Review MUST include exact proposed values.
- Activation MUST fail closed unless validation status is `PASS` and review exists.
- Activation MUST emit an auditable activation artifact path.
- Activation MUST NOT mutate unrelated registries/policies.

Validation constraints:
- `scenario` MUST be one of the backend-supported scenario enums.
- `include_inheritance` MUST be boolean.
- `horizon_months` MUST be integer within bounded safe range.
- `start_month` MUST be `YYYY-MM`.
