# Portfolio Authorization Contract

## Purpose

`portfolio_authorization_v1` is the sole household-portfolio execution artifact for advisory-origin trade admission. It states, explicitly and immutably, which actions are allowed, which are blocked, what incremental deployment is permitted, and when the authorization becomes stale.

## Authority Owner

Authorization Compilation Authority owns `portfolio_authorization_v1`.

Execution admission consumes the artifact. Execution must not reinterpret household policy, household state, allocation, or risk outside this artifact.

## Required Inputs

- `compiled_constraints_v1`
- `allocation_plan_v1`
- `risk_envelope_v1`
- `tax_adjudicated_rebalance_v1`
- proven trade intent / execution intent when present

## Required Output Semantics

`portfolio_authorization_v1` must state:

- `allowed_actions[]`
- `blocked_actions[]`
- `max_incremental_deployment`
- `required_prerequisite_actions[]`
- `account_route_permissions[]`
- `reason_codes[]`
- `valid_from`
- `valid_until`
- `stale_if_older_than_seconds`
- `emergency_mode`

Authorization is exact-match scoped. An action not listed as allowed is blocked.

## Forbidden Behaviors

- Execution admission may not recompute portfolio risk or tax logic from upstream artifacts.
- Execution admission may not issue a permissive interpretation when authorization is missing or stale.
- Authorization compilation may not silently invent policy defaults.

## Fail-Closed Rules

- Missing `portfolio_authorization_v1`: block
- Expired or stale `portfolio_authorization_v1`: block
- Action mismatch against `allowed_actions[]`: block
- Account-route mismatch: block
- Blocked-action match: block

Only explicitly authorized reductions may proceed in degraded or emergency modes.

## Deterministic Matching Rules

- Action identity is the canonical action key derived from execution intent household/account/symbol/side/quantity.
- Allowed and blocked action rows must be deterministically ordered.
- Re-evaluation of the same authorization against the same execution intent must yield the same verdict.

## Staleness Rules

- `eval_time_utc` must fall within `[valid_from, valid_until]`.
- `stale_if_older_than_seconds` is a hard expiry bound, not an advisory hint.

## Override Governance

- Operator overrides must be durable and recorded in `portfolio_decision_record_v1`.
- Overrides must not permit execution of actions absent from `allowed_actions[]`.

## Audit Requirements

- Each execution admission decision must be explainable by the exact `portfolio_authorization_v1` and its referenced upstream artifact chain.
- Failure paths must emit machine-readable reason codes, including stale and scope-violation cases.

## Non-Change Boundaries

- This contract does not replace existing kill-switch, readiness, or engine-authorization gates.
- This contract adds a stricter household portfolio gate above them for advisory-origin execution.
