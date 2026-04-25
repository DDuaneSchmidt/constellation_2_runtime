# Household Policy Compiler Contract

## Purpose

This contract governs the household portfolio policy compiler that sits above sleeve deployment and execution admission. The compiler is a deterministic, immutable artifact pipeline:

`policy_snapshot_v1 -> household_state_snapshot_v1 -> compiled_constraints_v1 -> allocation_plan_v1 -> risk_envelope_v1 -> rebalance_candidates_v1 -> tax_adjudicated_rebalance_v1 -> portfolio_authorization_v1 -> portfolio_decision_record_v1`

This layer is a hard-control subsystem. It is not advisory prose and it must fail closed.

## Authority Owners

- `policy_snapshot_v1`: Policy Snapshot Authority
- `household_state_snapshot_v1`: Household State Snapshot Authority
- `compiled_constraints_v1`: Constraint Compilation Authority
- `allocation_plan_v1`: Allocation Compilation Authority
- `risk_envelope_v1`: Risk Compilation Authority
- `rebalance_candidates_v1`: Rebalance Compilation Authority
- `tax_adjudicated_rebalance_v1`: Tax Adjudication Authority
- `portfolio_authorization_v1`: Authorization Compilation Authority
- `portfolio_decision_record_v1`: Audit / replay authority

Each artifact has exactly one owner. Downstream stages must consume upstream artifacts as written and must not recompute upstream truth.

## Allowed Inputs By Stage

- `policy_snapshot_v1`: durable household portfolio policy inputs only
- `household_state_snapshot_v1`: internal positions, imported positions, cash, account registrations, lot metadata, valuation state
- `compiled_constraints_v1`: `policy_snapshot_v1`, `household_state_snapshot_v1`
- `allocation_plan_v1`: `household_state_snapshot_v1`, `compiled_constraints_v1`, proven sleeve demand inputs
- `risk_envelope_v1`: `household_state_snapshot_v1`, `compiled_constraints_v1`, `allocation_plan_v1`
- `rebalance_candidates_v1`: `household_state_snapshot_v1`, `allocation_plan_v1`, `risk_envelope_v1`
- `tax_adjudicated_rebalance_v1`: `household_state_snapshot_v1`, `rebalance_candidates_v1`
- `portfolio_authorization_v1`: `compiled_constraints_v1`, `allocation_plan_v1`, `risk_envelope_v1`, `tax_adjudicated_rebalance_v1`, proven trade intent
- `portfolio_decision_record_v1`: all upstream artifact references plus override lineage

## Forbidden Behaviors

- Policy stages must not read live market state to reinterpret durable policy.
- State stages must not make allocation, tax, or authorization decisions.
- Constraint and risk stages must not select lots, route trades, or emit execution instructions.
- Rebalance stages must not perform tax adjudication.
- Execution consumers must not recompute risk, allocation, or policy from raw inputs when `portfolio_authorization_v1` exists.

## Fail-Closed Rules

- Missing, stale, invalid, or non-provable upstream artifacts block new deployment.
- Degraded inputs may permit only explicitly allowed risk-reduction actions.
- Missing tax truth allows only urgent risk reductions and defers non-urgent rebalances.
- Missing lot truth blocks tax-sensitive liquidation and discretionary imported-position reallocations.
- Missing cash truth blocks new deployment and leaves reductions available only when explicitly authorized.
- Stale state invalidates new portfolio authorization issuance.

## Deterministic Ordering Rules

- Forced reductions sort by breach severity, then excess concentration, then stable symbol/account key.
- Candidate actions sort by urgency, then risk impact, then stable symbol/account/action key.
- Immutable artifact identifiers are content-derived and frozen once written.

## Staleness Rules

- `portfolio_authorization_v1` must carry `valid_from`, `valid_until`, and `stale_if_older_than_seconds`.
- Consumers must reject authorizations outside that validity window.
- State-derived artifacts must surface stale input references explicitly through machine-readable reason codes.

## Override Governance

- Overrides must be explicit, durable, and included in `portfolio_decision_record_v1`.
- Overrides do not rewrite upstream artifacts.
- Overrides must not silently bypass fail-closed execution gates.

## Audit Requirements

- Every authorization compilation cycle must emit `portfolio_decision_record_v1`.
- Decision records must include exact upstream artifact references and accumulated reason codes.
- Replay on identical inputs must yield identical artifact payloads and decisions.

## Non-Change Boundaries

- This contract does not redesign sleeve edge logic, sleeve signal generation, broker plumbing, or unrelated runtime orchestration.
- This contract does not permit execution to consume raw household policy or household state directly when `portfolio_authorization_v1` is in scope.
