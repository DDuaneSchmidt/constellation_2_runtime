# Aegis Operator Action Model Design v1

## Design Principle

The Operator Action Model is a presentation and decision-composition artifact. It does not infer trading truth. It reads canonical Aegis artifacts, composes human-readable capability rows, and points every row back to source artifacts and hashes.

## Builder

Module:

`ops/aegis/operator_action_model_v1.py`

Tool wrapper:

`ops/tools/build_aegis_operator_action_model_v1.py`

Output:

`reports/aegis_operator_action_model_v1/<day_utc>/operator_action_model.v1.json`

## Self-Check

Module:

`ops/aegis/operator_action_model_self_check_v1.py`

Tool wrapper:

`ops/tools/run_aegis_operator_action_model_self_check_v1.py`

Output:

`reports/aegis_operator_action_model_self_check_v1/<day_utc>/self_check.v1.json`

## Data Flow

Runtime truth remains the authority for trade advice, manual capture, broker execution, and autonomous execution. Candidate state and canonical operator state remain authorities for candidate/run state. Paper ledger and outcome registry remain authorities for open/closed simulated positions. Statistical sufficiency remains the authority for validation maturity.

The new flow is:

Runtime/control/candidate/paper/outcome/validation artifacts
-> Operator Action Model builder
-> Command Center Action Capability Matrix
-> Operator-facing explanation

## UI Design

The Command Center should show:

- Top summary: `Monitoring only. No David action required.` when no real human task exists.
- Explanation: `Aegis is blocked from trade advice/manual capture, but not blocked from monitoring.`
- Table columns: Capability, Status, Reason, David Action.

The table replaces vague standalone cards such as:

- `Blocked from acting`
- `Runtime readiness n/a`
- `Runtime capability blockers n/a`

## Self-Check Rules

The self-check validates:

- all required capabilities are present
- every `BLOCKED`, `WAITING`, and `DISABLED_BY_POLICY` status has reason codes
- every capability has `david_action_required`
- no `n/a` blocker field is emitted when source blockers exist
- broker execution is `DISABLED_BY_POLICY`
- trade recommendation cannot be `READY` when `trade_advice_allowed=false`
- manual capture cannot be `READY` without eligible manual trade packet
- `DAVID_ACTION` is active only when a real human task exists
- reruns are deterministic for stable inputs

## Failure Modes

- Missing runtime truth: trade recommendation and manual capture must fail closed with explicit source-missing reason codes.
- Missing paper ledger/outcome registry: paper monitoring and outcome realization must report `BLOCKED` or `WAITING` with source reason codes.
- Missing statistical sufficiency: hypothesis validation must report `BLOCKED` with source reason codes.
- UI cannot fetch model: Command Center may render existing fallback state, but must not claim trade advice, manual capture, or broker execution is available.

## Safety Invariants

- No trade advice is enabled by this model.
- No broker execution is enabled by this model.
- No autonomous execution is enabled by this model.
- No manual capture is enabled unless upstream runtime truth and eligible packet evidence allow it.
- David action is separate from system capability blockers.
