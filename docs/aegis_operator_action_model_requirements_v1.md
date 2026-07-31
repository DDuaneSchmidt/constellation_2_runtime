# Aegis Operator Action Model Requirements v1

## Purpose

Create a canonical operator-explanation artifact that separates Aegis capability status from David action status. The model must eliminate vague combinations such as `Blocked from acting` and `No David action required` by stating exactly which capability is blocked, waiting, complete, active, disabled, or not applicable.

## Scope

The Operator Action Model is a read-only composition layer. It consumes existing runtime truth and daily Aegis artifacts. It does not own truth, enable trading, modify candidates, change validation state, or bypass policy gates.

## Required Questions

The model must answer separately:

1. Can Aegis operate?
2. Can Aegis generate candidates?
3. Can Aegis monitor paper positions?
4. Can Aegis close paper outcomes?
5. Can Aegis validate hypotheses?
6. Can Aegis issue trade recommendations?
7. Can Aegis allow manual trade capture?
8. Does David need to do anything?

## Required Capabilities

The capability matrix must include:

- `CANDIDATE_GENERATION`
- `PAPER_MONITORING`
- `OUTCOME_REALIZATION`
- `HYPOTHESIS_VALIDATION`
- `TRADE_RECOMMENDATION`
- `MANUAL_TRADE_CAPTURE`
- `BROKER_EXECUTION`
- `DAVID_ACTION`

## Capability Fields

Each capability row must include:

- `capability_id`
- `status`
- `status_label`
- `reason_codes`
- `human_readable_reason`
- `source_artifacts`
- `source_hashes`
- `next_expected_event`
- `david_action_required`

Allowed status values are `READY`, `COMPLETE`, `ACTIVE`, `WAITING`, `BLOCKED`, `NOT_APPLICABLE`, and `DISABLED_BY_POLICY`.

## Inputs

Use existing canonical artifacts where available:

- runtime truth kernel
- ChatGPT control packet
- canonical operator state
- candidate state
- candidate diagnostics / candidate UI projection
- sleeve analytics
- outcome registry
- statistical sufficiency
- research portfolio
- research allocation decisions
- paper position ledger
- command center queue audit
- portal runtime model where appropriate

## Non-Goals

- Do not enable trade advice.
- Do not enable broker execution.
- Do not enable autonomous execution.
- Do not weaken runtime truth.
- Do not create trading behavior.
- Do not duplicate truth ownership from the runtime kernel, candidate state, paper ledger, or validation artifacts.

## Success Criteria

- Current-day artifact exists at `reports/aegis_operator_action_model_v1/<day_utc>/operator_action_model.v1.json`.
- All required capabilities are present.
- `BLOCKED`, `WAITING`, and `DISABLED_BY_POLICY` rows include reason codes and plain-English reasons.
- David action is represented as a separate row and does not imply capability readiness.
- Command Center shows an Action Capability Matrix instead of vague blocker cards.
- Self-check passes and is wired into `npm run aegis:audit`.
