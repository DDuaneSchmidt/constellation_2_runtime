# fresh_day_admission_v1

`fresh_day_admission_v1` is a legacy admission surface retained for diagnostic continuity.

Binding day-entry authority is now owned by `target_day_admission_v1` under `session_authority_v1`.

`fresh_day_admission_v1` may still summarize the legacy three-artifact admission plane, but it is no longer the canonical rollover owner.

In PAPER bootstrap mode, the legacy surface may also summarize a governed bootstrap admission when all of the following exist for the target day:

- governed `paper_capital_seed_v1`
- governed paper `operator_statement.v1.json`
- sleeve `capital_risk_envelope_v2` with `status == PASS`

This diagnostic allowance does not transfer ownership away from `target_day_admission_v1`.

## Allowed outcomes

- `ADMIT`
- `BLOCKED`

## Required pre-admission artifacts

The following target-day artifacts must already exist before `ADMIT`:

- `paper_policy_verdict_v1`
- `trade_submit_readiness_c2_v1`
- `trading_day_state_machine_v1`

`next_day_readiness_probe_v1` remains forecast-only, but it is mandatory admission context. When the required target-day artifacts are missing or blocked, the probe status must be surfaced as part of the blocking explanation.

## Enforcement

No live target-day execution may begin unless `target_day_admission_v1.admission_status == ADMIT`.

## Non-goals

- no producer duplication
- no hidden target-day materialization after admission
- no replacement of `capability_state_v1`, `paper_policy_verdict_v1`, or `production_policy_verdict_v1`
- no execution side effects inside the admission decision itself
