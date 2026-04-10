# fresh_day_admission_v1

`fresh_day_admission_v1` is the fail-closed Fresh-Day Admission Plane authority.

It decides one question only:

- may `target_day_utc` enter live execution at all?

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

No live target-day execution may begin unless `fresh_day_admission_v1.admission_status == ADMIT`.

## Non-goals

- no producer duplication
- no hidden target-day materialization after admission
- no replacement of `capability_state_v1`, `paper_policy_verdict_v1`, or `production_policy_verdict_v1`
- no execution side effects inside the admission decision itself
