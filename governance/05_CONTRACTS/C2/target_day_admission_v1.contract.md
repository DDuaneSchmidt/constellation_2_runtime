# target_day_admission_v1

`target_day_admission_v1` is the binding admission decision for day entry.

Canonical output:
- `/home/node/constellation_runtime_data/truth/target_day_admission_v1/<DAY>.json`

Rules:
- it may consume canonical runtime truth only through `target_day_build_v1`
- it must emit `ADMIT` or `BLOCKED`
- `binding=true` is required
- admission inherits the governed BOD-critical execution substrate proof exclusively through `target_day_build_v1`; it must not bypass or recompute that prerequisite out-of-band
- admission must fail closed unless:
  - `completeness_result == COMPLETE`
  - `closure_status == CLOSED`
  - `hidden_dependency_check_result.status == PASS`
  - `blocker_chain[]` is empty
- PAPER-only exception:
  - `target_day_admission_v1` may emit `ADMIT` with `mode=PAPER_BOOTSTRAP` and `reason=PAPER_BOOTSTRAP_SESSION_ADMISSION`
  - this exception is allowed only when governed PAPER bootstrap prerequisites exist:
    - governed `paper_capital_seed_v1` for `target_day`
    - governed `cash_ledger_operator_statements/<DAY>/operator_statement.v1.json` for `target_day`
    - sleeve `capital_risk_envelope_v2` for `PRIMARY/PAPER` is materialized and `status == PASS`
  - the PAPER bootstrap exception must remain fail-closed outside `environment == PAPER`
  - the PAPER bootstrap exception must not alter non-PAPER admission rules or recompute production-only prerequisites out-of-band
- `blocking_reason_codes[]` must surface stable blocker taxonomy codes
- `ADMIT` must reference the exact build artifact used
- any missing build input, open closure, or hidden dependency result must block admission
- execution may not begin for `target_day` unless `target_day_admission_v1.admission_status == ADMIT`
- `ADMIT` authorizes downstream `day_open_trigger_v1` emission but does not by itself prove that the open command executed
- the open path must consume `ADMIT` through the governed `day_open_trigger_v1 -> day_open_attempt_v1` chain, not by timer-only inference
