# Aegis Scheduled Run Readiness Completion Report v1

Status: Completed

## Files Changed

- `docs/aegis_scheduled_run_readiness_requirements_v1.md`
- `docs/aegis_scheduled_run_readiness_spec_v1.md`
- `docs/aegis_scheduled_run_readiness_design_v1.md`
- `docs/aegis_scheduled_run_readiness_completion_report_v1.md`
- `ops/aegis/scheduled_run_registry_v1.py`
- `ops/aegis/scheduled_run_dependency_manifest_v1.py`
- `ops/aegis/scheduled_run_readiness_certificate_v1.py`
- `ops/aegis/scheduled_run_safe_repair_v1.py`
- `ops/aegis/scheduled_run_reconciliation_v1.py`
- `ops/aegis/scheduled_run_readiness_self_check_v1.py`
- `ops/tools/build_aegis_scheduled_run_readiness_v1.py`
- `ops/tools/run_aegis_scheduled_run_safe_repair_v1.py`
- `ops/tools/build_aegis_scheduled_run_reconciliation_v1.py`
- `ops/tools/run_aegis_scheduled_run_readiness_self_check_v1.py`
- `constellation_2/common/tests/test_aegis_scheduled_run_readiness_v1.py`
- `constellation_2/phaseL/ui/tests/test_aegis_scheduled_run_readiness_ui_v1.py`
- `constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py`
- `constellation_2/phaseL/ui/static/operator_shell/pages/index.js`
- `package.json`
- `aegis/modules/operator_portal/aegis.module.yaml`

## Docs Created

- `docs/aegis_scheduled_run_readiness_requirements_v1.md`
- `docs/aegis_scheduled_run_readiness_spec_v1.md`
- `docs/aegis_scheduled_run_readiness_design_v1.md`
- `docs/aegis_scheduled_run_readiness_completion_report_v1.md`

## Artifacts Created

For `TARGET_DAY=2026-05-30`:

- `/home/node/constellation_runtime_data/truth/reports/aegis_scheduled_run_registry_v1/2026-05-30/scheduled_run_registry.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_scheduled_run_dependency_manifest_v1/2026-05-30/dependency_manifest.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_scheduled_run_readiness_certificate_v1/2026-05-30/readiness_certificate.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_scheduled_run_safe_repair_v1/2026-05-30/safe_repair.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_scheduled_run_reconciliation_v1/2026-05-30/scheduled_run_reconciliation.v1.json`
- `/home/node/constellation_runtime_data/truth/reports/aegis_scheduled_run_readiness_self_check_v1/2026-05-30/self_check.v1.json`

## NPM Commands Added

- `npm run aegis:scheduled-run-readiness`
- `npm run aegis:scheduled-run-repair`
- `npm run aegis:scheduled-run-reconciliation`
- `npm run aegis:scheduled-run-readiness-self-check`

`npm run aegis:audit` now runs scheduled-run readiness and scheduled-run readiness self-check before strict verified graph.

## Scheduled Runs Registered

- `CANDIDATE_GENERATION_0950`
- `MIDDAY_MONITORING`
- `EOD_OUTCOME_UPDATE`
- `RESEARCH_ALLOCATION`
- `VALIDATION_MATURITY`
- `OPERATOR_DASHBOARD_REFRESH`

## Dependency Count

For `2026-05-30`:

- dependency count: `25`
- blocking dependency count: `24`
- ready dependency count: `22`
- blocked dependency count: `2`

## Certificate Examples

Readiness certificate summary for `2026-05-30`:

- certificate count: `6`
- certified ready count: `5`
- blocked count: `1`
- next scheduled run: `CANDIDATE_GENERATION_0950`
- next readiness status: `BLOCKED`

## Repair Examples

Safe repair run for `2026-05-30`:

- repair count: `3`
- success count: `0`
- failed count: `0`
- forbidden count: `0`
- skipped count: `1`

The default repair command is dry-run/constrained; it records safe-repair eligibility without executing external repair commands unless explicitly asked by CLI flag.

## 09:50 Readiness Example

`CANDIDATE_GENERATION_0950` for `2026-05-30`:

- readiness status: `BLOCKED`
- dependencies checked: `7`
- dependencies ready: `5`
- dependencies blocked: `2`
- blocked dependencies: `ALLOWED_SYMBOL_UNIVERSE`, `SLEEVE_INPUT_CONTRACTS`
- repairs attempted: `3`
- repairs successful: `0`
- repairs failed: `0`

This means Aegis does not falsely certify the 09:50 candidate run ready when predictable setup dependencies are missing.

## Post-Run Reconciliation Example

Reconciliation summary for `2026-05-30`:

- run count: `6`
- executed count: `6`
- valid-certificate count gap: `1`
- predictable failure count: `0`

The blocked 09:50 readiness state is preserved separately from run-output existence, so operators can distinguish a run that produced artifacts from a run that had a valid preflight certificate.

## Tests Run

- `python3 -m py_compile` on touched Python files and the UI server file: passed
- `python3 -m pytest constellation_2/common/tests/test_aegis_scheduled_run_readiness_v1.py constellation_2/phaseL/ui/tests/test_aegis_scheduled_run_readiness_ui_v1.py`: `13 passed`
- `TARGET_DAY=2026-05-30 npm run aegis:scheduled-run-readiness`: passed
- `TARGET_DAY=2026-05-30 npm run aegis:scheduled-run-readiness-self-check`: passed
- `TARGET_DAY=2026-05-30 npm run aegis:scheduled-run-reconciliation`: passed
- `TARGET_DAY=2026-05-30 npm run aegis:scheduled-run-repair`: passed
- `TARGET_DAY=2026-05-30 npm run aegis:audit`: passed

## Audit Result

`TARGET_DAY=2026-05-30 npm run aegis:audit` completed successfully after scheduled-run readiness was wired into audit.

- verified graph status: `READY`
- audit blocker count: `0`
- runtime truth classification remains `PARTIAL_CONTEXT`
- highest readiness layer remains `BLOCKED`
- trade advice remains disabled
- manual trade capture remains disabled
- broker submit/transmit remains disabled
- autonomous execution remains disabled

## Remaining Limitations

- The first version certifies artifact/day/schema readiness and records certificate validity windows; it does not replace cron or invoke the actual scheduled workflow.
- Default safe repair is conservative and dry-run oriented. External artifact builder execution should remain explicit.
- 09:50 candidate generation is currently blocked by missing allowed-symbol-universe and sleeve-input-contract artifacts for `2026-05-30`.
- Reconciliation infers execution from known artifact outputs; future versions can bind to explicit run ledger entries when available.

## Recommended Next Move

Use the 09:50 blocked dependency output to decide whether the missing allowed-symbol-universe and sleeve-input-contract artifacts should be regenerated by existing safe builders or documented as expected no-setup for the target day.
