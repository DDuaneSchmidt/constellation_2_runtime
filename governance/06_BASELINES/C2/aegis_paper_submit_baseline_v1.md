# Aegis Paper Submit Baseline V1

- Baseline ID: AEGIS-PAPER-SUBMIT-BASELINE-V1
- Purpose: establish a reproducible paper-submit milestone baseline without changing trading logic.

## Proof Summary
- Export contains execution package, broker submit evidence, readiness decision, and lifecycle/fill artifacts for submission `9ca5e4ae357877b29071813295b35595acb78d20784b191858c4b3a78bbd661c`.
- Baseline manifest records branch, commit, account, and known caveats.

## Artifact Export Location
- `runtime/exports/aegis_baselines/AEGIS-PAPER-SUBMIT-BASELINE-V1/`

## Validation Commands
- `python3 -m compileall governance/06_BASELINES/C2 runtime/exports/aegis_baselines/AEGIS-PAPER-SUBMIT-BASELINE-V1`
- `env PYTHONPATH=/home/node/constellation pytest -q constellation_2/common/tests/test_trade_readiness_reducer_v1.py constellation_2/common/tests/test_submission_lifecycle_refresh_v1.py constellation_2/common/tests/test_submit_boundary_execution_package_v1.py constellation_2/phaseD/tests/test_ib_paper_adapter_v2.py`

## Known Caveat
- broker_order_outcome_v1 is `UNKNOWN_PENDING` and terminal broker outcome is not yet observed.

## Rollback Instructions
1. `git checkout 7d64db4a5e4d68af1d89a56edf64fb9024bb218a`
2. Verify exported baseline files and hashes in `baseline_evidence_manifest_v1.json`.
3. Re-run validation commands above.

## Non-Goals
- Does not prove terminal fill.
- Does not prove live trading.
- Does not approve UI changes.
