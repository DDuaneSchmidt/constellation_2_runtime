# BACKTEST_004 Execution Specification

# IMP_004 — Dual Momentum

## Purpose

Freeze execution assumptions for reproducibility.

## Signal Generation

```text
Absolute momentum lookback:
12 months.

Signal:
SPY adjusted close at signal period close > SPY adjusted close 12 months prior.

Entry signal:
Absolute momentum filter is positive.

Exit signal:
Absolute momentum filter is not positive.
```

Signals must not use future information.

## Evaluation Timing

```text
Signal evaluated monthly after the signal period close.
```

## Execution Timing

```text
Signal generated after signal period close.
Trade executed next market open after signal period.
Same-day execution prohibited.
```

## Position State

```text
100% SPY
100% cash
```

No leverage. No partial allocation. No discretionary overrides.

## Cost Model

```text
10 bps round-trip
5 bps entry
5 bps exit
```

Future cost changes require a new cost model or stress-test artifact.

## Output Requirements

Backtest must produce:

```text
backtest_004_summary.json
backtest_004_metrics.json
backtest_004_random_baseline.json
backtest_004_subperiods.json
backtest_004_stress_tests.json
backtest_004_run_manifest.json
backtest_004_evidence_report.md
```

under:

```text
docs/aegis/technical_strategy_factory/evidence/backtest_004/
```

## Reproducibility Requirements

Run manifest must include:

```text
implementation_id: IMP_004
claim_id: CLAIM_001
test_plan_id: TEST_PLAN_004
backtest_id: BACKTEST_004
data_vendor
dataset
ticker
start_date
end_date
download_timestamp or cache_timestamp
price_fields_used
adjustment_policy
cost_model
execution_policy
random_seed
random_simulation_count
script_path
code_version_or_git_sha_if_available
run_timestamp
result_artifact_paths
```

## Prohibited Activities

```text
Parameter optimization
Look-ahead bias
Manual trade edits
Post-result rule changes
Undocumented assumptions
Claim certification
Capital allocation recommendation
```
