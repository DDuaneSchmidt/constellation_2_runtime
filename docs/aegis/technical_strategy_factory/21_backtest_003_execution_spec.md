# BACKTEST_003 Execution Specification

# IMP_003 — Turtle Breakout

## Purpose

Freeze execution assumptions for reproducibility.

## Signal Generation

```text
Entry breakout threshold:
Highest adjusted close over the prior 55 trading days, excluding the current day.

Entry signal:
Current adjusted close > prior 55-day high.

Exit threshold:
Lowest adjusted close over the prior 20 trading days, excluding the current day.

Exit signal:
Current adjusted close < prior 20-day low.
```

Signals must not use future information.

## Execution Timing

```text
Signal generated after market close.
Trade executed next market open.
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
backtest_003_summary.json
backtest_003_metrics.json
backtest_003_random_baseline.json
backtest_003_subperiods.json
backtest_003_stress_tests.json
backtest_003_run_manifest.json
backtest_003_evidence_report.md
```

under:

```text
docs/aegis/technical_strategy_factory/evidence/backtest_003/
```

## Reproducibility Requirements

Run manifest must include:

```text
implementation_id: IMP_003
claim_id: CLAIM_001
test_plan_id: TEST_PLAN_003
backtest_id: BACKTEST_003
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
