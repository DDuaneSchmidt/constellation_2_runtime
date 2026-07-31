# BACKTEST_002 Execution Specification

# IMP_002 — 52-Week Breakout

## Purpose

Freeze execution assumptions for reproducibility.

## Signal Generation

```text
Breakout threshold:
Highest adjusted close over the prior 252 trading days, excluding the current day.

Signal:
Current adjusted close > prior 252-day high.

Exit:
Current adjusted close < 100DMA.
```

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

No leverage. No partial allocation.

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
backtest_002_summary.json
backtest_002_metrics.json
backtest_002_random_baseline.json
backtest_002_subperiods.json
backtest_002_stress_tests.json
backtest_002_run_manifest.json
backtest_002_evidence_report.md
```

under:

```text
docs/aegis/technical_strategy_factory/evidence/backtest_002/
```

## Reproducibility Requirements

Run manifest must include:

```text
implementation_id: IMP_002
claim_id: CLAIM_001
test_plan_id: TEST_PLAN_002
backtest_id: BACKTEST_002
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

