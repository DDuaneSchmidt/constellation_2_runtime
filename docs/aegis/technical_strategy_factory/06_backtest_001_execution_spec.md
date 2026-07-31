# BACKTEST_001 Execution Specification

# IMP_001 — SPY 200DMA Trend Following

## Purpose

Define:

```text
Data source

Corporate action handling

Signal generation

Execution assumptions

Cost assumptions

Output requirements
```

required for reproducible testing.

## Implementation Reference

Reference:

```text
CLAIM_001

IMP_001

TEST_PLAN_001
```

No implementation modifications allowed.

## Data Source Policy

The backtest must explicitly identify:

```text
Vendor

Dataset

Date range

Data frequency
```

Example fields:

```text
data_vendor:
dataset:
frequency:
start_date:
end_date:
```

Do not populate with invented values. Create placeholders only.

## Price Series Policy

Specify:

```text
Adjusted prices required
```

Reason:

```text
Dividends and splits materially affect long-term SPY results.
```

## Corporate Action Policy

Require explicit handling of:

```text
Splits

Dividends

Ticker changes
```

Document:

```text
Backtest must use a price series
consistent with the chosen adjustment policy.
```

## Missing Data Policy

Define:

```text
No silent interpolation.
```

Missing data must be:

```text
Recorded

Explained

Handled according to documented rules
```

## Signal Generation Policy

Signal:

```text
Close > 200DMA
```

Signal evaluation:

```text
After market close.
```

Signal timestamp:

```text
End of trading day.
```

Signal must not use future information.

## Execution Policy

Trade timing:

```text
Signal generated at close.

Execution occurs next market open.
```

Explicitly prohibit:

```text
Same-day execution
```

because it introduces look-ahead bias.

## Position State Policy

Allowed states:

```text
100% SPY

100% Cash
```

No leverage.

No partial allocations.

No discretionary overrides.

## Cost Model

Initial assumption:

```text
10 bps round-trip
```

Record as:

```text
Version 1 cost model
```

Future changes require:

```text
New cost model version
```

not modification.

## Output Requirements

Backtest must produce:

```text
Total Return

CAGR

Annual Volatility

Sharpe

Sortino

Maximum Drawdown

Ulcer Index

Time In Market

Trade Count

Turnover

Average Holding Period
```

and comparison against registered baselines.

## Reproducibility Requirements

Backtest record must include:

```text
Implementation version

Test plan version

Data source version

Cost model version

Execution policy version

Run timestamp
```

## Prohibited Activities

Explicitly prohibit:

```text
Parameter optimization

Look-ahead bias

Manual trade edits

Post-result rule changes

Undocumented assumptions
```

## Completion Criteria

File exists:

```text
docs/aegis/technical_strategy_factory/06_backtest_001_execution_spec.md
```

No code changes.

No backtests executed.

No results generated.

## Recommended Next Step

After this document exists:

```text
Conduct BACKTEST_001_PRE_EXECUTION_AUDIT
```

The audit should review:

```text
Implementation specification

Test plan

Execution specification
```

and determine whether the factory is ready to authorize the first actual backtest.
