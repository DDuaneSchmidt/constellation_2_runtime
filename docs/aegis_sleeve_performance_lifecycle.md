# Aegis Sleeve Performance Lifecycle

Date: 2026-05-15

This document defines the canonical Aegis Lite paper-trade performance lifecycle. It does not add IB automation, broker submit, fill automation, production activation, automatic promotion, or autonomous execution.

## Lifecycle

The canonical measurement chain is:

`manual_trade_packet.v1 -> manual_execution_receipt.v1 -> outcome_ledger.v1 -> trade_outcome_attribution.v1 -> sleeve_performance_report.v1 -> Research Lab learning loop`

`sleeve_performance_report.v1` is the operator-facing report for Aegis Lite paper-trade performance. It is generated offline by:

```bash
python3 ops/tools/build_sleeve_performance_report_v1.py --truth_root <path> --day <YYYY-MM-DD>
```

The command requires an explicit `--truth_root`, reads existing artifacts, writes only `reports/sleeve_performance_report_v1/<day>/sleeve_performance_report.v1.json`, and does not access broker or IB paths.

## Source Of Truth

The recommendation source is `manual_trade_packet.v1`.

The execution source is `manual_execution_receipt.v1`. When IB is not integrated, the receipt is the operator-entered truth for actual fill, quantity, stop entry, fill timing, and deviations.

The outcome source is `outcome_ledger.v1`, with optional `trade_outcome_attribution.v1` enrichment. Missing receipt or outcome evidence is never treated as zero return.

## Join Rules

The deterministic join order is:

1. `manual_trade_packet.v1` trade candidate `recommended_trade_id`.
2. `manual_execution_receipt.v1` `recommended_trade_id`.
3. `outcome_ledger.v1` `trade_id`.
4. `sleeve_id` for sleeve attribution.
5. `source_hypothesis_id` for Research Lab lineage.
6. `event_id` and `alert_id` for event/alert attribution when present.

Duplicate recommendation, receipt, or outcome joins fail closed with `INVALID_PACKET` diagnostics.

## Trade Lifecycle Status

Each recommendation is classified as one of:

- `RECOMMENDED_NOT_EXECUTED`
- `EXECUTED_OPEN`
- `EXECUTED_CLOSED`
- `MISSING_RECEIPT`
- `MISSING_OUTCOME`
- `INVALID_PACKET`
- `BLOCKED`
- `IGNORED`
- `MISSED_VALIDITY_WINDOW`

Blocked or invalid packet candidates do not count as executed performance. Ignored, missed, missing-receipt, and missing-outcome rows do not receive synthetic zero returns.

## Sleeve Returns

Sleeve return aggregation uses closed executed rows with numeric `return_pct` values from `outcome_ledger.v1`.

The report computes:
- recommended, executed, ignored/missed, missing receipt, and missing outcome counts,
- total and average return,
- win rate,
- average win/loss,
- stop-hit rate,
- MAE/MFE,
- regime performance,
- event performance,
- alert-driven performance.

Open trades and missing outcomes are reported separately and are not folded into realized return.

## Execution Quality

Execution quality compares the Aegis recommendation with the manual receipt:
- recommended entry vs actual fill,
- slippage and slippage percent,
- valid-until compliance,
- max-entry-slippage compliance,
- stop entered,
- stop matched recommendation,
- operator deviations.

This is an operator-quality measurement, not a broker automation path.

## Research Learning

The report may recommend offline Research Lab follow-up tasks when it detects:
- sleeve or stop failure,
- edge-overlap failure,
- event failure,
- alert false positive,
- missed validity window,
- material slippage or operator deviation,
- missing outcome evidence.

The report does not write `research_task_queue.v1`. It includes deterministic task recommendations under `research_feedback.generated_or_recommended_tasks`; a separate explicit Research Lab step may enqueue them later.

## UI Status

No dedicated Lite UI surface for `sleeve_performance_report.v1` is proven yet. Until one is added, the operator-facing surface is the artifact path and CLI command above.

## Safety Boundary

`sleeve_performance_report.v1` is observational. It cannot:
- submit orders,
- enable transmit,
- infer fills,
- mutate canonical EOD state,
- mutate Lite runtime,
- promote sleeves,
- authorize trades,
- overwrite missing receipts or outcomes.
