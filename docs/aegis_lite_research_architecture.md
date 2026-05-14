# Aegis Lite / Research Lab Architecture

Aegis is pivoted away from autonomous IB paper execution. The long-term operating model is:

Research Lab -> Sleeve Library -> Aegis Lite EOD Engine -> Manual Execution Layer -> Outcome Ledger.

The principles are fixed:

- Research Lab discovers.
- Sleeve Library validates.
- Aegis Lite decides.
- Operator executes manually.
- Outcome Ledger teaches.

## Research Lab

Research Lab is offline and non-executable. It works from a durable `hypothesis_registry.v1`; every task starts from a structured hypothesis and an explicit trigger. Research may propose, test, deduplicate, reject, or recommend promotion, but it cannot create actionable trades or mutate Aegis Lite runtime behavior.

Supported lifecycle states are `proposed`, `exploratory`, `promising`, `validated`, `promoted`, `rejected`, and `retired`.

## Hypothesis Test Plan

Every hypothesis receives `hypothesis_test_plan.v1`. A hypothesis is not tested until the plan reaches a terminal state.

Required stages are:

1. `definition_check`
2. `duplicate_overlap_check`
3. `exploratory_backtest`
4. `regime_segmentation`
5. `robustness_check`
6. `transaction_friction_check`
7. `out_of_sample_check`
8. `failure_mode_review`
9. `edge_overlap_review`
10. `promotion_review`

Promotion requires a completed plan, out-of-sample support, regime notes, failure-mode notes, edge-overlap review, and human/operator approval.

## Promotion Boundary

Exploratory, promising, rejected, or unreviewed research cannot influence Aegis Lite EOD reports. Research Lab may emit `research_to_lite_promotion.v1`, but the artifact is advisory governance evidence only. Only promoted sleeves in `promoted_sleeve_library.v1` can be used by the Lite EOD engine.

## Sleeve Library

The promoted sleeve library contains only production-eligible sleeves. Each sleeve declares its edge family, behavioral thesis, regime fit, instrument universe, entry/stop/sizing/invalidation logic, known failure modes, overlap tags, and promotion evidence path.

## Aegis Lite EOD Engine

Aegis Lite runs once per trading day near 15:30-15:45 ET. It evaluates promoted sleeves only, classifies regime, generates candidate trades, performs edge-overlap review, applies governance, and writes a complete manual execution report. It does not submit orders or enable transmit.

The source boundary is `promoted_sleeve_library.v1`. The EOD engine wrapper filters candidate inputs to sleeves present in that library with `promotion_status=promoted`; research-only or unpromoted candidates are excluded before report generation.

## Manual Execution Layer

The operator manually enters any READY trades in IB paper and records `manual_execution_receipt.v1`. The receipt is observational and records actual symbol, side, quantity, order type, fill price, stop order, operator notes, and deviations from the recommendation.

## Outcome Ledger

`outcome_ledger.v1` compares the recommendation, manual receipt, actual outcome, stop behavior, sleeve attribution, and edge-overlap attribution. It feeds future Research Lab and sleeve-performance review without becoming an execution authority.
