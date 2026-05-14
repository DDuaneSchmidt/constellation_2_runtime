# aegis_lite_eod_report.v1

`aegis_lite_eod_report.v1` is the canonical Aegis Lite EOD manual trade report artifact.

## Purpose

The report is complete enough for an operator to open IB and manually enter or decline the candidate trades without inspecting internal artifacts.

It includes:

- Date, run id, generated timestamp, and market session.
- Market regime state and data freshness status.
- Governance status and simplified gate outcomes.
- Sleeve outputs and sleeve performance summary.
- Sandbox or research notes when available.
- Selected trade candidates from all sleeves.
- Symbol, direction, instrument type, entry reference, sizing guidance, stop price, stop logic, risk per trade, sleeve ownership, confidence, reason codes, and governance notes.
- Edge-overlap and correlation review.
- Duplicate thesis detection.
- Shared-risk and concentration warnings.
- Governance-adjusted approval, reduction, rejection, or manual-review status.
- Manual execution checklist.
- Do-not-trade blockers and warnings.
- Operator notes and source artifact lineage.
- Run receipt and audit archive location.

When feedback artifacts are available, the report also includes open manual positions, missing stop warnings, prior-day operator decisions, manual execution events, current exposure by edge cluster and sleeve, skipped candidate tracking, unsupported manual execution warnings, the operator execution queue, edge clusters, and performance summary.

## Manual Execution Only

The artifact always declares:

- `manual_execution_only: true`
- `ib_automation_status: DEFERRED`
- `broker_submit_required: false`
- `autonomous_order_routing_allowed: false`

Report success does not require IB submit authority. The report does not submit orders, enable transmit, or clear safety gates.

## Fail-Closed Rules

Missing critical candidate fields make a candidate non-executable or manual-review-required:

- Missing entry reference.
- Missing stop price or stop logic.
- Missing risk per trade.
- Missing sizing guidance or quantity.
- Missing symbol, direction, or instrument type.

Stale or missing market data blocks Gate 1. Missing governance or non-executable candidates block Gate 2. Incomplete report fields or missing overlap review block Gate 3.

## Replay

The report is deterministic and includes source artifact lineage. A replay uses the same candidate input, market data freshness status, governance status, and overlap review logic to reproduce the same report body.
