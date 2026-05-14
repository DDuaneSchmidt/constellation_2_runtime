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

## READY_FOR_MANUAL_ENTRY

`READY_FOR_MANUAL_ENTRY` is fail-closed. It is emitted only when:

- Data integrity status is exactly `PASS`.
- Governance status is exactly `PASS`.
- Every candidate has symbol, direction, instrument type, entry reference, stop, risk, and positive quantity.
- Every candidate is in a supported manual trade class.
- A valid `operator_execution_queue.v1` is present.
- Queue items are `READY_FOR_MANUAL_ENTRY`.
- Manual recipes are present.
- There are no do-not-trade blockers.
- Protective stop requirements for open manual positions are satisfied.

Unknown, missing, malformed, `WARN`, or `REVIEW_REQUIRED` statuses block readiness. Reports can still be generated as advisory artifacts, but they must not be treated as manual-entry ready.

## Fail-Closed Rules

Missing critical candidate fields make a candidate non-executable or manual-review-required:

- Missing entry reference.
- Missing stop price or stop logic.
- Missing risk per trade.
- Missing sizing guidance or quantity.
- Missing symbol, direction, or instrument type.

Stale or missing market data blocks Gate 1. Missing governance or non-executable candidates block Gate 2. Incomplete report fields or missing overlap review block Gate 3.

Do-not-trade blockers include missing entry, missing stop, missing risk, missing quantity, unsupported manual execution, malformed status, missing queue, missing recipe, missing protective stop, and unprotected open positions.

## Replay

The report is deterministic and includes source artifact lineage. A replay uses the same candidate input, market data freshness status, governance status, and overlap review logic to reproduce the same report body.
