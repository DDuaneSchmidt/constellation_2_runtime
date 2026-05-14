# Aegis Lite Pivot

Aegis Lite is the current near-term operating model for Aegis. The product is the EOD manual trade report, not autonomous IB execution.

## Status

- IB automation is deferred for this phase.
- Aegis Lite does not add broker-submit functionality, autonomous order routing, transmit-control logic, or fill lifecycle plumbing.
- Existing IB execution code remains available for future governed reactivation, but the Lite EOD path does not depend on broker submit authority.

## Canonical Flow

The EOD pipeline runs near the close, approximately 15:30-15:45 ET:

1. Market data snapshot validation.
2. Regime classification.
3. Sleeve evaluation.
4. Sandbox and performance review.
5. Trade candidate generation.
6. Sleeve-edge overlap and correlation review.
7. Governance review.
8. Stop and risk definition.
9. Manual execution report generation.
10. Run receipt and audit archive.

The lane is deterministic, replayable, fail-closed, schema validated, and artifact backed.

## Simplified Gates

Gate 1: Data Integrity

- Market data freshness.
- Artifact completeness.
- Schema validity.
- Timestamp consistency.
- Deterministic provenance.

Gate 2: Governance Integrity

- Stop exists.
- Risk contract is valid.
- Sizing guidance is valid.
- Regime compatibility is checked.
- Governance policy is applied.

Gate 3: Report Completeness

- Entry reference exists.
- Stop is defined.
- Sizing guidance exists.
- Reason codes exist.
- Edge-overlap review is complete.
- Manual checklist is present.
- Warnings and blockers are present.

## Root and Coupling Rules

Aegis Lite writes its reports under the selected truth root:

- `reports/sleeve_edge_overlap_review_v1/<DAY>/<RUN_ID>/sleeve_edge_overlap_review.v1.json`
- `reports/aegis_lite_eod_report_v1/<DAY>/<RUN_ID>/aegis_lite_eod_report.v1.json`

Readiness must be based on valid evidence and schema validation, not directory existence. The EOD report lane should not depend on submit-boundary, broker transmit controls, or PAPER-ready order routing artifacts.

## Future Broker Reactivation

Broker automation can be reactivated only through a separate governed phase. That future phase must explicitly reconnect submit authority, broker transmit controls, fill lifecycle handling, and post-trade reconciliation. Aegis Lite artifacts are advisory/manual-entry artifacts and must not be treated as broker execution approval.
