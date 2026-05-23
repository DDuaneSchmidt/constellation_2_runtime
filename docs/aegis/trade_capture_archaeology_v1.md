# Trade Creation / Manual Capture Archaeology V1

Date: 2026-05-20

## Active Paths Found

- `portfolio_gate_candidate_report_v1`: current selected-exposure authority. Produces selected candidate id, symbol, sleeve/engine, direction, score, rank, source day, source run context, and suppressed watchlist rows.
- `current_operator_truth_resolver_v1`: current truth resolver. Binds selected exposure to the matching same-day converter/package diagnostic and rejects stale selected pointers such as old QQQ.
- `operator_state_snapshot_v1`: previous UI read model. It exposed `manual_capture_candidate_v1`, but that shape did not require entry price, quantity, stop/invalidation, risk, or all governance fields.
- `trade_candidate_projection_v1`: new canonical selected trade/manual-capture read model. This is now the UI display authority.
- `manual_capture_record_v1`: append-only operator annotation record. It records manual activity only and does not execute, submit, allocate, or mutate selected exposure.
- `exposure_intent_paper_submission_package_v1`: converter/package diagnostic input. It can provide conversion status, paper intent status, market freshness, and conversion blockers.
- `submit_boundary_status_v1`: governance input to projection. Missing status is now explicit as `MISSING_SUBMIT_BOUNDARY_STATUS`.

## Legacy / Deprecated Paths Found

- `aegis_manual_external_capture_v1`: older UI/API capture path for candidate review workflows. It remains legacy read-only/migration-only and is not current trade authority.
- `manual_execution_receipt.v1`: legacy Lite/manual packet receipt contract. Useful for historical receipts and performance attribution, but not the current UI authority for selected exposure capture.
- `paper_intent_candidate_diagnostics_v1`: legacy diagnostic source. It may feed projections but is not UI authority.
- Raw `selected_intent_pointer.v1`: not current UI authority. Current truth resolver prefers same-day portfolio gate selected exposure.

## Regressed / Weak Areas

- Selected exposure could render as capturable while entry price, quantity/sizing, stop/invalidation, or risk basis were absent.
- Manual capture form collected quantity/fill fields but not stop/invalidation, and stale/blocker status was not contract-bound to capture validation.
- UI rendered the older `manual_capture_candidate_v1` shape, so read-model migrations could remove operator workflow fields.
- Converter mismatch handling correctly avoided mixing old QQQ blockers into AMT, but missing trade-definition fields were not surfaced as first-class blockers.

## Durable Contract Decision

`trade_candidate_projection_v1` is the sole selected trade/manual-capture display authority. A selected exposure is review-only or incomplete unless this projection either supplies required trade fields or emits explicit missing-data blockers.

No broker integration, live order path, order routing, capital allocation, paper submit side effect, or candidate selection behavior is introduced by this change.
