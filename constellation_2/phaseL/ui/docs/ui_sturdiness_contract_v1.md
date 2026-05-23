# UI Sturdiness Contract v1

- No UI route may crash because a generated artifact is missing.
- No frontend panel may assume artifact presence.
- All generated projections used by the UI must be versioned.
- Deprecated or missing artifacts must show degraded warnings, not break the shell.
- `backend_unavailable` appears only when the backend is unreachable.
- Degraded data is a valid UI state.
- Every new packet that touches UI/API must add regression coverage for route registration, import health, and degraded artifact handling.
- Operator projection routes are read-only and must not submit orders, allocate capital, route broker requests, mutate sleeves, or weaken governance.

## Operator workflows are protected invariants

- Current-truth architecture may change state sourcing.
- Operator interaction capabilities may not silently regress.
- If a selected exposure exists, the manual capture workflow must remain visible.
- Blockers and stale-data warnings may disable or constrain editing only with an explicit policy reason.

## Trade Creation / Manual Capture Invariants

- A selected trade candidate must always show quantity, price, stop/invalidation, and lifecycle status fields.
- If a required value is missing, the UI shows a blocker instead of hiding the field.
- Manual capture is operator annotation, not broker execution.
- UI migrations may not remove manual capture fields.
- Current truth resolver owns selected exposure.
- `paper_trade_construction_v1` owns selected trade/manual-capture display; legacy projections are compatibility-only.
- No frontend may derive trade readiness directly from raw artifacts.


## Paper Trade Construction Contract Invariants

- A selected exposure is not a trade.
- A selected exposure becomes capture-ready only through `paper_trade_construction_v1`.
- Every trade requires entry/reference price, quantity, notional, stop or invalidation, and risk estimate.
- Missing values must appear as explicit construction blockers with upstream causes.
- The UI may not hide required trade fields.
- Manual capture is append-only operator annotation; it is not broker execution, order routing, capital allocation, or paper submit.
- No frontend may calculate trade readiness or infer missing trade fields.
- No stale selected exposure may appear as current.
- Every packet touching UI or trade lifecycle must pass invariant tests.
- Submit-boundary blockers may be displayed, but must never be bypassed by UI or manual capture.

## TradeLifecycleCase Contract Invariants

- Selected exposure is not a trade.
- `TradeLifecycleCase_v1` owns manual paper trade lifecycle state.
- UI renders manual capture from `trade_case_projection_v1` only.
- Manual capture requires `CAPTURE_READY`.
- Every required trade field must display or appear as an explicit blocker.
- `manual_capture_record_v1` is append-only operator annotation and must reference the trade lifecycle case plus paper trade construction.
- No frontend may calculate trade readiness, hide blockers, submit orders, allocate capital, or bypass the paper construction contract.

## Trade Readiness Domain UI Contract

- The Opportunities manual-capture UI renders TradeTicketProjection_v1 only.
- Every selected exposure shown to an operator must include all readiness domains: Market Data, Construction, Capital, Stop/Risk, Manual Capture, Paper Submit, and Execution.
- Manual capture readiness is separate from paper-submit readiness; paper-submit blockers must not imply manual annotation is unavailable.
- Execution readiness is separate and disabled by default.
- Required ticket fields must display as values or as domain blockers; the frontend may not hide missing entry, quantity, stop/invalidation, or risk.
- The frontend may not infer capture readiness from raw selected exposure, paper construction, submit boundary, or broker state.

## Universe Authority Display

- UI/API surfaces must identify universe type and canonical authority lineage; frontend code may not infer dynamic universe readiness from sleeve-local manifests.
- Universe health must show current count, last-known-good count, shrink percent, writer process, freshness, degraded status, and blocked overwrite attempts.
- Dynamic sleeve evaluation counts below policy floor must display `UNIVERSE_BREADTH_FAILURE`; a tiny curated or sleeve-local manifest must never appear as a valid dynamic source.

