# Aegis Operational Invariants V1

## Manual Capture Workflow Invariants

- If a selected exposure exists, the operator must be able to view it.
- If manual capture existed previously, editable capture state must remain available unless explicitly deprecated.
- Current-truth architecture must not remove operator editing capability.
- UI migrations must preserve operator workflows.
- Canonical projections may centralize truth, but not eliminate manual workflows.
- Operator annotation/capture is NOT equivalent to broker execution.
- Manual capture records are append-only and non-executing.
- Missing broker integration must not disable operator capture tracking.

## Manual Capture Safety Boundary

`manual_capture_record_v1` records operator/manual activity only. It does not route orders, submit trades, allocate capital, mutate selected exposure, create paper-submit artifacts, or weaken governance blockers.

If governance policy disables editing, the workflow must remain visible, fields must be disabled explicitly, and the UI must show the exact policy reason.

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

## Readiness Domain Evaluation v1

- Readiness is multi-domain, not a single blocked/ready pipeline.
- TradeLifecycleCase owns lifecycle state and attaches ReadinessDomainEvaluation_v1 artifacts.
- Manual capture readiness is separate from paper submit readiness.
- Execution readiness is separate and remains NOT_ENABLED unless explicitly governed in a separate packet.
- UI must render domain blockers separately and may not collapse paper-submit or execution blockers into manual-capture blockers.
- Frontend may not infer readiness from raw artifacts; it must render TradeTicketProjection_v1.
- captured_manually requires ManualCaptureDomain READY and does not require PaperSubmitDomain or ExecutionDomain.

## Canonical Dynamic Universe Authority

- There is exactly one authoritative dynamic universe source: `canonical_universe_authority_v1` under canonical truth.
- Dynamic ranked sleeves may consume only `CANONICAL_DYNAMIC`, `RANKED_DYNAMIC`, or `ENGINE_FILTERED` universes with lineage to the canonical authority id.
- Sleeve-local, curated, temporary, deprecated, allocation, portfolio gate, active-registry, alignment, and operator-state artifacts are downstream/read-only for canonical dynamic universe purposes.
- A canonical dynamic universe write that shrinks more than 20% from the last valid authority is blocked as `UNIVERSE_COLLAPSE_PROTECTED` unless an explicit narrowing override artifact exists.
- If current authority breadth is below the configured floor, the authority is `FAILED`, downstream dynamic sleeves block, and last-known-good authority is preserved.

