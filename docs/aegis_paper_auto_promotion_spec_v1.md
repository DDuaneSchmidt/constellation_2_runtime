# Aegis Paper Auto-Promotion Spec v1

Date: 2026-06-01

## Scope

This spec adds a paper-only auto-promotion path for research observations. It does not change live trading, trade advice, broker execution, manual capture, autonomous execution, or safety gates.

## State Model

### `AUTO_PROMOTED_TO_PAPER_TRACKING`

The candidate has passed paper-only eligibility and is admitted to the paper position ledger as a research observation.

### `AUTO_PROMOTION_BLOCKED`

The candidate could otherwise be considered for paper tracking, but a blocking dependency prevents auto-promotion.

### `AUTO_PROMOTION_NOT_ELIGIBLE`

The candidate is not eligible for auto-promotion because required candidate contract, governance, linkage, or certification fields are absent or invalid.

## Existing State Preserved

`APPROVED_FOR_PAPER` continues to mean explicit human approval. Auto-promotion must not emit or require this state.

## Auto-Promotion Eligibility

Input row: valid candidate contract from `aegis_candidate_contracts_v1`.

Required candidate fields:

- `candidate_id`
- `symbol`
- `sleeve_id`
- `hypothesis_id`
- `thesis_id`
- `entry_reference_price_certification_status = CERTIFIED`
- governed instrument status

Required artifacts:

- candidate contracts
- candidate review packet or paper review queue
- paper trade construction
- paper session ledger
- paper position ledger
- runtime truth kernel

## Paper Position Admission

Eligible auto-promoted candidates create paper-position rows with:

- `status = PAPER_POSITION_OPEN`
- `current_state = AUTO_PROMOTED_TO_PAPER_TRACKING`
- `paper_tracking_mode = AUTO_PROMOTED_RESEARCH_OBSERVATION`
- `human_approval_status = NOT_HUMAN_APPROVED`
- `operator_review_required = false`
- `trade_advice_allowed = false`
- `manual_capture_allowed = false`
- `broker_execution_allowed = false`
- `autonomous_execution_allowed = false`

## Audit Fields

Each auto-promoted position must include:

- `candidate_id`
- `hypothesis_id`
- `thesis_id`
- `sleeve_id`
- `auto_promotion_reason_codes`
- `source_artifacts`
- `source_hashes`
- `promotion_timestamp`

## Outcome and Validation Integration

Auto-promoted paper positions must be visible to:

- `aegis_outcome_registry_v1`
- `aegis_validation_samples_v1`

Existing outcome and validation builders may consume the paper position ledger if their contracts already support paper positions. No live-capital path may consume auto-promotion as approval.

Auto-promotion must preserve the certified entry reference price lineage needed by outcome validation: entry price, entry price timestamp, source artifact, source hash, and certification status. This lineage is required so later realized returns can be replayed from evidence instead of inferred from UI or latest-market context.

## Safety Policy

Auto-promotion is paper-only:

- `trade_advice_allowed = false`
- `manual_capture_allowed = false`
- `broker_execution_allowed = false`
- `broker_submit_transmit_allowed = false`
- `autonomous_execution_allowed = false`
- `live_trade_eligible = false`
- `automatic_approval_allowed = false`

## Determinism

For the same input artifacts, auto-promotion output must be stable except for generated timestamps. Normalized self-check comparison may ignore generated timestamps.
