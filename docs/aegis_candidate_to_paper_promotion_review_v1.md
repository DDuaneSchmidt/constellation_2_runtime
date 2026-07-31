# Aegis Candidate-to-Paper Promotion Review v1

Target day: 2026-06-01

## Review Scope

This review traced the current candidate promotion chain:

Valid Candidate Contract -> Candidate Review Eligibility -> Promotion Gate -> Paper Session Eligibility -> Paper Position Construction -> Paper Ledger Entry -> Outcome Registry.

Safety constraints remained unchanged: no trade advice, no manual capture enablement, no broker execution, no autonomous execution, and no forced paper positions.

## Reproduced Current State

`TARGET_DAY=2026-06-01 npm run aegis:candidate-diagnostics` reported:

- total_raw_signals: 21
- valid_candidate_contracts: 19
- rejected_candidate_contracts: 2
- total_candidates_generated: 19
- total_sleeves_run: 7

The two rejected raw signals remain rejected for `INSTRUMENT_TYPE_NOT_GOVERNED` and are outside the valid-candidate-to-paper bottleneck.

## Valid Candidate Distribution

| Sleeve | Valid candidates | Symbols |
| --- | ---: | --- |
| C2_CROSS_ASSET_TREND_V1 | 1 | QQQ |
| C2_TREND_EQ_PRIMARY_V1 | 18 | AAL, AAPL, AMD, AMDL, AMT, APTV, ARM, BAC, BANC, BKSY, BNS, BOXX, BTSG, BURL, CACC, CCL, CRDO, CSCO |

## Lifecycle Findings

The canonical lifecycle artifact `reports/aegis_candidate_to_paper_lifecycle_v1/2026-06-01/candidate_to_paper_lifecycle.v1.json` reports:

- valid_candidate_contract_count: 19
- review_eligible_count: 19
- promotion_eligible_count: 0
- constructed_paper_trade_count: 19
- paper_positions_created_count: 0
- blocked_from_paper_count: 19
- awaiting_review_count: 19

All 19 valid candidates reached review eligibility and paper-trade construction. None reached paper position ledger entry.

## Per-Candidate Classification

Every valid candidate has:

- review_status: `AWAITING_REVIEW`
- promotion_status: `BLOCKED_PENDING_OPERATOR_REVIEW`
- paper_session_status: `CANDIDATES_GENERATED`
- paper_construction_status: `CONSTRUCTED`
- paper_position_id: empty
- blocker_classification: `PROMOTION_GATE_BLOCKED`
- blocker_reason_codes: `HUMAN_REVIEW_REQUIRED`, `PROMOTION_GATE_BLOCKED`
- expected_safety_behavior: true
- repairable_system_issue: false

## Root Cause

The candidates did not silently disappear and did not fail paper construction. They stopped at the governed human-review promotion gate used by `HUMAN_REVIEWED_PAPER_MODE`.

The repairable issue was evidence visibility and lineage preservation:

- candidate review packet rows did not consistently preserve `hypothesis_id` and `thesis_id`
- paper review queue rows did not expose full lifecycle status fields
- constructed paper trade rows did not preserve full candidate/sleeve/hypothesis/thesis lineage
- there was no canonical candidate-to-paper lifecycle artifact proving where each valid candidate stopped

## Blocker Classification

| Blocker | Count | Interpretation |
| --- | ---: | --- |
| PROMOTION_GATE_BLOCKED | 19 | Expected human-review gate, not a wiring failure |
| HUMAN_REVIEW_REQUIRED | 19 | Operator approval is required before paper ledger entry |

## Repair Boundary

No promotion standards were lowered. No paper positions were forced. The implementation adds traceability, reason codes, self-checks, and Command Center visibility only.
