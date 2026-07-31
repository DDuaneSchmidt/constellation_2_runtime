# Aegis Paper Auto-Promotion Design v1

Date: 2026-06-01

## Architecture

Paper auto-promotion is implemented inside the existing candidate-to-paper lifecycle boundary instead of adding a new scheduler or live execution path.

Existing:

Candidate contract -> paper review queue -> human approval -> paper position

New paper-only path:

Candidate contract -> auto-promotion eligibility -> `AUTO_PROMOTED_TO_PAPER_TRACKING` -> paper position -> outcome registry -> validation

## Data Ownership

`aegis_candidate_to_paper_lifecycle_v1` owns the paper-only promotion decision and emits:

- per-candidate auto-promotion status
- auto-promotion reason codes
- paper position tracking rows
- source artifact and source hash lineage

`aegis_paper_position_ledger_v1` remains the paper position ledger consumer and should carry auto-promoted paper observations without treating them as human-approved entries.

## Eligibility Evaluation

Eligibility is evaluated per valid candidate contract.

Allowed:

- valid contract
- certified entry reference price
- governed instrument
- hypothesis linkage
- sleeve linkage
- thesis linkage
- available paper session
- available paper ledger
- runtime truth does not block paper candidates/review/tracking

Blocked:

- paper session missing or blocked
- paper ledger missing
- paper construction failed for an otherwise eligible candidate
- runtime truth blocks paper tracking capability

Not eligible:

- invalid contract
- uncertified entry reference price
- ungoverned instrument
- missing hypothesis/sleeve/thesis linkage

## Human Approval Separation

`APPROVED_FOR_PAPER` is unchanged and remains an explicit operator decision state.

Auto-promotion emits `AUTO_PROMOTED_TO_PAPER_TRACKING`; it does not mark candidates human approved and does not satisfy any live-capital approval requirement.

## Safety Boundaries

The implementation must not modify:

- trade advice gates
- manual capture gates
- broker execution gates
- autonomous execution gates
- live trading gates
- sleeve logic
- candidate generation

Auto-promoted paper observations are research data only.

## UI Projection

Existing UI projections can read new summary fields:

- `auto_promoted_to_paper_tracking_count`
- `human_approved_for_paper_count`
- `blocked_from_paper_count`
- `auto_promotion_blocked_count`
- `auto_promotion_not_eligible_count`

## Self-Check Design

The candidate-to-paper self-check verifies:

- every auto-promoted row has complete lineage
- every auto-promoted row is paper-only
- no auto-promoted row enables trade advice, manual capture, broker execution, or autonomous execution
- every auto-promoted row has an outcome registry row
- validation pipeline can consume the resulting paper position ledger
- deterministic rerun stability

## Rollout

The first implementation supports current valid candidate contracts for `2026-06-01`, where 19 valid candidates are currently blocked by `HUMAN_REVIEW_REQUIRED`.
