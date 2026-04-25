# Promotion Record Contract

Contract ID: `ADVISORY_PROMOTION_RECORD_CONTRACT_V1`

## Purpose

Define `PromotionRecord` as the single authoritative bridge from the advisory domain to the trading domain.

## Scope

This contract governs advisory-to-trading promotion only. It does not authorize broker submission directly.

## Ownership

The authoritative owner is the future promotion boundary under `constellation_2/common/advisor_bridge/`.

## Canonical Inputs

- exactly one parent `PortfolioIntent`
- execution mode
- account scope
- approval requirements
- freshness bounds
- operational readiness inputs

## Canonical Outputs

One immutable `PromotionRecord` carrying at minimum:

- `record_id`
- `promotion_record_id`
- `parent_portfolio_intent_id`
- `contract_version`
- `promoted_at`
- `eligible_scope`
- `blocked_scope`
- `reason_codes`
- `approval_status`
- `freshness_bound`
- `validity_tier`
- `trading_lineage_link`

## Allowed Writers

- the governed promotion boundary only

## Forbidden Writers

- recommendation producers
- UI/read-model writers
- broker adapters
- trading execution writers
- legacy fragmented promotion writers acting as final authority

## Invariants

- exactly one parent `PortfolioIntent`
- blocked scope must remain explicit
- promotion does not mutate portfolio intent
- promotion does not generate broker actions directly
- duplicate promotion must be detectable and rejectable
- this is the only authority surface for advisory-to-trading promotion under the new model
- `approved_delta` must be canonicalized before authorization identity is derived
- `approved_delta` must use deterministic ordering
- `approved_delta` must not contain duplicates
- `approved_delta` must not contain zero-effect entries
- every `approved_delta` entry must carry explicit side and quantity
- stable canonical serialization must be applied before hashing or idempotency derivation

## Failure States

- stale portfolio intent
- missing approval
- account-scope conflict
- execution mode disallows promotion
- duplicate promotion risk
- operational readiness failure

Failure handling:

- write a blocked or rejected `PromotionRecord`
- do not create trading lineage when promotion validity fails
- do not allow `ExecutionIntent` derivation when `approved_delta` is missing, ambiguous, or non-canonical

## Validity Model

Allowed validity tiers:

- `VALID_EXECUTION_ELIGIBLE`
- `VALID_ADVISORY_ONLY`
- `INVALID_REMEDIABLE`
- `INVALID_HARD_STOP`

## Lineage Requirements

Every record must carry:

- `record_id`
- `parent_record_id`
- `contract_version`
- `timestamp_basis`
- `household_id`
- `account_scope`
- `actor_or_source`
- `validity_tier`
- `approval_refs`
- `freshness_attestation_refs`
- `trading_lineage_link` when promotion succeeded

## Replay Expectations

- replay from the same parent portfolio intent and the same promotion boundary rules must reproduce the same promotion result
- replay from the same canonical `approved_delta` must reproduce the same promotion authorization identity

## Projection Relationship

- promotion dashboards, readiness tiles, and operator views are derived only

## Operator Intervention Rules

- manual approval or denial must be recorded as explicit approval or denial artifacts with lineage
- manual intervention must not rewrite a prior promotion record

## Legacy Surface Demotion

The following legacy surfaces are not authoritative promotion records under this contract set:

- `advisor_trade_translation_v1`
- `advisor_trade_intent_proposal_v1`
- `promotion_candidate_v1`
- `promotion_review_v1`
- `promotion_manual_review_v1`
- `promotion_gate_result_v1`

They may remain temporarily only as migration or compatibility surfaces until runtime cutover.

## Pre-Execution Hardening Note

Before advisory-origin execution implementation proceeds:

- `ExecutionIntent` must be defined as a pure transformation of `PromotionRecord.approved_delta`
- `PromotionRecord.idempotency_key` must remain the upstream idempotency anchor for advisory-origin execution
- no advisory-origin executable consequence may be derived from non-canonical or partially-specified approved delta content
