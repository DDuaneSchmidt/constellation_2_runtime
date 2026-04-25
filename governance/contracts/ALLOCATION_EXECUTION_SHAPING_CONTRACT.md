# Allocation Execution Shaping Contract

Contract ID: `ADVISORY_ALLOCATION_EXECUTION_SHAPING_CONTRACT_V1`

## Purpose

Define the last-step translation from actionable allocation advice into execution-shaped deltas without changing the existing execution authority chain.

## Scope

This contract governs only the deterministic shaping of already-actionable allocation advice into the existing advisory-to-execution bridge.

## Required Authority Chain

Allocation-origin execution may proceed only through:

`InvestorIntent -> Policy -> HouseholdSnapshot -> PortfolioIntent -> PromotionDecision -> PromotionRecordV2 -> ExecutionIntent -> execution_package.v1 -> submit_boundary_paper_v4`

No parallel execution pathway is permitted.

## Preconditions

Execution shaping may begin only when all of the following are true:

- governed value basis is valid
- allocation advice is present and value-based
- actionability outcome is `promote`
- lineage across policy, snapshot, portfolio intent, and decision is intact

## Translation Rule

Execution shaping must:

- reuse `PromotionRecordV2`
- reuse `ExecutionIntent`
- reuse the existing execution package builder and submit boundary
- apply explicit deterministic sizing and rounding rules
- fail closed if executable deltas cannot be derived safely

## Prohibitions

Execution shaping must not:

- weaken `PromotionRecordV2` authority
- bypass `ExecutionIntent`
- bypass `execution_package.v1`
- bypass `submit_boundary_paper_v4`
- read live advisory state after the frozen upstream artifacts are created
- introduce new execution pathways

## Determinism Rule

Same frozen inputs and same shaping rules must yield the same:

- approved delta
- `PromotionRecordV2` authorization scope
- `ExecutionIntent`
- downstream execution package identity

## Non-Scope

This contract does not authorize:

- execution submission kernel redesign
- execution lifecycle kernel redesign
- broker policy redesign
- generalized portfolio optimization
