# Advisory Kernel Boundary Contract

## Purpose

Define the narrow advisory replacement kernel boundary for Constellation.

The advisory kernel exists to answer exactly four questions and no broader planning scope:

- what should the household hold
- what needs to change
- should the system act now
- what execution-shaped artifact follows from an approved promotion

The governed expansion order for value-based allocation advisory is:

- Value Basis Authority
- Allocation Advisory
- Actionability Gate
- Execution Shaping

Execution shaping is forbidden until the first three layers are explicitly governed and satisfied.

## Canonical authority chain

The only valid advisory-origin execution chain under this contract is:

`InvestorIntent -> Policy -> HouseholdSnapshot -> PortfolioIntent -> PromotionDecision -> PromotionRecord -> ExecutionIntent -> existing paper trading`

Every stage is immutable and lineage-bearing.

## Required authority ownership

- `InvestorIntent` remains the top desire authority.
- `Policy` remains the compiled enforceable advisory authority.
- `HouseholdSnapshot` remains the frozen current-state authority.
- `PortfolioIntent` is the only advisory target-state authority.
- `PromotionDecision` is a pure gate result only.
- `PromotionRecord` is the sole advisory-to-execution authorization authority.
- `ExecutionIntent` is the only advisory-produced execution artifact.
- `KernelRunEnvelope` is the mandatory run-level audit envelope for every kernel run.

## Execution hardening constraints

Before runtime cutover beyond `PortfolioIntent`, the following execution-boundary constraints are binding:

- `ExecutionIntent` must be a pure transformation of `PromotionRecord.approved_delta`
- `PromotionRecord.approved_delta` must be canonicalized before hashing and authorization
- candidate directory staging must match the proven Phase D identity-set contract exactly
- advisory-side execution idempotency must align from `PromotionRecord` through `ExecutionIntent` to downstream submission identity derivation
- `KernelRunEnvelope` must link `PromotionRecord`, `ExecutionIntent`, and `execution_package.v1.json`
- inability to derive `ExecutionIntent`, construct the candidate identity set, or seal `execution_package.v1.json` must fail closed

## Forbidden authority paths

The following may not create advisory-origin executable consequence under this contract:

- `official_recommendation_set_v1`
- recommendation-led `decision_plan_v1`
- `advisor_trade_translation_v1`
- `advisor_trade_intent_proposal_v1`
- any direct legacy promotion input artifact
- any UI read model, memo, dashboard, or explanation artifact

Legacy artifacts may survive only as:

- read-only projection surfaces
- migration compatibility inputs
- historical replay surfaces

## Non-scope

This kernel does not own:

- retirement-planning expansion
- scenario engines
- Monte Carlo
- recommendation narratives
- dashboard or UI redesign
- paper-trading redesign
- live-trading redesign
- broad optimizer frameworks

## Paper-trading boundary rule

The existing paper-trading pipeline remains the downstream execution owner.

The advisory kernel must reuse the proven paper-trading intake boundary and may only add the minimum adapter needed for `ExecutionIntent` compatibility.

Parallel advisory-origin execution paths are forbidden.

## Fail-closed rule

If any stage in the advisory kernel chain is missing, invalid, stale beyond governed limits, lineage-broken, or non-deterministic:

- promotion must not occur
- `PromotionRecord` may not authorize execution
- `ExecutionIntent` may not be emitted
- `KernelRunEnvelope` must still be emitted with the blocked or no-action result

## Determinism rule

Same frozen upstream inputs and same compiler or builder versions must yield the same:

- `PortfolioIntent`
- `PromotionDecision`
- `PromotionRecord` authorization scope
- `ExecutionIntent`
- staged candidate identity set
- `execution_package.v1.json`

No ambient defaults, floating latest-state reads, or recommendation-era heuristics may change kernel meaning.

## Kernel envelope rule

Every kernel run must emit one immutable `KernelRunEnvelope`, including:

- promote
- blocked
- no_action

`KernelRunEnvelope` is observability and audit only. It is not business authority.

## Replay rule

The advisory kernel must be replayable from immutable upstream authority artifacts and must reconstruct the full lineage from:

`InvestorIntent` through `ExecutionIntent` and the downstream paper-trading handoff result.
