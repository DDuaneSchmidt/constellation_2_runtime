# Advisory Kernel Execution Contract

## Purpose

Define the execution-facing rules for the narrow advisory kernel.

This contract freezes:

- `PromotionRecord` as the sole authorization boundary
- `ExecutionIntent` as the only advisory-produced execution artifact
- reuse of the existing paper-trading pipeline
- prohibition on recommendation-led execution authority

## Canonical execution rule

Advisory-origin execution may proceed only through:

`PortfolioIntent -> PromotionDecision -> PromotionRecord -> ExecutionIntent -> existing paper-trading intake`

No other advisory artifact may imply execution eligibility.

For governed multi-delta rebalance sets, the canonical extension is:

`PortfolioIntent -> PromotionDecision -> PromotionRecord -> ApprovedChangeSet -> MultiDeltaExecutionDecision -> MultiDeltaExecutionRecord -> ExecutionSetIntent -> ordered member ExecutionIntentV1 -> existing paper-trading intake`

No other advisory or bridge artifact may imply multi-trade execution eligibility.

## Sole authorization boundary

`PromotionRecord` is the only advisory authority allowed to authorize movement into execution.

No advisory-origin `ExecutionIntent` may exist without exactly one executable `PromotionRecord`.

Blocked, stale, rejected, superseded, or duplicate promotion states must fail closed.

## ExecutionIntent boundary

`ExecutionIntent` must:

- be derived only from one `PromotionRecord`
- be a pure transformation of `PromotionRecord.approved_delta`
- preserve lineage back to `PromotionRecord`
- use deterministic content and identity rules
- match the proven paper-trading intake contract as closely as possible
- avoid recommendation semantics
- avoid advisory target-state authority ownership

`ExecutionIntent` must not:

- act as portfolio-target authority
- bypass `PromotionRecord`
- redesign broker or submit-boundary ownership
- create a parallel paper-trading path
- recompute target state from `PortfolioIntent`
- round or coerce executable quantities or prices
- read live advisory state at build time

## Downstream reuse rule

The existing paper-trading pipeline is the downstream owner.

The normative downstream path already proven in repo is:

- sealed `execution_package.v1.json`

The legacy compatibility path is:

- raw Phase C candidate directory only when explicitly enabled by the existing paper submit boundary

Any advisory-to-paper adapter must be the minimum boundary adaptation needed to feed the existing path. It may not redesign submit-boundary semantics.

## Choke-point enforcement rule

The paper submit boundary must mechanically reject advisory-origin submission unless all of the following are present and consistent:

- a sealed `execution_package.v1.json`
- package advisory provenance stamped as:
  - `origin = advisory_kernel_v1`
  - `execution_intent_id`
  - `execution_intent_canonical_hash`
  - `promotion_record_id`
  - `promotion_idempotency_key`
- candidate `execution_identity_record.v1.json` `source_refs` carrying the same advisory provenance values
- `execution_package.intent_id`, candidate `execution_identity_record.v1.json.intent_id`, and candidate `equity_order_plan.v2.json.source_intent_id` all matching the stamped `execution_intent_id`
- candidate `equity_order_plan.v2.json.intent_hash` matching `promotion_idempotency_key`

Raw-candidate submit is forbidden for advisory-origin identity sets, even when legacy raw-candidate mode remains available for non-advisory compatibility.

The submit boundary must fail closed on:

- advisory-origin candidate without sealed execution package
- sealed package with missing advisory provenance
- advisory provenance present in the package but missing from the candidate identity set
- any mismatch between package provenance and candidate provenance
- any mismatch between advisory provenance and downstream execution identity fields

## Candidate directory exactness rule

When advisory-origin execution is adapted into the existing paper-trading path, the staged candidate identity set must match Phase D expectations exactly.

The canonical candidate directory contents are:

- `equity_order_plan.v2.json`
- `mapping_ledger_record.v2.json`
- `binding_record.v2.json`
- `submit_preflight_decision.v1.json`
- `execution_identity_record.v1.json`
- sibling `attempt_state.v1.json` in the parent `attempt_<ID>/` directory

No advisory adapter may omit, rename, or substitute these artifacts when using the existing paper-trading intake path.

Phase C proposal families are not a valid substitute for this kernel path.

## Execution package adapter rule

`ExecutionPackageBuilderFromExecutionIntent` is the only permitted future adapter shape for advisory-origin execution handoff.

It must:

- take exactly one `ExecutionIntent`
- materialize the exact candidate identity set required by the existing execution package builder and submit boundary
- invoke the existing governed execution-package sealing path
- preserve lineage back to `PromotionRecord`
- remain deterministic and replay-safe

It must not:

- bypass `execution_package.v1.json`
- bypass the existing execution build authority
- reinterpret policy, target state, or promotion semantics
- read live advisory state during package construction

## Field-binding rule

`ExecutionIntent` must contain only the executable payload required to stage the existing candidate identity set and execution package.

It must include at minimum:

- instrument identifiers
- side
- explicit quantity
- account routing only when already proven in repo
- lineage reference to `PromotionRecord`
- idempotency key

It must not include:

- narratives
- investor-intent or policy blobs
- portfolio construction metadata
- recommendation text

## Idempotency alignment rule

Idempotency must align across the advisory-to-execution boundary:

- `PromotionRecord.idempotency_key`
- `ExecutionIntent.idempotency_key`
- downstream `submission_id` derivation

No independent advisory-side execution id generation is allowed outside that lineage.

Derived downstream identifiers may use existing governed execution identity utilities, but they must remain a deterministic function of the upstream authorized payload.

For multi-delta execution sets, set-level idempotency must be preserved through:

- `ApprovedChangeSet.approved_change_set_id`
- `MultiDeltaExecutionRecord.multi_delta_execution_record_id`
- `ExecutionSetIntent.execution_set_intent_id`
- ordered member `ExecutionIntentV1.execution_intent_id`
- downstream per-trade `submission_id`

Per-trade execution intent identities must be deterministic member derivations of the set-level authority. They may not be ad hoc free-standing ids.

## Recommendation-led prohibition

The following are forbidden from emitting advisory-origin executable intent under this contract:

- `official_recommendation_set_v1`
- recommendation-led `decision_plan_v1`
- `advisor_trade_translation_v1`
- `advisor_trade_intent_proposal_v1`
- legacy `promotion_candidate_v1`
- legacy `promotion_review_v1`
- legacy `promotion_manual_review_v1`
- legacy `promotion_gate_result_v1`

These may remain only as non-authoritative compatibility or explanatory surfaces.

## Fail-closed execution rule

If any of the following are true:

- `PortfolioIntent` invalid
- `PromotionDecision` blocked or no_action
- `PromotionRecord` not executable
- advisory lineage incomplete
- `ExecutionIntent` cannot be derived exactly from `approved_delta`
- candidate identity set cannot be constructed exactly
- `execution_package.v1.json` cannot be sealed with `closure_status=COMPLETE`
- downstream intake contract incompatible
- duplicate authorization or handoff detected
- multi-delta member set cannot be normalized into one coherent governed set
- multi-delta set-level authority artifacts cannot be created exactly

then:

- no `ExecutionIntent` may be emitted
- no `ExecutionSetIntent` may be emitted
- no paper-trading handoff may be attempted
- `KernelRunEnvelope` must record the blocked state

## Deterministic lineage rule

All advisory-origin execution artifacts must preserve deterministic lineage across:

- `PortfolioIntent`
- `PromotionDecision`
- `PromotionRecord`
- `ExecutionIntent`
- `execution_package.v1.json`
- downstream handoff outcome

The same approved promotion input must produce the same `ExecutionIntent`.

## Kernel envelope requirement

Every advisory kernel execution attempt must emit `KernelRunEnvelope`, including:

- upstream artifact refs
- stage validation results
- blocked or no_action outcomes
- `PromotionRecord` ref if created
- `ExecutionIntent` ref if created
- `execution_package.v1.json` ref if created
- downstream handoff status if attempted
