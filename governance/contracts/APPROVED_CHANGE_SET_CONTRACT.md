# Approved Change-Set Contract

## Purpose

Freeze the smallest authoritative contract for a coherent approved multi-change rebalance input.

## Required source authority

`ApprovedChangeSet` may be created only from:

- one valid `PromotionRecordV2`
- the exact upstream `PromotionDecisionV1`
- the exact upstream `PortfolioIntentV1`
- the governed value basis frozen in the linked `HouseholdSnapshot`

## Contract meaning

`ApprovedChangeSet` answers:

- what exact tradable changes belong to this approved rebalance set
- on what governed value basis and advisory lineage those changes were approved
- in what deterministic order the changes must be evaluated downstream

## Required fields

At minimum the contract must carry:

- stable `approved_change_set_id`
- upstream `portfolio_intent_id`
- upstream `promotion_decision_id`
- upstream `promotion_record_id`
- upstream `value_basis_id`
- ordered tradable change list
- per-change deterministic change identity
- reason codes
- parent lineage refs
- source artifact refs

## Ordered tradable change rule

Each member change must be fully specified and deterministic.

At minimum a member must carry:

- change id
- symbol
- asset type
- currency
- side
- quantity shares
- reference price
- routing account id when required by current repo structures

## Validation rule

`ApprovedChangeSet` must fail closed on:

- empty or incomplete approved changes when outcome requires execution
- unsupported instrument kinds
- missing reference prices
- zero-sized trades
- ambiguous ordering keys
- missing governed value-basis linkage

## Identity rule

The same frozen upstream artifacts and the same ordered tradable changes must produce the same `approved_change_set_id`.

Any change to:

- ordering
- member set
- sizing
- governed value basis
- upstream promotion authority lineage

must change the identity deterministically.

## Out of scope

This contract does not add:

- portfolio optimization
- alternative trade selection
- tax-aware substitutions
- live market reads
