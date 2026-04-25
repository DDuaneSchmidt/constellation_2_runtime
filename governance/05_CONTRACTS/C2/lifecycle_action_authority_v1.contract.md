---
id: C2_LIFECYCLE_ACTION_AUTHORITY_CONTRACT_V1
title: "C2 Lifecycle Action Authority Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_action_eligibility
---

# C2 Lifecycle Action Authority Contract V1

## Canonical purpose

Core 3 is the governed post-entry action-eligibility layer.

Core 3 answers only:

- whether autonomous post-entry action evaluation is permitted
- what governed candidate actions exist
- what one canonical final action posture exists for a trade identity
- why the result is explainable, replayable, and fail-closed

## Canonical owner

The one external canonical Core 3 truth owner is:

- `lifecycle_action_authority.v1`

There is one canonical Core 3 action-authority artifact per governed `trade_identity_id` per Core 2 materialization boundary.

## Permanent internal layers

### Layer A — Global Actionability Gate

Internal artifact:

- `global_actionability_gate.v1`

This layer runs first and answers only:

- is autonomous post-entry action evaluation permitted at all right now

It may resolve only:

- `ACTIONABLE`
- `DEGRADED_REVIEW_REQUIRED`
- `BLOCKED`

This layer must not decide a final action choice.

### Layer B — Candidate Action Generator

Internal artifact:

- `candidate_action_set.v1`

This layer nominates a small governed candidate set from governed Core 2 truth plus governed Core 3 policy basis.

It must not become the final owner of action truth.

### Layer C — Canonical Lifecycle Action Authority

Canonical owner:

- `lifecycle_action_authority.v1`

This layer resolves candidate actions into one canonical action-authority artifact per trade identity with explicit:

- final action posture
- allowed actions
- required actions
- forbidden actions
- blocked actions
- action safety posture
- first blocker
- ambiguity state
- upstream refs
- policy refs
- rule version

No contradictory final posture is allowed.

### Layer D — Decision Provenance

Derived audit artifact:

- `action_decision_provenance.v1`

This layer records:

- which Core 2 fields were read
- which policy rules nominated candidate actions
- which blockers fired
- which conflict rule resolved the final posture
- why an action became required instead of merely allowed
- why review was required instead of autonomous action
- why an action became forbidden or blocked

### Layer E — Derived Operator Surface

Derived-only artifact:

- `lifecycle_action_operator_surface.v1`

This layer may expose:

- per-trade action dossiers
- blocked-action index
- required-action index
- review-required index
- close-eligible index
- protection-required index
- aggregate Core 3 health summary

These are not truth owners.

## Scope

Core 3 owns only governed post-entry action eligibility derived from governed Core 2 truth plus governed Core 3 policy.

Core 3 is in scope for:

- autonomous-action gate posture
- governed candidate actions
- canonical action eligibility truth
- deterministic conflict resolution
- explicit decision provenance
- derived operator read models

## Non-scope

Core 3 does not own:

- raw broker evidence
- broker fact normalization
- reconciled trade truth
- broker submit authority
- operator truth ownership
- workflow truth ownership
- strategy-alpha selection
- scheduler behavior
- broker transmission

Core 3 must not re-read Core 1 raw events directly.

## Canonical runtime families

Core 3 writes under the governed sleeve execution root:

- `truth_sleeves/<sleeve_id>/<mode>/lifecycle_action_authority_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/global_actionability_gate.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/lifecycle_action_authority_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/candidate_action_set.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/lifecycle_action_authority_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/lifecycle_action_authority.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/lifecycle_action_authority_v1/materializations/<DAY>/<materialization_set_id>/trades/<trade_identity_id>/action_decision_provenance.v1.json`
- `truth_sleeves/<sleeve_id>/<mode>/reports/lifecycle_action_operator_surface_v1/<DAY>/<materialization_set_id>/lifecycle_action_operator_surface.v1.json`

## Fail-closed behavior

Core 3 must fail closed when relied-on Core 2 truth is stale, ambiguous, foreign/manual, blocked, or otherwise unsafe for autonomous post-entry action evaluation.

If the global gate is `BLOCKED`, later stages must not publish rich autonomous action posture.

If the global gate is `DEGRADED_REVIEW_REQUIRED`, later stages may publish only review/hold-safe posture and must not escalate to autonomous modifying actions.

## Hard invariants

1. `lifecycle_action_authority.v1` is the one external canonical owner of post-entry action eligibility.
2. Global Actionability Gate runs before candidate generation and final resolution.
3. Core 3 derives only from governed Core 2 truth and governed Core 3 policy.
4. Allowed, required, forbidden, and blocked action semantics remain explicit.
5. Review-required semantics remain explicit and separate from autonomous action eligibility.
6. No contradictory final posture may be materialized for one trade identity.
7. Core 3 must never transmit broker instructions.
8. Identical Core 2 inputs plus identical Core 3 rule versions must produce identical Core 3 outputs.

## Proof basis

- `governance/05_CONTRACTS/C2/reconciled_trade_state_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciliation_health_v1.contract.md`
- `governance/05_CONTRACTS/C2/reconciled_trade_description_v1.contract.md`
- `governance/05_CONTRACTS/C2/trade_identity_resolution_v1.contract.md`
