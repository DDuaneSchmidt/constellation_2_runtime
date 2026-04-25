---
title: Meta Governance Tiers And Invariants
status: active
owner: constellation
authority: canonical
doc_type: contract
---

# Meta Governance Tiers And Invariants

## Tier Meanings

### Tier 0 — Constitutional Invariants

Near-immutable hard boundaries for system legitimacy and replayability.

Examples:

- no governed runtime behavior change without an activation snapshot
- no critical autonomy expansion without explicit human approval
- no mutation without lineage
- no weakening of auditability guarantees
- no activation with unresolved dependencies
- no activation with missing interpreter version

### Tier 1 — Governance Protocols

Rules that define how lower tiers may mutate.

Examples:

- approval requirements
- blast-radius classification rules
- predicate families
- promotion gates
- rollback rules
- review windows
- emergency mutation protocol

### Tier 2 — Policy Modules

Bounded business decision modules.

Examples:

- risk policy
- tax policy
- allocation policy
- lifecycle policy
- advisory escalation policy

### Tier 3 — Tunable Parameters

Bounded parameter groups under Tier 2 modules.

Examples:

- concentration limits
- drift bands
- stop multipliers
- tax thresholds

## Non-Weakenable Invariants

- no governed runtime behavior may change without an `ActivationSnapshot`
- runtime consumers must use the compiled `CanonicalGovernanceGraph`, not raw mutable source files
- no critical autonomy broadening without explicit human approval
- no mutation without lineage
- no weakening of auditability guarantees
- no bypass of approval derivation from computed blast radius
- no activation with unresolved dependencies
- no activation with missing interpreter version
- no activation with missing rollback-ready predecessor
- no rollback may rewrite historical artifacts

## Mutation Objects

Every mutation must be represented through explicit immutable records:

- proposal
- deterministic diff
- blast-radius assessment
- predicate evaluations
- approval decision
- compiled graph
- activation snapshot
- lineage records
- audit events

## Interpreter-Version Pinning

Governance semantics are pinned to one canonical interpreter identity.

The interpreter identity must be embedded in:

- module registration
- graph compilation
- activation snapshots
- rollback validation

Any missing or inconsistent interpreter identity is activation-blocking.
