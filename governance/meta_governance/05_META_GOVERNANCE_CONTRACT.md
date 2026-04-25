---
title: Meta Governance Contract
status: active
owner: constellation
authority: canonical
doc_type: contract
---

# Meta Governance Contract

## Purpose

Define the hard contract for how Constellation may mutate its own governing rules.

## Contract

1. No governed runtime behavior may change without an Activation Snapshot.
2. Runtime consumers must use the compiled Canonical Governance Graph, not raw mutable source files.
3. All governance semantics are tiered:
   - Tier 0: Constitutional Invariants
   - Tier 1: Governance Protocols
   - Tier 2: Policy Modules
   - Tier 3: Tunable Parameters
4. Any Tier 0 or Tier 1 mutation is critical and requires explicit human approval.
5. No mutation may activate unless all required predicates pass.
6. Approval class must be derived from blast radius, not manually asserted as source of truth.
7. Every Activation Snapshot must pin:
   - graph hash
   - interpreter version
   - approval references
   - evaluation references
   - predecessor snapshot
8. Historical governance artifacts are immutable.
9. Rollback must activate a prior valid snapshot rather than rewriting history.
10. Every governed runtime decision must be attributable through lineage:
    decision → snapshot → graph → modules/parameters → proposals → approvals → evidence

## Non-Weakenable Invariants

- No critical autonomy broadening without explicit human approval.
- No weakening of auditability guarantees.
- No bypass of lineage requirements.
- No direct runtime dependence on mutable raw governance files.
- No activation with unresolved dependencies.
- No activation with missing interpreter version.
- No activation with missing rollback-ready predecessor.

## Required Lifecycle States

- draft
- proposed
- validated
- approved
- shadow
- pilot
- active
- review_due
- deprecated
- superseded
- archived
- blocked
- rolled_back

## Required Evidence for Activation

- validated proposal
- deterministic diff
- blast radius assessment
- predicate evaluation results
- approval record(s)
- compiled graph
- rollback-ready predecessor reference

## Enforcement Expectation

This contract must be enforced in code, not only documented in prose.
