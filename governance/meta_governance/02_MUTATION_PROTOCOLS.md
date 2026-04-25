---
title: Meta Governance Mutation Protocols
status: active
owner: constellation
authority: canonical
doc_type: contract
---

# Meta Governance Mutation Protocols

## General Protocol

Every mutation must provide:

- explicit proposal
- deterministic diff
- blast-radius assessment
- predicate evaluation results
- approval decision(s)
- rollback-ready predecessor reference before activation

Comparative or observational evidence may supplement review, but it must never override failed required predicates.

## Tier 0 Protocol

- explicit proposal required
- full invariant review required
- human approval required
- no auto-promotion
- mandatory post-activation review
- rollback plan required before activation

## Tier 1 Protocol

- explicit proposal required
- protocol impact analysis required
- human approval required
- no emergency mutation that broadens lower-tier mutation powers

## Tier 2 Protocol

- explicit proposal required
- semantic diff required
- dependency check required
- predicate gate evaluation required
- shadow or pilot path supported where applicable
- approval must be derived from computed blast radius

## Tier 3 Protocol

- bounded parameter mutation only
- strict range validation
- dependency and invariant checks required
- fast path is allowed only when all of the following are true:
  - no Tier 0 or Tier 1 surface is affected
  - no autonomy broadening occurs
  - no approval threshold weakening occurs
  - no audit surface weakening occurs

## Predicate Families

Required predicate families:

- invariant preservation
- dependency closure
- audit completeness
- rollback readiness
- interpreter pinning
- activation snapshot completeness
- approval sufficiency
- no forbidden scope broadening
- bounds validity
- schema validity

Activation is forbidden unless all required predicates pass.

## Approval Derivation Rules

Approval class must be derived from computed blast radius.

Minimum classes:

- `safe`
- `moderate`
- `critical`

Automatic critical cases:

- any Tier 0 change
- any Tier 1 change
- any autonomy broadening
- any audit weakening
- any approval-semantics change
