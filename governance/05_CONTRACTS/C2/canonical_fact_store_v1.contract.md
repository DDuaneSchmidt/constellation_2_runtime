---
doc_kind: contract
doc_id: C2_CANONICAL_FACT_STORE_V1
title: "Constellation 2.0 Canonical Fact Store V1"
version: 1
status: DRAFT
created_utc: 2026-04-07T00:00:00Z
repo_root_authoritative: /home/node/constellation
scope:
  - "canonical fact classes for current paper-day critical path"
  - "single-writer fact authority"
  - "lineage envelope authority from intent onward"
---

# Purpose

This contract defines the minimum canonical fact classes required to remove drift-prone duplicated truth from the current critical path.

# Canonical fact classes

The authoritative fact families introduced by this contract are:

- `market_close_fact.v1`
- `positions_snapshot_fact.v1`
- `intent_fact.v1`
- `lineage_envelope.v1`
- `risk_policy_fact.v1`
- `submission_decision_fact.v1`

# Single-writer rule

Each fact class MUST have exactly one writer boundary on the current critical path.

- market close fact: same-day market close consumer boundary
- positions snapshot fact: positions snapshot producer boundary
- intent fact: engine intent producer boundary
- lineage envelope: first post-intent plan boundary
- risk policy fact: governed policy loader boundary
- submission decision fact: preflight / submission decision boundary

Fail-closed: if multiple active writers exist for the same fact class and scope, the attempt MUST fail.

# Canonical lineage rule

`lineage_envelope.v1` is the canonical lineage authority from order-plan onward.

Downstream artifacts MAY embed lineage fields for compatibility, but embedded fields MUST be derived from the envelope and MUST NOT be treated as a separate authority source.

# Fact authority posture

Facts are authoritative immutable inputs to invariant evaluation.

Derived reports, manifests, and compatibility outputs MUST reference canonical facts and MUST NOT override them.
