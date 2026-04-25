---
title: Meta Governance Overview
status: active
owner: constellation
authority: canonical
doc_type: contract
---

# Meta Governance Overview

## Purpose

Define the Meta-Governance Layer as the deterministic mutation-control system for Constellation.

## Runtime Authority Rule

Governed runtime behavior must not reason directly from raw mutable YAML or other mutable source files at runtime.

Runtime authority is:

1. the compiled `CanonicalGovernanceGraph`
2. the active `ActivationSnapshot`

Every governed runtime decision must bind the active `snapshot_id`.

## Tier Model

- `tier_0`: Constitutional Invariants
- `tier_1`: Governance Protocols
- `tier_2`: Policy Modules
- `tier_3`: Tunable Parameters

Tier boundaries are hard governance boundaries, not descriptive labels.

## Canonical Mutation Objects

- `GovernanceModuleRef`
- `ParameterGroupRef`
- `MutationProposal`
- `ProposalDiff`
- `BlastRadiusAssessment`
- `PredicateEvaluation`
- `ApprovalDecision`
- `CanonicalGovernanceGraph`
- `ActivationSnapshot`
- `RuntimeDecisionLineage`

## Lifecycle Model

Mutable governance artifacts must use explicit validated lifecycle states:

- `draft`
- `proposed`
- `validated`
- `approved`
- `shadow`
- `pilot`
- `active`
- `review_due`
- `deprecated`
- `superseded`
- `archived`
- `blocked`
- `rolled_back`

Silent state mutation is forbidden.

## Determinism Requirements

- canonical serialization with deterministic field ordering
- explicit schema versions
- immutable write-once historical artifacts
- deterministic graph compilation
- stable content hashes
- explicit interpreter-version pinning
- replay-grade lineage and auditability

## Enforcement Requirement

This layer must be enforced in code, not only documented in prose.
