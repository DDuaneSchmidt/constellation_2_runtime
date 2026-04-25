---
title: Audit And Lineage
status: active
owner: constellation
authority: canonical
doc_type: contract
---

# Audit And Lineage

## Lineage Requirement

Every live governed decision must be traceable through:

`decision -> snapshot -> graph -> modules/parameters -> proposals -> approvals -> evidence`

## Required Historical Records

The system must preserve immutable historical records for:

- governance modules
- parameter groups
- proposals
- proposal diffs
- evaluation artifacts
- approvals
- activation snapshots
- rollback records
- lineage records
- audit events

## Negative History

The audit surface must also preserve:

- rejected proposals
- blocked lifecycle transitions
- failed predicates
- invalid snapshots
- rollbacks
- emergency freezes

## Rollback Semantics

Rollback means activation of a prior valid snapshot.

Rollback does not mean editing active files back, deleting history, or mutating existing artifacts in place.

## Replay Requirement

The system must retain enough immutable evidence to replay:

- which governance state was active
- which interpreter semantics were in force
- which approvals and predicate results authorized activation
- which governed modules and parameter groups informed a runtime decision
