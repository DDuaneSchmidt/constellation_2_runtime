---
id: C2_RECONCILIATION_PROVENANCE_CONTRACT_V1
title: "C2 Reconciliation Provenance Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_trade_provenance
---

# C2 Reconciliation Provenance Contract V1

## Canonical owner

- `reconciliation_provenance.v1`

## Required provenance

Every Core 2 materialized trade-state object must be explainable by explicit provenance.

At minimum provenance must include:

- incorporated fact references
- ignored fact references and reasons
- blocked fact references and reasons
- prior-state change summary
- rule/version identifiers
- exact upstream Core 1 evidence references used

## Change record requirement

Each materialization must record one of:

- `INITIAL_MATERIALIZATION`
- `NO_MATERIAL_CHANGE`
- `MATERIAL_CHANGE`

For `MATERIAL_CHANGE`, provenance must name the changed top-level fields.

## Rule/version requirement

Provenance must identify:

- Core 2 rule pack version
- identity rule version
- descriptive rule version
- health rule version

## Evidence reference requirement

Provenance must reference the Core 1 evidence actually used, including:

- broker raw journal reference
- fact ledger references
- broker observation health reference

Core 2 provenance must point back to Core 1 evidence. It must not replace Core 1 as evidence authority.

