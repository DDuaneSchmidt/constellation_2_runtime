---
id: C2_TRADE_IDENTITY_RESOLUTION_CONTRACT_V1
title: "C2 Trade Identity Resolution Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_trade_identity
---

# C2 Trade Identity Resolution Contract V1

## Canonical owner

- `trade_identity.v1`

## Purpose

Trade Identity Authority is the governed answer to:

- which observed broker facts belong to the same trade-state object

## Governing model

Trade identity resolution is deterministic from:

- Core 1 canonical fact ledgers
- Core 1 broker observation health
- governed execution identity binding
- governed sleeve/account topology
- governed identity rules in this contract

## Identity basis

Canonical identity resolution must bind facts using:

- environment
- sleeve id
- account id when available
- instrument identity from governed broker fact content
- open/close continuity segment
- broker lineage references such as order id, perm id, and execution id when present

The canonical `trade_identity_id` must remain stable for identical inputs and identical rules.

## Ownership classification model

Every trade identity record must resolve to exactly one ownership classification:

- `CONSTELLATION_OWNED`
- `FOREIGN_MANUAL`
- `AMBIGUOUS_OWNERSHIP`
- `INSUFFICIENT_EVIDENCE`

Classification rules:

1. `CONSTELLATION_OWNED`
   - attributed Core 1 facts align to the governed sleeve/account boundary
   - no contradictory foreign/manual evidence remains unresolved
2. `FOREIGN_MANUAL`
   - evidence is explicitly outside the governed sleeve/account boundary
   - or Core 1 marks foreign/manual suspicion without contradictory governed ownership evidence
3. `AMBIGUOUS_OWNERSHIP`
   - mixed governed and foreign/manual evidence
   - conflicting account or sleeve identity
   - unresolved attribution ambiguity
4. `INSUFFICIENT_EVIDENCE`
   - trade grouping exists but required account/instrument/lineage proof is not strong enough for a stronger classification

## Lineage attachment rules

Trade Identity Authority must attach broker lineage references deterministically:

- order ids and perm ids bind order and order-status observations
- execution ids bind fill observations
- position facts attach by governed account plus instrument identity
- fact-record ids provide the complete attachment ledger

If lineage cannot be attached without ambiguity, identity resolution must emit explicit blocker codes instead of guessing.

## Open/close continuity rules

Facts for the same environment, sleeve, account, and instrument belong to the same continuity segment until a governed closure boundary is observed.

Closure boundaries include:

- a flat position observation for that governed identity/instrument
- a net fill boundary that deterministically returns the segment to flat

New post-flat activity for the same governed identity/instrument starts a new continuity segment and therefore a new `trade_identity_id`.

Direct reversal without a safely provable flat boundary is ambiguous and must block identity certainty.

## Foreign/manual boundary

Trade Identity Authority must preserve the boundary between:

- governed Constellation-owned activity
- foreign/manual suspected activity

Foreign/manual suspected activity may be materialized as observed trade-state records, but those records must never be silently reclassified as Constellation-owned.

## Ambiguity handling

At minimum, identity resolution must explicitly classify and preserve:

- trade identity unresolved
- ownership ambiguous
- account mismatch
- instrument identity unresolved
- lineage unresolved
- mixed ownership evidence

Ambiguous identity must fail closed for downstream action reliance.

