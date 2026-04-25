---
id: C2_OPERATOR_NARRATIVE_RENDERING_CONTRACT_V1
title: "C2 Operator Narrative Rendering Contract V1"
status: DRAFT
version: 1
created_utc: 2026-04-11
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_post_entry_operator_narrative_rendering
---

# C2 Operator Narrative Rendering Contract V1

## Purpose

Human-readable narrative and timeline outputs are downstream explanatory views only.

## Required derivation order

Narrative and timeline rendering must derive only from:

- `operator_trade_health.v1`
- `operator_summary_provenance.v1`
- lower-core references already recorded by those artifacts

## Hard rules

- no freeform reinterpretation of lower-core truth
- every narrative statement must be traceable to provenance claims and lower-core refs
- timeline entries must derive from artifact diff and prior-state comparison, not inferred causality
- insufficient provenance must degrade narrative output explicitly
- narrative and timeline artifacts must never become canonical truth owners
