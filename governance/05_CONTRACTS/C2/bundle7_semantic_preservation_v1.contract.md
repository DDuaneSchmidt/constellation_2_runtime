---
id: C2_BUNDLE7_SEMANTIC_PRESERVATION_V1
title: "C2 Bundle 7 Semantic Preservation Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_bundle7_semantic_preservation
---

# C2 Bundle 7 Semantic Preservation Contract v1

## Purpose

Ratify that Bundle 7 is a baseline-closure and validator-expansion bundle only.

## Bundle 3 preservation

Bundle 7 MUST preserve:

- configuration activation family semantics
- `compiled_active_config_v1` as the only runtime-consumable configuration artifact
- `configuration_state_v1/current.json` as the only published current-state artifact in that family

## Bundle 4 preservation

Bundle 7 MUST preserve:

- stage-admission and certification semantics
- startup-chain family validation and boundary-validation meaning

## Bundle 5 preservation

Bundle 7 MUST preserve:

- transition-engine evaluation semantics
- blocked/admitted/certified/recompute/supersession meaning in `control_stage_transition_record_v1`

## Bundle 6 preservation

Bundle 7 MUST preserve:

- trust-plane projection semantics
- authority-label, freshness, governing-ref, and semantic-event meaning already ratified for Bundle 6

## Prohibited changes

Bundle 7 MUST NOT:

- redesign advisory semantics
- redesign control-plane semantics
- change artifact classes for Bundle 3-6 families
- weaken fail-closed behavior on existing Bundle 3-6 authority paths
