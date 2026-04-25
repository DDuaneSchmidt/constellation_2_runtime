---
id: C2_SLEEVE_EDGE_FACT_LEDGER_V1
title: "C2 Sleeve Edge Fact Ledger Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_control_stack
---

# C2 Sleeve Edge Fact Ledger Contract v1

## Purpose

Define the canonical fact-only ledger for sleeve edge measurement.

This ledger is immutable and auditable.

It does not score, throttle, or allocate capital.

## Canonical writer

- `ops/tools/run_sleeve_edge_measurement_v1.py`
- implementation owner: `constellation_2/common/sleeve_edge_measurement_v1.py`

## Canonical artifact path

- `truth_sleeves/<execution_sleeve>/<mode>/reports/sleeve_edge_fact_ledger_v1/<DAY_UTC>/<logical_sleeve_id>/<snapshot_id>/sleeve_edge_fact_ledger.v1.json`

## Required fields

- `sleeve_id`
- `strategy_family`
- `source_execution_sleeve_id`
- `core2_materialization_set_id`
- `engine_ids`
- `fact_input_hash`
- trade fact rows with:
  - `trade_identity_id`
  - `measurement_class`
  - `ownership_classification`
  - lineage references
  - source artifact references
  - fill-count and fee facts
  - inclusion/exclusion facts

## Ownership boundary

- Source trade truth must anchor to `reconciled_trade_state_v1`, `trade_identity_resolution_v1`, and `reconciliation_provenance_v1`.
- Imported or adopted handling must remain anchored to reconciled trade ownership classification.
- The ledger may derive `measurement_class`, but it must not derive allocator actions.
- Allowed `measurement_class` values are `NATIVE_ENTRY`, `ADOPTED_POSITION_MANAGEMENT`, and `UNKNOWN_ATTRIBUTION`.
- `UNKNOWN_ATTRIBUTION` must never be coerced into native or adopted metrics.

## Prohibited couplings

- no confidence multiplier
- no readiness score
- no allocation merit score
- no regime-conditioned control logic
- no capital allocation decisions

## Revision rules

- published fact ledgers are immutable
- corrections must materialize a new snapshot id
- silent mutation is forbidden

## Fail-closed rules

- missing required source artifact references must fail closed
- ambiguous native engine attribution must fail closed
- unknown attribution must be excluded explicitly and must invalidate qualification when required inputs are no longer complete
- unsupported measurement classes must not be coerced

## Audit expectations

- every included or excluded trade must be explicitly listed
- engine-attribution evidence must be bound by artifact path and sha256
- `fact_input_hash` must reproduce the exact fact set used

## Versioning rule

The field set and meaning of `measurement_class`, `fact_input_hash`, and trade inclusion semantics must not change without an explicit contract version bump.
