---
id: C2_SLEEVE_EDGE_SNAPSHOT_V1
title: "C2 Sleeve Edge Snapshot Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-15
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_control_stack
---

# C2 Sleeve Edge Snapshot Contract v1

## Purpose

Define the frozen publication snapshot for sleeve measurement and qualification.

## Canonical artifact path

- `truth_sleeves/<execution_sleeve>/<mode>/reports/sleeve_edge_snapshot_v1/<DAY_UTC>/<logical_sleeve_id>/<snapshot_id>/sleeve_edge_snapshot.v1.json`

## Required contents

- `sleeve_id`
- `as_of_ts`
- `metric_window`
- `included_trade_ids`
- `excluded_trade_ids`
- `exclusion_details`
- `fact_input_hash`
- `calculation_version`
- `policy_version`
- factual metrics
- `unavailable_metrics`
- qualification outputs
- `reason_codes`
- snapshot lineage

`fact_input_hash` is the exact reproducibility binding for the measured fact input set used to derive factual metrics and qualification. It must be recomputable from the bound fact ledger's summary manifest plus per-trade fact references.

## Required revision lineage fields

- `snapshot_lineage.previous_snapshot_id`
- `snapshot_lineage.revision_type`
- `snapshot_lineage.revision_reason`

Allowed `revision_type` values:

- `INITIAL_PUBLISH`
- `DATA_CORRECTION`
- `POLICY_CHANGE`
- `RECLASSIFICATION`
- `BACKFILL`

## Historical read rule

Historical reads must use published frozen snapshots.

They must not silently recompute from current code or current facts.

## Integrity read rule

- allocator-facing reads must validate snapshot schema completeness before use
- allocator-facing reads must validate the bound fact-ledger reference exists and matches the recorded sha256
- allocator-facing reads must validate `fact_input_hash` is present on the snapshot, present on the bound fact ledger, and matches the recomputed exact fact-input descriptor from the bound fact ledger
- malformed, incomplete, or sha-mismatched snapshot artifacts must fail closed
- a structurally invalid latest snapshot must not be silently skipped in favor of a different snapshot

## Publication rule

- snapshot publication must remain atomic
- the canonical writer may satisfy this with a same-directory temp write plus atomic replace pattern
- control consumers must still fail closed if the artifact is absent or malformed

## Revision rules

- immutable once published
- restatement must publish a new snapshot id
- prior snapshot lineage must remain visible
- initial publishes must use `INITIAL_PUBLISH`
- revised publishes must link the immediately prior snapshot in the lineage
- lineage must restart instead of linking backward when `policy_version` changes
- lineage must restart instead of linking backward when `calculation_version` changes incompatibly

## Audit expectations

- snapshot lineage must explain state transitions
- every state change must be reproducible from bound facts, version ids, and policy version
- exact fact-input binding must remain visible through `fact_input_hash` and the immutable fact-ledger reference
