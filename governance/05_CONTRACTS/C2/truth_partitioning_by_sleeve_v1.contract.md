---
id: C2_TRUTH_PARTITIONING_BY_SLEEVE_CONTRACT_V1
title: "C2 Truth Partitioning by Sleeve Contract"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Truth Partitioning by Sleeve Contract

## Purpose

This contract defines the canonical truth partitioning model for multi-sleeve operation.

The primary goal is to eliminate cross-sleeve contamination and preserve deterministic replay and audit semantics.

## Canonical sleeve truth root

All sleeve writes MUST be scoped to:

- `constellation_2/runtime/truth_sleeves/<sleeve_id>/<mode>/...`

Where `<mode>` is exactly `PAPER` or `LIVE`.

## Canonical global rollup root (derived only)

Global aggregate outputs MAY be written to the canonical truth root only under:

- `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<day>/...`

This rollup MUST be derived from sleeve partition pointer heads (read-only) and MUST NOT contain sleeve-private mutable state.

## Invariants

### C2_NO_CROSS_PARTITION_WRITES_V1

1. **Isolation**
   - A sleeve MUST NOT write outside its `truth_sleeves/<sleeve_id>/<mode>` root.

2. **No global writes by sleeves**
   - Sleeve pipelines MUST NOT write into `constellation_2/runtime/truth/...` except via a dedicated governed rollup producer tool.

3. **Pointer indices are sleeve-scoped**
   - Latest-pointer indices and canonical pointer indices MUST live under the sleeve partition and MUST NOT be shared across sleeves.

4. **Fail-closed enforcement**
   - Any detected write outside the sleeve root is a safety breach → ABORT.

## Required evidence surfaces (minimum)

- For each sleeve day:
  - pointer head file path (inside sleeve partition)
  - target artifact path (inside sleeve partition)
  - sha256 of target artifact

- For global rollup day:
  - list of sleeve heads consumed (paths + sha256)
  - rollup artifact path + sha256

## Non-claims

- This contract does not specify the content schemas of all artifacts.
- This contract defines directory authority and isolation semantics only.
