---
id: C2_PER_SLEEVE_ORCHESTRATOR_CONTRACT_V1
title: "C2 Per-Sleeve Orchestrator Contract"
status: DRAFT
version: 1
created_utc: 2026-03-04
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Per-Sleeve Orchestrator Contract

## Purpose

This contract requires orchestration to be executed **per sleeve**, producing:

- per-sleeve verdict artifacts under the sleeve truth partition
- a global summary rollup verdict under the canonical truth root (derived only)

## Invariants

### C2_ORCHESTRATE_PER_SLEEVE_V1

1. **Sleeve iteration**
   - The multi-sleeve orchestrator MUST load the sleeve registry and iterate enabled sleeves deterministically.

2. **Readiness prerequisite**
   - For each enabled sleeve, the orchestrator MUST verify the sleeve readiness pointer head resolves to a PASS (or explicitly governed NO_ACTIVITY), before running the sleeve’s orchestration stages.

3. **Per-sleeve verdict**
   - Each sleeve MUST produce a sleeve verdict artifact and pointer head under:
     - `constellation_2/runtime/truth_sleeves/<sleeve_id>/<mode>/reports/...`

4. **Global rollup verdict**
   - The orchestrator MUST always emit a global verdict artifact under:
     - `constellation_2/runtime/truth/reports/sleeve_rollup_v1/<day>/...`

5. **Non-bricking requirement**
   - If one sleeve fails, the orchestrator MUST still emit:
     - per-sleeve verdicts for all sleeves attempted (including failure reasons)
     - a global rollup verdict
   - Only hard safety breaches (account mismatch, cross-partition write, registry invalid) may ABORT the entire run.

## Verdict semantics (required)

Global rollup verdict MUST be one of:

- `PASS` (all enabled sleeves PASS)
- `DEGRADED` (some sleeves are NO_ACTIVITY or optional stages fail but no required sleeve fails)
- `FAIL` (any required sleeve fails)
- `ABORTED` (only for explicit safety breach: account mismatch, truth partition mismatch, registry invalid, cross-partition write evidence)

## Required evidence surfaces (minimum)

- per-sleeve verdict pointer heads + sha256 proof lines
- global rollup verdict + sha256
- rollup must list the sleeve heads it consumed (paths + sha256)

## Non-claims

- This contract does not define strategy logic.
- This contract defines orchestration scoping, verdict surfaces, and non-bricking guarantees only.
