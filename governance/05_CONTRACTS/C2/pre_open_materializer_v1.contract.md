---
id: C2_PRE_OPEN_MATERIALIZER_V1
title: "C2 Pre-Open Materializer V1"
status: DRAFT
version: 1
created_utc: 2026-04-16
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_paper_session_startup
---

# C2 Pre-Open Materializer V1

Canonical writer:
- `ops/tools/run_pre_open_materializer_v1.py`

Canonical common owner:
- `constellation_2/common/pre_open_materializer_v1.py`

Canonical output:
- `/home/node/constellation_runtime_data/truth/reports/pre_open_bundle_v1/<DAY>/pre_open_bundle.v1.json`

Rules:
- this surface is the canonical startup prerequisite materialization owner for Session Authority day activation
- it must materialize or refresh the governed day-bound startup prerequisites that Session Admission depends on
- it must publish exactly one canonical `pre_open_bundle_v1` per target day plus immutable attempt history
- it must delegate to already-governed producer tools where those owners already exist; it must not hand-create prerequisite truth artifacts
- it must fail closed on missing, malformed, stale, contradictory, or target-day-mismatched prerequisite evidence
- it must keep prerequisite materialization separate from Session Admission binding; it is not itself the admission decision owner
- it must publish stable blocking reason codes and per-prerequisite machine-readable results
- delegated producer outcomes must be normalized into explicit `producer_results[].result_state` values:
  - `PASS`
  - `FAIL`
  - `STALE`
  - `MISMATCH`
  - `BLOCKED`
  - `UNAVAILABLE`
- producer-result normalization must prefer authoritative day artifacts and pointer/head facts over raw subprocess exit codes wherever governed producer artifacts are available
- delegated producers may perform one immediate post-write read-back verification of the artifact(s) they just wrote
- pre-open consumption may perform one immediate post-producer artifact revalidation before classifying the producer result
- canonical morning operator surfaces must stop before Session Authority and day-open progression when this materializer does not produce `materialization_state=COMPLETE`
- polling, loops, sleeps, or opaque multi-shot retries are not allowed
- it must not introduce score-like readiness values or discretionary heuristics

Required delegated producers in this pass:
- `ops/tools/run_ib_api_handshake_spine_v1.py`
- `ops/tools/run_pointer_heads_materialize_v1.py`
- `ops/tools/run_global_kill_switch_v1.py`

Required startup prerequisite families in this pass:
- `ib_api_handshake_latest_pointer_v1`
- `ib_api_handshake_v1`
- `global_kill_switch_state_v1`
- `primary_scoped_canonical_authority_head_v1`

Non-claims:
- this contract does not replace `startup_materialization_v1`
- this contract does not own `target_day_admission_v1`
- this contract does not own `active_session_v1/current.json`
- this contract does not own sleeve-edge, allocation, or execution sequencing
