---
id: C2_SLEEVE_LIVE_READINESS_CONTRACT_V1
title: "C2 Sleeve Live Readiness Contract v1"
status: DRAFT
version: 1
created_utc: 2026-03-09
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# C2 Sleeve Live Readiness Contract v1

## Purpose

Define a governed future-grade surface for PAPER to LIVE promotion readiness, separate from raw execution PASS/FAIL.

This surface is non-canonical for sleeve-edge control decisions.

## Artifact surface

Target artifact path:

- `/home/node/constellation_runtime_data/truth_sleeves/<sleeve_id>/PAPER/readiness_v1/sleeve_live_readiness_v1/<DAY_UTC>/sleeve_live_readiness.v1.json`

This artifact is advisory for promotion governance and is not a substitute for execution authority or gate-stack verdict.

It must carry an explicit non-canonical warning and must not be used as allocator or qualification control truth.

## Separation from execution status

- `execution PASS` means current sleeve run completed in scope.
- `sleeve_live_readiness grade` is a promotion-grade signal and may remain UNKNOWN while execution is PASS.

## Intended inputs (design stub)

- sleeve execution stability trend
- monitoring freshness and quality signals
- capital and reconciliation continuity
- governance conformance checks

## Output intent (design stub)

- `readiness_state`: `READY|NOT_READY|UNKNOWN`
- `readiness_summary`: short deterministic operator summary derived from structured blockers
- `promotion_decision_basis`: deterministic concise basis statement for promotion withhold/eligibility
- `readiness_score`: 0..100 policy-scored
- `readiness_grade`: governed categorical band
- `promotion_candidate`: boolean, true only when all required promotion gates pass
- `promotion_blockers`: explicit blocker codes when promotion_candidate is false
- `root_blockers`: minimal true blocking set
- `derived_blockers`: aggregate/derived blockers (non-root)
- `aggregate_blocker_summary`: root vs derived counts and candidate status
- `promotion_blockers_detail`: structured blockers with `blocker_code`, `blocker_class`, `severity`, `affected_surface`, `exact_evidence_path`, `clearance_condition`
- `minimum_conditions_summary`: concise required conditions for promotion_candidate=true
- `current_vs_required`: deterministic current vs required comparison surface
- `smallest_clearance_set`: minimum clearance conditions required for promotable state
- `blocker_dependency_order`: deterministic order to clear blockers
- `estimated_promotion_gate_sequence`: policy gate sequence for promotion review
- `root_blockers` vs `derived_blockers` separation must preserve fail-closed truth while reducing operator noise.
- `evidence_paths`: explicit artifact path list used in scoring
- `top_blockers_ordered`: ordered blocker summary for operator review
- `pass_conditions_remaining`: explicit conditions still required for promotion_candidate=true
- `recommended_next_actions`: deterministic action list tied to blocker codes/artifacts
- `calibration_support`: policy values + current outcome + failed checks + score contribution breakdown
- `promotion_checklist`: compact deterministic operator checklist with `must_be_true`, `currently_false`, `gating_conditions`, `informational_conditions`
- explicit evidence references, per-check detail, and reason codes

## Methodology policy

Readiness scoring and thresholds MUST be governed by:

- `governance/02_REGISTRIES/C2_SLEEVE_LIVE_READINESS_POLICY_V1.json`
- `governance/02_REGISTRIES/C2_LIFECYCLE_DEPENDENCY_CLASSIFICATION_V1.json`

At minimum, policy must enforce:

- sufficient PAPER history window (multi-day, not single-day PASS)
- explicit hard-gate pass set
- monitoring freshness compliance
- no unresolved safety/control failures
- score/grade threshold for READY
- readiness output must be artifact-driven (no UI-derived or heuristic inputs)

## Policy hygiene

- All active keys in `C2_SLEEVE_LIVE_READINESS_POLICY_V1.json` must be enforced by writer logic.
- Dead or advisory-only keys must not remain in governed policy.

## Governed schema

- `governance/04_DATA/SCHEMAS/C2/READINESS/sleeve_live_readiness.v1.schema.json`

## Ownership boundary

- Producer ownership: diagnostics/readiness governance pipeline.
- Consumer ownership: operator UX and promotion control workflows.
