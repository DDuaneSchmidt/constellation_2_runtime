---
id: C2_REFINEMENT_ACTION_MODEL_V1
title: "C2 Refinement Action Model Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_refinement_plane
---

# refinement_action_model_v1

Legal actions:
- `preserve_top_level`
- `compress_summary`
- `demote_to_secondary`
- `preserve_drilldown_only`
- `retire_from_default_surface`
- `refinement_withheld`

Action laws:
- `preserve_top_level`
  - required evidence: trust-critical or safety-preserving basis
  - prohibited targets: none
  - reversibility: only when upstream evidence changes
- `compress_summary`
  - required evidence: complete basis and non-critical prominence reduction
  - prohibited targets: claim-strength-critical, degraded-critical, blocked-critical targets
  - reversibility: reversible on next governed review
- `demote_to_secondary`
  - required evidence: non-critical but still active review relevance
  - prohibited targets: trust-critical targets
  - reversibility: reversible on next governed review
- `preserve_drilldown_only`
  - required evidence: governed proof preserved elsewhere and top-level prominence no longer required
  - prohibited targets: trust-critical targets
  - reversibility: reversible on next governed review
- `retire_from_default_surface`
  - not legal in first-wave scope
- `refinement_withheld`
  - required evidence: insufficient basis or conflicting evidence
  - visibility law: preserve current or prior safer visibility; do not optimize away truth
