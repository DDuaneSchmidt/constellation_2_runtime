---
id: C2_POLICY_EVOLUTION_ACTION_MODEL_V1
title: "C2 Policy Evolution Action Model Contract v1"
status: DRAFT
version: 1
created_utc: 2026-04-19
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0_certified_policy_evolution_plane
---

# policy_evolution_action_model_v1

Legal actions:
- `preserve_current_policy`
- `propose_strengthen_emphasis`
- `propose_reduce_emphasis`
- `propose_more_compression`
- `evolution_withheld`
- `evolution_expired`
- `rollback_to_prior_policy`

Action laws:
- `preserve_current_policy`
  - required evidence: stable but non-escalating support, or trust-preserving bias
  - prohibited targets: none
  - downstream effect boundary: preserves current policy snapshot only
- `propose_strengthen_emphasis`
  - required evidence: repeated governed support for higher visibility
  - prohibited targets: targets lacking sufficient history
  - downstream effect boundary: proposal only; no direct product mutation
- `propose_reduce_emphasis`
  - required evidence: repeated non-critical secondary/drill-down suitability
  - prohibited targets: trust-protected targets
  - downstream effect boundary: proposal only
- `propose_more_compression`
  - required evidence: repeated compression-safe support
  - prohibited targets: trust-protected targets
  - downstream effect boundary: proposal only
- `evolution_withheld`
  - required evidence: insufficient or unstable history
  - downstream effect boundary: preserve prior safer policy
- `evolution_expired`
  - required evidence: prior proposal aged beyond review window without renewed support
  - downstream effect boundary: explicit re-review required
- `rollback_to_prior_policy`
  - required evidence: prior proposal contradicted by current trust-protected or unstable evidence
  - downstream effect boundary: revert to prior safer policy snapshot
