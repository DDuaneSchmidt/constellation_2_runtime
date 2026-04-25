# advisory_decision_state_kernel_v1

This contract governs the Bundle 8 certified advisory decision plane.

Core law:
- `constellation_2.common.advisory_decision_state_kernel_v1` is the only legal semantic source for advisory decision safety on the active C2 path
- advisory/operator surfaces MUST NOT recompute decision state, actionability, freshness, visibility, promotion eligibility, or explanation semantics outside that kernel
- advisory decision evaluation MUST consume certified truth only and MUST fail closed on forbidden or unprovable upstream truth

Allowed inputs:
- admitted stage artifacts:
  - `control_stage_day_admitted_v1`
  - `control_stage_context_admitted_v1`
  - `control_stage_session_admitted_v1`
  - `control_stage_execution_build_admitted_v1`
- stage certification artifacts:
  - `control_stage_day_certification_v1`
  - `control_stage_context_certification_v1`
  - `control_stage_session_certification_v1`
  - `control_stage_execution_build_certification_v1`
- `startup_chain_certification_v1`
- `control_stage_transition_record_v1`
- `deployment_state_machine_v1` when release/baseline actionability is claimed
- `tax_state_v1` only through the governed tax-aware advisory binding path when tax-aware evaluation is requested
- `opportunity_state_v1` only through the governed opportunity-aware advisory binding path when proactive opportunity evaluation is requested

Forbidden inputs:
- direct reasoning over raw runtime artifacts outside the certified path
- `official_recommendation_set_v1`, `advisor_trade_intent_proposal_v1`, or any legacy `promotion_*` surface as an authority-bearing advisory truth input
- trust-plane or UI surfaces as upstream truth for decision-state evaluation
- UI-, CLI-, or read-model-local actionability logic
- prose-implied or confidence-implied promotion eligibility

Canonical decision-object law:
- one advisory evaluation MUST yield one canonical advisory decision object
- the decision object MUST carry:
  - `decision_state`
  - `actionability_state`
  - `freshness_state`
  - `visibility_state`
  - `promotion_eligibility_state`
  - governing refs
  - primary invalidation or downgrade rule
  - deterministic explanation payload
- the same certified truth basis MUST yield the same advisory decision object except for ratified volatile timestamps

Decision-state precedence law:
- advisory decision state MUST be resolved by one deterministic precedence matrix, not scattered conditionals
- higher-risk states dominate lower-risk states
- the precedence/invalidation matrix is governed separately by `advisory_decision_state_precedence_v1`

Explanation law:
- advisory explanation mapping MUST be table-driven
- explanation MUST preserve authority label, governing refs, decision state, freshness state, visibility state, and invalidation reason
- explanation MUST NOT invent unsupported certainty or causal meaning

Semantic-preservation law:
- Bundle 8 MUST preserve Bundle 7 release/readiness semantics
- Bundle 8 MUST preserve Bundle 6 trust-plane semantics
- Bundle 8 MUST preserve Bundle 5 transition semantics
- Bundle 8 MUST preserve Bundle 4 certification/boundary semantics
- Bundle 8 MUST preserve Bundle 3 configuration activation semantics
