# control_plane_decomposition_antipatterns_v1

This contract governs Bundle 5 decomposition discipline.

Thin orchestrator law:
- orchestration code MUST request canonical transitions in stage order and stop on failure for mutating flows
- orchestration code MUST NOT inspect deep family payloads for business/control decisions

Thin stage-definition law:
- stage definitions may declare:
  - `stage_id`
  - required upstream admitted stage
  - required families
  - stage invariants
  - allowed policies
  - certification requirements
  - whether current projection is allowed
- all other logic belongs in the transition engine

Banned patterns:
- business logic in orchestrators
- root resolution in orchestrators
- invariant duplication across validators, engine, and orchestrators
- recovery path outside the transition engine
- stage admission outside the transition engine
- certification semantics forked away from the transition engine

