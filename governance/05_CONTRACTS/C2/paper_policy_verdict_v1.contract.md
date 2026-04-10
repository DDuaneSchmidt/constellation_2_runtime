# paper_policy_verdict_v1

`paper_policy_verdict_v1` consumes `capability_state_v1` and the governed capability policy registry.

It authorizes paper operation only on capabilities classified as `paper_role = BLOCKING`.

It must not mutate evidence or gate artifacts and must not weaken production semantics.
