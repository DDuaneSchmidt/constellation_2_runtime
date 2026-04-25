# paper_policy_verdict_v1

`paper_policy_verdict_v1` consumes `capability_state_v1` and the governed capability policy registry.

It authorizes paper operation only on capabilities classified as `paper_role = BLOCKING`.

For PAPER startup, the governed blocking authorization capability is `startup_authorization_gate_set_ready`, which is derived from `authorization_gate_verdict_v1`.

Lifecycle-scoped economic or backward-compatible diagnostic capabilities may remain visible in `advisory_items`, but they must not block PAPER startup unless the governed capability policy classifies them as `paper_role = BLOCKING`.

It must not mutate evidence or gate artifacts and must not weaken production semantics.
