# control_plane_recovery_recompute_legality_v1

This contract governs Bundle 5 recovery and recompute legality.

`recompute_frozen`:
- legal only when explicit frozen authoritative refs are supplied from a prior transition record
- legal only when the currently resolved authoritative inputs exactly match the frozen refs
- legal only when the upstream admitted-stage ref remains the same basis
- MUST NOT silently substitute a newly resolved current input
- MAY emit a transition record
- MUST NOT emit a semantically new admission path under a frozen recompute claim

`supersede_from_new_inputs`:
- required when authoritative inputs have changed relative to the frozen or prior transition basis
- required when the upstream admitted-stage ref has changed
- MUST carry explicit supersession lineage through `supersedes_ref`
- MAY emit new admission and certification artifacts only through the transition engine

`certify_only`:
- MUST NOT mutate admission truth
- MAY emit certification artifacts only when a matching admitted-stage artifact already exists

`explain_blocked`:
- MUST NOT mutate truth

