# artifact_authority_registry_v1

`artifact_authority_registry_v1` is the lightweight contract registry for active paper-day artifact authority.

Rules:
- one artifact class maps to one authoritative writer
- domain/root ownership must be explicit
- mirrored artifacts must declare one authoritative bridge per mirror path
- consumers may only read artifacts that list them as legal consumers
- build and authority stages must declare consumed artifacts in their dependency inventory; registry membership does not replace explicit inventory rows
- missing required artifacts remain fail-closed unless the contract explicitly allows governed materialization
- contracts are repo-native governance metadata, not a workflow engine
