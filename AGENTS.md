# Aegis Runtime Truth Rules

Before modifying Aegis:

1. Run `npm run aegis:audit`.
2. Read the latest `verified_runtime_graph.v1.json`.
3. Do not infer readiness from code.
4. Trust the runtime truth kernel and verified runtime graph.
5. After changes, update manifests and tests. Codex must update `aegis/modules/**/aegis.module.yaml` whenever adding or modifying Aegis behavior, capabilities, commands, evidence, policies, or UI surfaces.
6. Run `npm run aegis:audit` again.

Core invariant: no consumer invents truth. Consumers query verified truth.
