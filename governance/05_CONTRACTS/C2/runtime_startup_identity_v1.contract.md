---
id: C2_RUNTIME_STARTUP_IDENTITY_V1
title: "Runtime Startup Identity V1"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Runtime Startup Identity V1

Canonical output:
- `/home/node/constellation_runtime_data/runtime_startup_identity_v1/<STARTUP_ID>/runtime_startup_identity.v1.json`

Rules:
- this artifact is append-only startup evidence for the effective runtime identity used by the canonical startup seam
- it MUST validate against `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_startup_identity.v1.schema.json`
- canonical writer: `ops/tools/run_runtime_startup_identity_v1.py`
- canonical helper: `constellation_2/common/runtime_identity_v1.py`
- startup wrappers MUST emit this artifact after resolving active runtime identity and before launching existing runtime behavior
- it MUST record:
  - the active runtime contract path and sha256
  - the resolved authoritative repo root
  - the resolved runtime roots
  - the governed runtime environment
  - the referenced governed execution identity binding
  - the resolved governed execution identity and execution root snapshot
  - the requested and resolved Python executable
  - the startup entrypoint and service identity
  - release provenance sufficient to reproduce the startup configuration context
- it MUST fail closed if the active runtime contract is missing, invalid, or references an execution identity that cannot be resolved
- it MUST not become a second authority for broker configuration or runtime roots; those remain owned by the active runtime contract and governed execution identity authorities
