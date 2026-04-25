---
id: C2_RUNTIME_LIFECYCLE_V1
title: "Runtime Lifecycle V1"
status: DRAFT
version: 1
created_utc: 2026-04-18
owner: Constellation
authority: governance+git+runtime_truth
scope: constellation_2_0
---

# Runtime Lifecycle V1

Canonical outputs:
- `/home/node/constellation_runtime_data/runtime_lifecycle_v1/active/<SEAM_ID>/current.json`
- `/home/node/constellation_runtime_data/runtime_lifecycle_v1/receipts/<RUN_ID>/runtime_lifecycle_receipt.v1.json`
- `/home/node/constellation_runtime_data/runtime_lifecycle_v1/exit_receipts/<RUN_ID>/runtime_lifecycle_exit_receipt.v1.json`

Rules:
- runtime lifecycle is the governed startup-admission layer for the canonical runtime startup seam
- it MUST reference active runtime identity instead of duplicating runtime root or broker authority
- it MUST validate:
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_lifecycle_active.v1.schema.json`
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_lifecycle_receipt.v1.schema.json`
  - `governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_lifecycle_exit_receipt.v1.schema.json`
- canonical helper: `constellation_2/common/runtime_lifecycle_v1.py`
- canonical tool: `ops/tools/run_runtime_lifecycle_v1.py`
- canonical startup wrappers MUST:
  - resolve active runtime identity
  - request runtime lifecycle admission before launch
  - fail closed if lifecycle admission is denied
  - register one active run for the startup seam when admitted
  - emit append-only runtime lifecycle receipts
  - emit append-only runtime lifecycle exit receipts on wrapper termination/finalization
  - launch existing runtime behavior unchanged after admission
- runtime lifecycle MUST deny duplicate active starts on the same canonical seam
- runtime lifecycle MUST fail closed when a stale or invalid active-state artifact is present
- lifecycle receipts MUST be append-only and operator-auditable
- lifecycle exit receipts MUST record the governed run_id and active-state release outcome for the terminating startup seam
- lifecycle active state is a narrow current-state control-plane surface used only for startup uniqueness on the canonical seam
- runtime lifecycle must not become a second authority for:
  - runtime roots
  - governed execution identity
  - day/session admission
  - trading decisions
