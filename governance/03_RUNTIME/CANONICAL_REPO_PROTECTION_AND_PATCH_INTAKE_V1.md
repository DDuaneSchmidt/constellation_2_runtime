# CANONICAL_REPO_PROTECTION_AND_PATCH_INTAKE_V1

Status: ACTIVE  
Owner: Constellation Runtime Governance  
Effective Date: 2026-04-25

## Purpose

Define a fail-closed operating model that preserves canonical source reproducibility and prevents uncontrolled direct edits.

## Contract

1. `/home/node/constellation` is the canonical source root.
2. Canonical source must remain clean (`git status --porcelain` empty).
3. Codex/AI agents may not directly edit canonical source.
4. Codex/AI task work must occur under `/home/node/constellation_agent_workspace/<task_id>`.
5. Codex/AI outputs patch bundles only, under `/home/node/constellation_patch_inbox/<task_id>`.
6. `ops/tools/apply_codex_patch_bundle_v1.py` is the only authorized canonical writer workflow.
7. Runtime truth/output must live under `/home/node/constellation_runtime_data`.
8. Releases may build only from clean committed canonical source.
9. Dirty canonical source blocks release and READY/readiness claims.
10. Violations fail closed.

## Enforcement Surface

- Clean guard: `ops/tools/require_canonical_repo_clean_v1.py`
- Workspace creation and verification:
  - `ops/tools/create_codex_agent_workspace_v1.py`
  - `ops/tools/verify_codex_agent_workspace_v1.py`
- Intake-only canonical writes:
  - `ops/tools/apply_codex_patch_bundle_v1.py`
- Repo protection controls:
  - `ops/tools/protect_canonical_repo_v1.py`
  - `ops/tools/unprotect_canonical_repo_for_intake_v1.py`
- Runtime protection status:
  - `/home/node/constellation_runtime_data/repo_protection_v1/status.json`
  - `/home/node/constellation_runtime_data/repo_protection_v1/audit.jsonl`

## Fail-Closed Conditions

The system must return non-success status when any of the following are true:

- canonical repo is dirty,
- patch manifest or patch payload is missing,
- patch attempts to touch forbidden runtime paths,
- intake tests fail,
- commit step fails,
- reprotection cannot be restored after intake.

