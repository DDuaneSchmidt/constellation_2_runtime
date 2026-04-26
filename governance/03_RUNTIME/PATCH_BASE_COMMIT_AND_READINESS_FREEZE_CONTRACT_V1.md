# Patch Base Commit And Readiness Freeze Contract V1

Status: ACTIVE  
Scope: `/home/node/constellation` patch intake and readiness proof discipline

## Contract

1. `/home/node/constellation` is the canonical source root and must be clean before workspace creation and patch intake.
2. Patch bundles expire when canonical `HEAD` changes.
3. Patch intake must fail closed on `base_commit` mismatch.
4. Workspaces may create patch bundles and run tests, but may not produce authoritative runtime readiness evidence.
5. Only active-release runtime output is authoritative readiness proof.
6. Readiness fixes are serialized: multiple pending readiness bundles are forbidden during freeze.
7. During readiness freeze, unrelated feature work is forbidden.
8. Stale bundles must be regenerated from current canonical `HEAD`; force-apply and auto-rebase at intake are forbidden.
9. `validation_report.md` must label `validation_source` and `authoritative_runtime_evidence`.
10. Workspace runtime artifacts are draft evidence only and must not be promoted as readiness proof.

## Enforcement Tools

- `ops/tools/apply_codex_patch_bundle_v1.py`
- `ops/tools/verify_patch_bundle_base_commit_v1.py`
- `ops/tools/run_readiness_freeze_preflight_v1.py`

## Fail-Closed Codes

- `PATCH_BUNDLE_BASE_COMMIT_MISMATCH`
- `READINESS_FREEZE_CANONICAL_DIRTY`
- `READINESS_FREEZE_MULTIPLE_PENDING_READINESS_BUNDLES`
- `READINESS_FREEZE_STALE_BASE_COMMIT`
- `READINESS_FREEZE_WORKSPACE_RUNTIME_CLAIM_FORBIDDEN`
- `READINESS_FREEZE_DISALLOWED_PATH`
- `READINESS_FREEZE_ACTIVE_RELEASE_NOT_LATEST`
