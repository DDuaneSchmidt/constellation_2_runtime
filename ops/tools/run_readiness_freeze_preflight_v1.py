#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import (
    CANONICAL_REPO_ROOT,
    PATCH_INBOX_ROOT,
    RELEASES_ROOT,
    git_status_porcelain_paths_v1,
)
from ops.tools.verify_patch_bundle_base_commit_v1 import (
    evaluate_patch_bundle_base_commit_v1,
)


ACTIVE_RELEASE_LINK = Path("/home/node/constellation_active").resolve()
ALLOWED_READINESS_PATH_PREFIXES = (
    "ops/tools/run_submit_boundary_status_v1.py",
    "ops/tools/run_paper_session_ledger_v1.py",
    "ops/tools/run_paper_day_control_plane_v1.py",
    "ops/tools/run_trading_day_state_machine_v1.py",
    "ops/tools/run_aegis_day_closure_authority_v1.py",
    "constellation_2/common/tests/",
)


def _normalize_path(path: str) -> str:
    return str(path or "").strip().replace("\\", "/")


def _path_allowed_in_readiness_freeze(path: str) -> bool:
    normalized = _normalize_path(path)
    return any(
        normalized == prefix.rstrip("/") or normalized.startswith(prefix)
        for prefix in ALLOWED_READINESS_PATH_PREFIXES
    )


def _parse_patch_paths(patch_text: str) -> list[str]:
    paths: list[str] = []
    for line in patch_text.splitlines():
        if not line.startswith("diff --git "):
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        b_path = str(parts[3]).strip()
        if b_path.startswith("b/"):
            b_path = b_path[2:]
        if b_path:
            paths.append(b_path)
    return sorted(set(paths))


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("manifest is not a JSON object")
    return payload


def _collect_touched_paths(*, bundle_root: Path, manifest: dict[str, Any]) -> list[str]:
    changed_files = manifest.get("changed_files")
    if isinstance(changed_files, list) and all(isinstance(item, str) for item in changed_files):
        return sorted({_normalize_path(str(item)) for item in changed_files if _normalize_path(str(item))})
    patch_path = (bundle_root / "changes.patch").resolve()
    if patch_path.exists() and patch_path.is_file():
        return _parse_patch_paths(patch_path.read_text(encoding="utf-8"))
    return []


def _is_readiness_bundle(*, manifest: dict[str, Any], touched_paths: list[str]) -> bool:
    scope = str(manifest.get("validation_scope") or "").strip().lower()
    if "readiness" in scope:
        return True
    return any(_path_allowed_in_readiness_freeze(path) for path in touched_paths)


def _parse_validation_report_labels(*, text: str) -> dict[str, Any]:
    lines = text.splitlines()
    validation_sources: list[str] = []
    authoritative_runtime_evidence: str = ""
    has_validation_source = False
    has_authoritative_runtime_evidence = False

    def _consume_list(start_idx: int) -> list[str]:
        items: list[str] = []
        idx = start_idx + 1
        while idx < len(lines):
            raw = lines[idx]
            stripped = raw.strip()
            if not stripped:
                idx += 1
                continue
            if stripped.startswith("- "):
                items.append(stripped[2:].strip())
                idx += 1
                continue
            if re.match(r"^[A-Za-z0-9_/-]+\s*:", stripped):
                break
            break
        return items

    for idx, raw in enumerate(lines):
        stripped = raw.strip()
        if re.match(r"^validation_source\s*:", stripped, flags=re.IGNORECASE):
            has_validation_source = True
            _, _, value = stripped.partition(":")
            text_value = value.strip()
            if text_value:
                validation_sources.extend(
                    token.strip() for token in text_value.split(",") if token.strip()
                )
            else:
                validation_sources.extend(_consume_list(idx))
        if re.match(r"^authoritative_runtime_evidence\s*:", stripped, flags=re.IGNORECASE):
            has_authoritative_runtime_evidence = True
            _, _, value = stripped.partition(":")
            text_value = value.strip()
            if text_value:
                authoritative_runtime_evidence = text_value
            else:
                items = _consume_list(idx)
                if items:
                    authoritative_runtime_evidence = items[0]

    sources_normalized = sorted({item.strip().lower() for item in validation_sources if item.strip()})
    authoritative_value = authoritative_runtime_evidence.strip().lower()
    return {
        "has_validation_source": has_validation_source,
        "validation_sources": sources_normalized,
        "has_authoritative_runtime_evidence": has_authoritative_runtime_evidence,
        "authoritative_runtime_evidence": authoritative_value,
        "workspace_path_detected": "/home/node/constellation_agent_workspace/" in text,
    }


def _validate_active_release_points_to_latest(*, releases_root: Path, active_link: Path) -> tuple[bool, dict[str, str]]:
    details = {"active_release_path": "", "latest_release_path": ""}
    if not releases_root.exists() or not releases_root.is_dir():
        return False, details
    release_dirs = sorted(
        [path.resolve() for path in releases_root.iterdir() if path.is_dir()],
        key=lambda item: item.name,
    )
    if not release_dirs:
        return False, details
    latest_release = release_dirs[-1]
    details["latest_release_path"] = str(latest_release)
    if not active_link.exists():
        return False, details
    active_target = active_link.resolve()
    details["active_release_path"] = str(active_target)
    return active_target == latest_release, details


def evaluate_readiness_freeze_preflight_v1(
    *,
    canonical_repo_root: Path = CANONICAL_REPO_ROOT,
    patch_inbox_root: Path = PATCH_INBOX_ROOT,
    releases_root: Path = RELEASES_ROOT,
    active_release_link: Path = ACTIVE_RELEASE_LINK,
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    pending_readiness_bundles: list[str] = []
    base_commit_matches = True

    dirty_paths = git_status_porcelain_paths_v1(canonical_repo_root)
    canonical_clean = len(dirty_paths) == 0
    if not canonical_clean:
        violations.append(
            {
                "code": "READINESS_FREEZE_CANONICAL_DIRTY",
                "detail": "canonical repo must be clean",
                "dirty_paths": dirty_paths,
            }
        )

    bundle_roots: list[Path] = []
    if patch_inbox_root.exists() and patch_inbox_root.is_dir():
        bundle_roots = sorted([item.resolve() for item in patch_inbox_root.iterdir() if item.is_dir()], key=lambda p: p.name)

    for bundle_root in bundle_roots:
        task_id = bundle_root.name
        manifest_path = (bundle_root / "manifest.json").resolve()
        patch_path = (bundle_root / "changes.patch").resolve()
        if not manifest_path.exists() or not patch_path.exists():
            continue

        try:
            manifest = _load_manifest(manifest_path)
        except Exception as exc:
            violations.append(
                {
                    "code": "READINESS_FREEZE_MANIFEST_INVALID",
                    "task_id": task_id,
                    "detail": f"{type(exc).__name__}:{exc}",
                    "manifest_path": str(manifest_path),
                }
            )
            continue

        touched_paths = _collect_touched_paths(bundle_root=bundle_root, manifest=manifest)
        if not _is_readiness_bundle(manifest=manifest, touched_paths=touched_paths):
            continue

        pending_readiness_bundles.append(task_id)

        base_eval = evaluate_patch_bundle_base_commit_v1(
            task_id=task_id,
            canonical_repo_root=canonical_repo_root,
            patch_inbox_root=patch_inbox_root,
        )
        if base_eval.get("status") != "PASS":
            base_commit_matches = False
            violations.append(
                {
                    "code": "READINESS_FREEZE_STALE_BASE_COMMIT",
                    "task_id": task_id,
                    "detail": base_eval.get("code"),
                    "manifest_base_commit": base_eval.get("manifest_base_commit", ""),
                    "canonical_head": base_eval.get("canonical_head", ""),
                }
            )

        disallowed_paths = [path for path in touched_paths if not _path_allowed_in_readiness_freeze(path)]
        if disallowed_paths:
            violations.append(
                {
                    "code": "READINESS_FREEZE_DISALLOWED_PATH",
                    "task_id": task_id,
                    "paths": sorted(disallowed_paths),
                }
            )

        validation_report_path = (bundle_root / "validation_report.md").resolve()
        if not validation_report_path.exists() or not validation_report_path.is_file():
            violations.append(
                {
                    "code": "READINESS_FREEZE_VALIDATION_REPORT_MISSING",
                    "task_id": task_id,
                    "validation_report_path": str(validation_report_path),
                }
            )
            continue

        report_text = validation_report_path.read_text(encoding="utf-8")
        report_meta = _parse_validation_report_labels(text=report_text)
        if not report_meta["has_validation_source"]:
            violations.append(
                {
                    "code": "READINESS_FREEZE_VALIDATION_SOURCE_MISSING",
                    "task_id": task_id,
                    "validation_report_path": str(validation_report_path),
                }
            )
        if not report_meta["has_authoritative_runtime_evidence"]:
            violations.append(
                {
                    "code": "READINESS_FREEZE_AUTHORITATIVE_RUNTIME_EVIDENCE_MISSING",
                    "task_id": task_id,
                    "validation_report_path": str(validation_report_path),
                }
            )

        report_authoritative_yes = report_meta["authoritative_runtime_evidence"] == "yes"
        if report_authoritative_yes and (
            "workspace" in report_meta["validation_sources"] or report_meta["workspace_path_detected"]
        ):
            violations.append(
                {
                    "code": "READINESS_FREEZE_WORKSPACE_RUNTIME_CLAIM_FORBIDDEN",
                    "task_id": task_id,
                    "validation_report_path": str(validation_report_path),
                }
            )

        manifest_authoritative = bool(manifest.get("authoritative_runtime_validation") is True)
        if manifest_authoritative and "active_release" not in report_meta["validation_sources"]:
            violations.append(
                {
                    "code": "READINESS_FREEZE_ACTIVE_RELEASE_SOURCE_REQUIRED",
                    "task_id": task_id,
                    "validation_report_path": str(validation_report_path),
                }
            )
        if not manifest_authoritative and report_authoritative_yes:
            violations.append(
                {
                    "code": "READINESS_FREEZE_MANIFEST_REPORT_RUNTIME_CLAIM_MISMATCH",
                    "task_id": task_id,
                    "validation_report_path": str(validation_report_path),
                }
            )

    if len(pending_readiness_bundles) > 1:
        violations.append(
            {
                "code": "READINESS_FREEZE_MULTIPLE_PENDING_READINESS_BUNDLES",
                "pending_readiness_bundles": pending_readiness_bundles,
            }
        )

    active_release_valid, active_release_details = _validate_active_release_points_to_latest(
        releases_root=releases_root,
        active_link=active_release_link,
    )
    if not active_release_valid:
        violations.append(
            {
                "code": "READINESS_FREEZE_ACTIVE_RELEASE_NOT_LATEST",
                **active_release_details,
            }
        )

    return {
        "status": "PASS" if len(violations) == 0 else "FAIL",
        "canonical_clean": canonical_clean,
        "pending_readiness_bundles": pending_readiness_bundles,
        "base_commit_matches": base_commit_matches,
        "active_release_valid": active_release_valid,
        "violations": violations,
    }


def main(argv: list[str] | None = None) -> int:
    _ = argparse.ArgumentParser(prog="run_readiness_freeze_preflight_v1").parse_args(argv)
    payload = evaluate_readiness_freeze_preflight_v1()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
