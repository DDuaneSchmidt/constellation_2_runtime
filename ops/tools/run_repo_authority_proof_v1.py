#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


DEFAULT_AUTH_REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_RUNBOOK_RELPATHS = (
    Path("ops/runbooks/C2_PAPER_READY_DEPLOYMENT_GATE_V1.md"),
    Path("ops/runbooks/C2_PAPER_OPS_RUNBOOK_V1.md"),
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path.read_text(encoding="utf-8")


def _service_runtime_root() -> str:
    unit_path = Path.home() / ".config/systemd/user/c2-paper-day-orchestrator.service"
    if not unit_path.exists():
        raise FileNotFoundError(str(unit_path))
    text = unit_path.read_text(encoding="utf-8")
    match = re.search(r"WorkingDirectory=(.+)", text)
    if not match:
        raise ValueError("MISSING_WORKING_DIRECTORY")
    return match.group(1).strip()


def _configured_runtime_roots(*, registry: dict[str, Any], auth_marker: dict[str, Any]) -> list[Path]:
    roots: list[Path] = []
    for candidate in list(registry.get("runtime_copy_roots") or []) + list(auth_marker.get("runtime_copy_roots") or []):
        candidate_text = str(candidate or "").strip()
        if not candidate_text:
            continue
        path = Path(candidate_text).expanduser().resolve()
        if path not in roots:
            roots.append(path)
    return roots


def _check_runtime_root(*, authoritative_repo_root: Path, runtime_root: Path) -> tuple[list[str], bool | None]:
    failures: list[str] = []
    runtime_marker_path = runtime_root / "repo_role.v1.json"
    runtime_manifest_path = runtime_root / "governance/00_MANIFEST.yaml"

    runtime_marker = _read_json(runtime_marker_path)
    if runtime_marker.get("repo_role") != "deployed_runtime_copy":
        failures.append("RUNTIME_MARKER_ROLE_MISMATCH")
    if runtime_marker.get("governance_authority") is not False:
        failures.append("RUNTIME_MARKER_GOVERNANCE_NOT_FALSE")
    if runtime_marker.get("design_authority") is not False:
        failures.append("RUNTIME_MARKER_DESIGN_NOT_FALSE")
    marker_points_to_authority = runtime_marker.get("authoritative_repo_root") == str(authoritative_repo_root)
    if not marker_points_to_authority:
        failures.append("RUNTIME_MARKER_AUTH_SOURCE_MISMATCH")

    runtime_manifest_text = _read_text(runtime_manifest_path)
    if f"authoritative_repo_root: {authoritative_repo_root}" not in runtime_manifest_text:
        failures.append("RUNTIME_MANIFEST_AUTH_SOURCE_MISSING")
    if "governance_authority: false" not in runtime_manifest_text:
        failures.append("RUNTIME_MANIFEST_GOVERNANCE_FALSE_MISSING")
    if "design_authority: false" not in runtime_manifest_text:
        failures.append("RUNTIME_MANIFEST_DESIGN_FALSE_MISSING")
    if f"repo_root: {runtime_root}" in runtime_manifest_text and "deployment_copy:" not in runtime_manifest_text:
        failures.append("RUNTIME_MANIFEST_STILL_SELF_CANONICAL")

    disallowed_patterns = [
        rf"Expected authority root:\s*[\r\n]+- `{re.escape(str(runtime_root))}`",
        rf"repo authority path is `{re.escape(str(runtime_root))}`",
        r"canonical design authority",
    ]
    for relpath in RUNTIME_RUNBOOK_RELPATHS:
        path = (runtime_root / relpath).resolve()
        text = _read_text(path)
        for pattern in disallowed_patterns:
            if re.search(pattern, text, flags=re.MULTILINE):
                failures.append(f"COMPETING_RUNTIME_CLAIM:{path}")
                break
    return failures, marker_points_to_authority


def collect_repo_authority_proof_v1(
    *,
    authoritative_repo_root: Path | str | None = None,
    mode: str = "paired_deployment",
) -> tuple[dict[str, Any], list[str]]:
    auth_root = Path(authoritative_repo_root or DEFAULT_AUTH_REPO_ROOT).expanduser().resolve()
    if mode not in {"paired_deployment", "authoritative_source_only"}:
        raise ValueError(f"UNSUPPORTED_REPO_AUTHORITY_PROOF_MODE:{mode}")

    failures: list[str] = []
    auth_registry_path = auth_root / "governance/02_REGISTRIES/REPO_AUTHORITY_V1.json"
    auth_marker_path = auth_root / "repo_role.v1.json"
    auth_manifest_path = auth_root / "governance/00_MANIFEST.yaml"

    registry = _read_json(auth_registry_path)
    auth_marker = _read_json(auth_marker_path)

    if registry.get("authoritative_repo_root") != str(auth_root):
        failures.append("AUTH_REGISTRY_ROOT_MISMATCH")
    if auth_marker.get("repo_role") != "authoritative_source":
        failures.append("AUTH_MARKER_ROLE_MISMATCH")
    if auth_marker.get("governance_authority") is not True:
        failures.append("AUTH_MARKER_GOVERNANCE_FALSE")
    if auth_marker.get("design_authority") is not True:
        failures.append("AUTH_MARKER_DESIGN_FALSE")

    auth_manifest_text = _read_text(auth_manifest_path)
    if f"repo_root: {auth_root}" not in auth_manifest_text:
        failures.append("AUTH_MANIFEST_CANONICAL_ROOT_MISSING")
    if "repo_role:" not in auth_manifest_text:
        failures.append("AUTH_MANIFEST_ROLE_SECTION_MISSING")

    configured_runtime_roots = _configured_runtime_roots(registry=registry, auth_marker=auth_marker)
    runtime_roots_checked: list[str] = []
    runtime_marker_points_to_authority: bool | None = None
    deployed_runtime_root = ""

    if mode == "paired_deployment":
        if not configured_runtime_roots:
            failures.append("RUNTIME_COPY_ROOTS_UNCONFIGURED")
        try:
            deployed_runtime_root = _service_runtime_root()
        except Exception as exc:
            failures.append(f"LIVE_SERVICE_RUNTIME_ROOT_UNPROVEN:{type(exc).__name__}:{exc}")
        for runtime_root in configured_runtime_roots:
            runtime_roots_checked.append(str(runtime_root))
            try:
                runtime_failures, marker_points = _check_runtime_root(
                    authoritative_repo_root=auth_root,
                    runtime_root=runtime_root,
                )
            except Exception as exc:
                failures.append(f"RUNTIME_ROOT_UNREADABLE:{runtime_root}:{type(exc).__name__}:{exc}")
                continue
            if runtime_marker_points_to_authority is None:
                runtime_marker_points_to_authority = marker_points
            failures.extend(runtime_failures)
        if deployed_runtime_root and deployed_runtime_root not in runtime_roots_checked:
            failures.append("LIVE_SERVICE_RUNTIME_ROOT_MISMATCH")

    payload = {
        "schema_id": "C2_REPO_AUTHORITY_PROOF_V1",
        "schema_version": 1,
        "mode": mode,
        "authoritative_repo_root": str(auth_root),
        "configured_runtime_copy_roots": [str(path) for path in configured_runtime_roots],
        "runtime_copy_roots_checked": runtime_roots_checked,
        "current_deployed_runtime_root": deployed_runtime_root,
        "runtime_marker_points_to_authority": runtime_marker_points_to_authority,
        "competing_authority_claims_remaining": sorted(set(failures)),
        "status": "PASS" if not failures else "FAIL",
    }
    return payload, sorted(set(failures))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_repo_authority_proof_v1")
    ap.add_argument("--authoritative_repo_root", default=str(DEFAULT_AUTH_REPO_ROOT))
    ap.add_argument(
        "--mode",
        default="paired_deployment",
        choices=["paired_deployment", "authoritative_source_only"],
    )
    args = ap.parse_args(argv)

    try:
        payload, failures = collect_repo_authority_proof_v1(
            authoritative_repo_root=args.authoritative_repo_root,
            mode=args.mode,
        )
    except Exception as exc:
        payload = {
            "schema_id": "C2_REPO_AUTHORITY_PROOF_V1",
            "schema_version": 1,
            "mode": args.mode,
            "authoritative_repo_root": str(Path(args.authoritative_repo_root).expanduser().resolve()),
            "configured_runtime_copy_roots": [],
            "runtime_copy_roots_checked": [],
            "current_deployed_runtime_root": "",
            "runtime_marker_points_to_authority": None,
            "competing_authority_claims_remaining": [f"REPO_AUTHORITY_PROOF_UNUSABLE:{type(exc).__name__}:{exc}"],
            "status": "FAIL",
        }
        failures = list(payload["competing_authority_claims_remaining"])
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
