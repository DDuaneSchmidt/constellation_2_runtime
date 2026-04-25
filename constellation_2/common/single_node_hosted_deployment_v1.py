from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.release_current_shadow_validator_v1 import (
    load_release_current_shadow_ref_if_present,
)
from constellation_2.common.runtime_identity_v1 import (
    load_active_runtime_identity_snapshot_v1,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


SINGLE_NODE_HOSTED_PREFLIGHT_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/single_node_hosted_preflight.v1.schema.json"
)
LEGACY_RUNTIME_ROOT = "/home/node/constellation_2_runtime"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now_compact() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _sha256_hex(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _sanitize_token(value: str) -> str:
    raw = str(value or "").strip().lower()
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_")
    sanitized = "_".join(part for part in sanitized.split("_") if part)
    return sanitized or "hosted_preflight"


def _select_requested_python(repo_root: Path, requested_python: str = "") -> tuple[str, str]:
    explicit = str(requested_python or os.getenv("PYTHON_BIN_OVERRIDE") or "").strip()
    if explicit:
        return explicit, "ENV_OVERRIDE"
    repo_local_venv_python = (Path(repo_root).resolve() / ".venv_c2/bin/python").resolve()
    if repo_local_venv_python.exists() and repo_local_venv_python.is_file():
        return str(repo_local_venv_python), "REPO_LOCAL_VENV"
    return "python3", "SYSTEM_PYTHON3"


def resolve_hosted_python_selection_v1(
    *,
    repo_root: Path,
    requested_python: str = "",
) -> dict[str, str]:
    requested, selection_mode = _select_requested_python(Path(repo_root).resolve(), requested_python=requested_python)
    if "/" in requested:
        resolved = Path(requested).expanduser().resolve()
        if not resolved.exists() or not resolved.is_file():
            raise SystemExit(f"FAIL: hosted_preflight_python_missing:path={resolved}")
    else:
        located = shutil.which(requested)
        if not located:
            raise SystemExit(f"FAIL: hosted_preflight_python_unresolved requested={requested!r}")
        resolved = Path(located).resolve()
    return {
        "requested_python": requested,
        "resolved_python_executable": str(resolved),
        "python_selection_mode": selection_mode,
    }


def _service_unit_path(repo_root: Path, service_name: str) -> Path:
    return (Path(repo_root).resolve() / "ops/systemd/user" / str(service_name).strip()).resolve()


def _file_ref(path: Path) -> dict[str, str]:
    target = Path(path).resolve()
    if not target.exists() or not target.is_file():
        raise SystemExit(f"FAIL: hosted_preflight_required_file_missing:path={target}")
    return {"path": str(target), "sha256": _sha256_hex(target)}


def _check_service_unit_alignment(
    *,
    service_unit_path: Path,
    repo_root: Path,
    entrypoint_path: Path,
    release_root: Path | None = None,
) -> tuple[bool, str]:
    text = service_unit_path.read_text(encoding="utf-8")
    repo_root_resolved = Path(repo_root).resolve()
    entrypoint_resolved = Path(entrypoint_path).resolve()
    allowed_workdirs = {str(repo_root_resolved)}
    if release_root is not None:
        allowed_workdirs.add(str(Path(release_root).resolve()))

    working_dirs = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("WorkingDirectory="):
            _, value = stripped.split("=", 1)
            candidate = value.strip()
            if candidate:
                working_dirs.append(str(Path(candidate).expanduser().resolve()))
    if not working_dirs:
        return False, "MISSING:WorkingDirectory"
    if not any(path in allowed_workdirs for path in working_dirs):
        return (
            False,
            "MISSING_OR_MISMATCH:WorkingDirectory:"
            f"expected_one_of={sorted(allowed_workdirs)}:observed={sorted(set(working_dirs))}",
        )

    allowed_entrypoints = {str(entrypoint_resolved)}
    if release_root is not None:
        try:
            rel = entrypoint_resolved.relative_to(repo_root_resolved)
        except ValueError:
            rel = None
        if rel is not None:
            allowed_entrypoints.add(str((Path(release_root).resolve() / rel).resolve()))
    observed_paths = {
        str(Path(token).expanduser().resolve())
        for token in re.findall(r"/[^\s'\";]+", text)
    }
    if not any(path in observed_paths for path in allowed_entrypoints):
        return (
            False,
            "MISSING_ENTRYPOINT:"
            f"expected_one_of={sorted(allowed_entrypoints)}:observed={sorted(observed_paths)}",
        )
    return True, "aligned"


def _probe_python_modules(
    *,
    resolved_python_executable: str,
    modules: list[str],
) -> tuple[bool, str]:
    if not modules:
        return True, "no additional modules required"
    probe_cmd = [
        str(Path(resolved_python_executable).resolve()),
        "-c",
        "import importlib, sys; [importlib.import_module(name) for name in sys.argv[1:]]",
        *modules,
    ]
    proc = subprocess.run(probe_cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or f"returncode={proc.returncode}").strip()
        return False, detail[:400]
    return True, "modules importable"


def _run_repo_authority_proof(
    *,
    resolved_python_executable: str,
    repo_root: Path,
) -> tuple[bool, str]:
    cmd = [
        str(Path(resolved_python_executable).resolve()),
        str((Path(repo_root).resolve() / "ops/tools/run_repo_authority_proof_v1.py").resolve()),
        "--mode",
        "authoritative_source_only",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or f"returncode={proc.returncode}").strip()
        return False, detail[:400]
    try:
        payload = json.loads(proc.stdout)
    except Exception as exc:
        return False, f"repo_authority_proof_parse_error:{type(exc).__name__}:{exc}"
    if str(payload.get("status") or "").strip().upper() != "PASS":
        return False, ",".join(str(item) for item in payload.get("competing_authority_claims_remaining") or []) or "FAIL"
    return True, "PASS"


def _require_release_current_startup_guarantee_v1() -> dict[str, str]:
    try:
        contract_ref = read_control_plane_surface_v1(domain="release", surface="active_runtime_contract")
    except Exception as exc:
        raise SystemExit(
            "FAIL: HOSTED_PREFLIGHT_RELEASE_CURRENT_CONTRACT_UNREADABLE:"
            f"{type(exc).__name__}:{exc}"
        ) from exc
    canonical_truth_root = Path(str(contract_ref.payload.get("canonical_truth_root") or "")).resolve()
    if not canonical_truth_root.is_absolute() or not canonical_truth_root.exists() or not canonical_truth_root.is_dir():
        raise SystemExit(
            f"FAIL: HOSTED_PREFLIGHT_RELEASE_CURRENT_TRUTH_ROOT_INVALID:{canonical_truth_root}"
        )
    release_current_ref = load_release_current_shadow_ref_if_present(truth_root=canonical_truth_root)
    if release_current_ref is None:
        missing_path = (canonical_truth_root / "release_current_v1" / "current.json").resolve()
        raise SystemExit(f"FAIL: HOSTED_PREFLIGHT_RELEASE_CURRENT_MISSING:{missing_path}")
    return {
        "path": str(release_current_ref.path),
        "release_current_id": str(release_current_ref.payload.get("release_current_id") or "").strip(),
    }


def derive_single_node_hosted_preflight_payload_v1(
    *,
    repo_root: Path,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    requested_python: str = "",
    required_python_modules: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    release_current_guarantee = _require_release_current_startup_guarantee_v1()
    runtime_identity = load_active_runtime_identity_snapshot_v1(repo_root=repo_root)
    python_selection = resolve_hosted_python_selection_v1(repo_root=repo_root, requested_python=requested_python)
    resolved_python_executable = python_selection["resolved_python_executable"]
    service_unit_path = _service_unit_path(repo_root, service_name)
    entrypoint_ref = _file_ref(Path(entrypoint_path).resolve())
    service_unit_ref = _file_ref(service_unit_path)
    tool_refs = [
        {"role": "runtime_startup_identity", **_file_ref(repo_root / "ops/tools/run_runtime_startup_identity_v1.py")},
        {"role": "runtime_lifecycle", **_file_ref(repo_root / "ops/tools/run_runtime_lifecycle_v1.py")},
        {"role": "repo_authority_proof", **_file_ref(repo_root / "ops/tools/run_repo_authority_proof_v1.py")},
        {"role": "release_builder", **_file_ref(repo_root / "ops/tools/build_constellation_release_v1.py")},
        {"role": "release_activation", **_file_ref(repo_root / "ops/tools/activate_constellation_release_v1.py")},
        {"role": "deployment_state_machine", **_file_ref(repo_root / "ops/tools/run_deployment_state_machine_v1.py")},
    ]

    checks: list[dict[str, str]] = []
    blocking_codes: list[str] = []

    def add_check(check_id: str, passed: bool, detail: str, reason_code: str = "") -> None:
        checks.append(
            {
                "check_id": check_id,
                "status": "PASS" if passed else "FAIL",
                "detail": str(detail or ("PASS" if passed else "FAIL")),
            }
        )
        if not passed and reason_code:
            blocking_codes.append(reason_code)

    add_check(
        "release_current_startup_guarantee",
        True,
        (
            f"path={release_current_guarantee['path']};"
            f"release_current_id={release_current_guarantee['release_current_id'] or 'UNKNOWN'}"
        ),
    )
    add_check(
        "authoritative_repo_root_match",
        str(runtime_identity.get("authoritative_repo_root") or "").strip() == str(repo_root),
        f"runtime_identity_repo_root={runtime_identity.get('authoritative_repo_root')}",
        "HOSTED_PREFLIGHT_RUNTIME_IDENTITY_REPO_MISMATCH",
    )
    add_check(
        "runtime_data_root_present",
        Path(str(runtime_identity.get("runtime_data_root") or "")).resolve().is_dir(),
        str(runtime_identity.get("runtime_data_root") or ""),
        "HOSTED_PREFLIGHT_RUNTIME_DATA_ROOT_MISSING",
    )
    add_check(
        "canonical_truth_root_present",
        Path(str(runtime_identity.get("canonical_truth_root") or "")).resolve().is_dir(),
        str(runtime_identity.get("canonical_truth_root") or ""),
        "HOSTED_PREFLIGHT_TRUTH_ROOT_MISSING",
    )
    add_check(
        "truth_sleeves_root_present",
        Path(str(runtime_identity.get("truth_sleeves_root") or "")).resolve().is_dir(),
        str(runtime_identity.get("truth_sleeves_root") or ""),
        "HOSTED_PREFLIGHT_TRUTH_SLEEVES_ROOT_MISSING",
    )

    aligned, alignment_detail = _check_service_unit_alignment(
        service_unit_path=service_unit_path,
        repo_root=repo_root,
        entrypoint_path=Path(entrypoint_path).resolve(),
        release_root=Path(str(runtime_identity.get("release_root") or "")).resolve()
        if str(runtime_identity.get("release_root") or "").strip()
        else None,
    )
    add_check(
        "service_unit_alignment",
        aligned,
        alignment_detail,
        "HOSTED_PREFLIGHT_SERVICE_UNIT_MISMATCH",
    )

    legacy_runtime_forbidden = LEGACY_RUNTIME_ROOT not in resolved_python_executable
    add_check(
        "legacy_runtime_python_forbidden",
        legacy_runtime_forbidden,
        resolved_python_executable,
        "HOSTED_PREFLIGHT_LEGACY_RUNTIME_PYTHON_FORBIDDEN",
    )

    modules = [str(item).strip() for item in required_python_modules if str(item).strip()]
    modules_ok, modules_detail = _probe_python_modules(
        resolved_python_executable=resolved_python_executable,
        modules=modules,
    )
    add_check(
        "required_python_modules_importable",
        modules_ok,
        modules_detail,
        "HOSTED_PREFLIGHT_REQUIRED_MODULES_UNAVAILABLE",
    )

    repo_authority_ok, repo_authority_detail = _run_repo_authority_proof(
        resolved_python_executable=resolved_python_executable,
        repo_root=repo_root,
    )
    add_check(
        "repo_authority_proof",
        repo_authority_ok,
        repo_authority_detail,
        "HOSTED_PREFLIGHT_REPO_AUTHORITY_PROOF_FAILED",
    )

    generated_at_utc = _utc_now_iso()
    preflight_id = f"{_utc_now_compact()}__{_sanitize_token(service_name)}__pid{int(os.getpid())}"
    return {
        "schema_id": "single_node_hosted_preflight.v1",
        "schema_version": "v1",
        "preflight_id": preflight_id,
        "status": "PASS" if not blocking_codes else "FAIL",
        "generated_at_utc": generated_at_utc,
        "hosting_mode": "SYSTEMD_SINGLE_NODE",
        "entrypoint_name": str(entrypoint_name).strip(),
        "entrypoint_ref": entrypoint_ref,
        "service_name": str(service_name).strip(),
        "service_unit_ref": service_unit_ref,
        "requested_python": python_selection["requested_python"],
        "resolved_python_executable": resolved_python_executable,
        "python_selection_mode": python_selection["python_selection_mode"],
        "required_python_modules": modules,
        "runtime_identity_ref": {
            "contract_path": str(runtime_identity.get("contract_path") or "").strip(),
            "contract_sha256": str(runtime_identity.get("contract_sha256") or "").strip(),
            "authoritative_repo_root": str(runtime_identity.get("authoritative_repo_root") or "").strip(),
            "runtime_environment": str(runtime_identity.get("runtime_environment") or "").strip(),
            "primary_execution_identity_ref": runtime_identity.get("primary_execution_identity_ref") or {},
        },
        "runtime_root_snapshot": {
            "runtime_data_root": str(runtime_identity.get("runtime_data_root") or "").strip(),
            "canonical_truth_root": str(runtime_identity.get("canonical_truth_root") or "").strip(),
            "truth_sleeves_root": str(runtime_identity.get("truth_sleeves_root") or "").strip(),
            "release_root": str(runtime_identity.get("release_root") or "").strip(),
        },
        "tool_refs": tool_refs,
        "checks": checks,
        "blocking_codes": sorted(set(blocking_codes)),
    }


def resolve_single_node_hosted_preflight_receipt_path_v1(
    *,
    runtime_data_root: Path,
    preflight_id: str,
) -> Path:
    return (
        Path(runtime_data_root).resolve()
        / "single_node_hosted_preflight_v1"
        / str(preflight_id).strip()
        / "single_node_hosted_preflight.v1.json"
    ).resolve()


def write_single_node_hosted_preflight_receipt_v1(
    *,
    repo_root: Path,
    payload: dict[str, Any],
) -> Path:
    runtime_root_snapshot = payload.get("runtime_root_snapshot")
    if not isinstance(runtime_root_snapshot, dict):
        raise SystemExit("FAIL: single_node_hosted_preflight_runtime_root_snapshot_invalid")
    runtime_data_root = Path(str(runtime_root_snapshot.get("runtime_data_root") or "").strip()).resolve()
    if not runtime_data_root:
        raise SystemExit("FAIL: single_node_hosted_preflight_runtime_data_root_missing")
    path = resolve_single_node_hosted_preflight_receipt_path_v1(
        runtime_data_root=runtime_data_root,
        preflight_id=str(payload.get("preflight_id") or "").strip(),
    )
    validate_against_repo_schema_v1(payload, Path(repo_root).resolve(), SINGLE_NODE_HOSTED_PREFLIGHT_SCHEMA_RELPATH)
    path.parent.mkdir(parents=True, exist_ok=False)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
