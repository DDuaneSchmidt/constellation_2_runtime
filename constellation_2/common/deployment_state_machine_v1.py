from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
ACTIVE_POINTER = Path("/home/node/constellation_active")
ACTIVE_RUNTIME_CONTRACT_PATH = (
    Path("/home/node/constellation_runtime_data")
    / "runtime_contract_v1"
    / "active_runtime_contract.v1.json"
).resolve()
LIVE_SERVICE_NAME = "c2-paper-day-orchestrator.service"
LIVE_SERVICE_FALLBACK_PATH = Path("/home/node/.config/systemd/user") / LIVE_SERVICE_NAME
RUNTIME_COPY_ROOT = Path("/home/node/constellation_2_runtime")
RUNTIME_SERVICE_SOURCE_PATH = (
    RUNTIME_COPY_ROOT / "ops/systemd/user/c2-paper-day-orchestrator.service"
).resolve()
AUTHORITATIVE_SERVICE_SOURCE_PATH = (
    REPO_ROOT / "ops/systemd/user/c2-paper-day-orchestrator.service"
).resolve()
AUTHORITATIVE_LAUNCHER_SOURCE_PATH = (
    REPO_ROOT / "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh"
).resolve()
REQUIRED_STARTUP_STACK_FILES = [
    "ops/tools/run_trading_day_intent_generation_v1.py",
    "ops/tools/run_startup_materialization_inputs_prep_v1.py",
    "ops/tools/run_phasec_risk_inputs_prep_v1.py",
    "ops/tools/run_trading_day_state_machine_v1.py",
    "governance/05_CONTRACTS/C2/trading_day_state_machine_v1.contract.md",
    "constellation_2/common/paper_session_ledger_v1.py",
    "governance/05_CONTRACTS/C2/paper_session_ledger_v1.contract.md",
]

_EXEC_PATH_RE = re.compile(
    r"(/home/node/[A-Za-z0-9_./-]+/ops/run/c2_paper_day_orchestrator_systemd_entry_v1\.sh)"
)


@dataclass(frozen=True)
class ReleaseInspectionV1:
    release_id: str
    release_root: Path
    manifest_path: Path
    manifest: dict[str, Any]
    manifest_sha256: str
    bundled_file_hash_summary: dict[str, Any]
    required_startup_stack_present: bool
    missing_required_files: list[str]


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_sort_key(value: str) -> str:
    return str(value or "").strip()


def git_sha_or_fail(repo_root: Path) -> str:
    try:
        value = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            text=True,
        ).strip()
    except Exception as exc:
        raise SystemExit(f"FAIL: deployment git sha unavailable root={repo_root} err={type(exc).__name__}:{exc}") from exc
    if len(value) != 40:
        raise SystemExit(f"FAIL: invalid deployment git sha root={repo_root} value={value!r}")
    return value


def git_branch_or_fail(repo_root: Path) -> str:
    try:
        value = subprocess.check_output(
            ["git", "-C", str(repo_root), "branch", "--show-current"],
            text=True,
        ).strip()
    except Exception as exc:
        raise SystemExit(f"FAIL: deployment branch unavailable root={repo_root} err={type(exc).__name__}:{exc}") from exc
    if not value:
        raise SystemExit(f"FAIL: empty deployment branch root={repo_root}")
    return value


def git_status_entries(repo_root: Path) -> list[str]:
    try:
        output = subprocess.check_output(
            ["git", "-C", str(repo_root), "status", "--short", "--branch"],
            text=True,
        )
    except Exception as exc:
        raise SystemExit(f"FAIL: deployment status unavailable root={repo_root} err={type(exc).__name__}:{exc}") from exc
    return [line.rstrip() for line in output.splitlines() if line.strip()]


def git_cleanliness_status(repo_root: Path) -> tuple[str, list[str]]:
    entries = git_status_entries(repo_root)
    dirty_entries = [line for line in entries if not line.startswith("## ")]
    return ("CLEAN" if not dirty_entries else "DIRTY", dirty_entries)


def require_clean_git_worktree_or_fail(repo_root: Path) -> None:
    cleanliness, dirty_entries = git_cleanliness_status(repo_root)
    if cleanliness != "CLEAN":
        sample = dirty_entries[:10]
        raise SystemExit(
            "FAIL: immutable release build blocked by dirty worktree "
            f"root={repo_root} dirty_entry_count={len(dirty_entries)} sample={sample}"
        )


def _load_json_or_fail(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"FAIL: unreadable json path={path} err={type(exc).__name__}:{exc}") from exc


def required_startup_stack_presence(root: Path) -> tuple[bool, list[str]]:
    missing = [rel for rel in REQUIRED_STARTUP_STACK_FILES if not (root / rel).exists()]
    return (not missing, missing)


def bundled_file_hash_summary_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    file_hashes_raw = manifest.get("included_file_hashes")
    if not isinstance(file_hashes_raw, dict) or not file_hashes_raw:
        raise SystemExit("FAIL: release manifest missing included_file_hashes")
    normalized = {str(key): str(value).strip().lower() for key, value in file_hashes_raw.items()}
    aggregate = _sha256_bytes(
        json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return {
        "file_count": len(normalized),
        "aggregate_sha256": aggregate,
    }


def inspect_release_root(release_root: Path) -> ReleaseInspectionV1:
    resolved = Path(release_root).resolve()
    manifest_path = (resolved / "release_manifest.v1.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        raise SystemExit(f"FAIL: release manifest missing path={manifest_path}")
    manifest = _load_json_or_fail(manifest_path)
    release_id = str(manifest.get("release_id") or "").strip()
    if not release_id:
        raise SystemExit(f"FAIL: release manifest missing release_id path={manifest_path}")
    manifest_sha256 = sha256_file(manifest_path)
    summary = bundled_file_hash_summary_from_manifest(manifest)
    required_present, missing_required = required_startup_stack_presence(resolved)
    return ReleaseInspectionV1(
        release_id=release_id,
        release_root=resolved,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        bundled_file_hash_summary=summary,
        required_startup_stack_present=required_present,
        missing_required_files=missing_required,
    )


def resolve_live_service_fragment_path() -> Path:
    try:
        output = subprocess.check_output(
            ["systemctl", "--user", "show", "-p", "FragmentPath", LIVE_SERVICE_NAME],
            text=True,
        ).strip()
    except Exception:
        output = ""
    if output.startswith("FragmentPath="):
        candidate = Path(output.split("=", 1)[1]).expanduser().resolve()
        if candidate.exists() and candidate.is_file():
            return candidate
    fallback = LIVE_SERVICE_FALLBACK_PATH.expanduser().resolve()
    if fallback.exists() and fallback.is_file():
        return fallback
    raise SystemExit(f"FAIL: live service fragment path unavailable service={LIVE_SERVICE_NAME}")


def evaluate_service_resolution(service_unit_path: Path) -> dict[str, Any]:
    resolved_unit = Path(service_unit_path).expanduser().resolve()
    if not resolved_unit.exists() or not resolved_unit.is_file():
        raise SystemExit(f"FAIL: service unit missing path={resolved_unit}")
    service_text = resolved_unit.read_text(encoding="utf-8")
    match = _EXEC_PATH_RE.search(service_text)
    if not match:
        raise SystemExit(f"FAIL: service unit missing orchestrator launcher path path={resolved_unit}")
    launcher_path = Path(match.group(1)).expanduser()
    execution_root = launcher_path.parents[2]
    active_root_match = execution_root == ACTIVE_POINTER
    return {
        "service_unit_path": str(resolved_unit),
        "launcher_path": str(launcher_path.resolve()) if launcher_path.exists() else str(launcher_path),
        "resolved_execution_root": str(execution_root),
        "active_root_match": active_root_match,
    }


def load_active_runtime_contract_if_present() -> dict[str, Any] | None:
    if not ACTIVE_RUNTIME_CONTRACT_PATH.exists() or not ACTIVE_RUNTIME_CONTRACT_PATH.is_file():
        return None
    return _load_json_or_fail(ACTIVE_RUNTIME_CONTRACT_PATH)


def evaluate_post_activation_verification(*, release_root: Path) -> dict[str, Any]:
    release_inspection = inspect_release_root(release_root)
    if not ACTIVE_POINTER.exists() or not ACTIVE_POINTER.is_symlink():
        raise SystemExit(f"FAIL: active pointer missing or non-symlink path={ACTIVE_POINTER}")
    active_target = ACTIVE_POINTER.resolve()
    active_match = active_target == release_inspection.release_root
    live_execution = evaluate_service_resolution(resolve_live_service_fragment_path())
    contract = load_active_runtime_contract_if_present()
    contract_match = False
    contract_path = str(ACTIVE_RUNTIME_CONTRACT_PATH)
    if contract is not None:
        contract_match = (
            str(contract.get("release_root") or "").strip() == str(release_inspection.release_root)
            and str(contract.get("release_id") or "").strip() == release_inspection.release_id
            and str(contract.get("status") or "").strip().upper() == "ACTIVE"
        )
    passed = (
        active_match
        and release_inspection.required_startup_stack_present
        and live_execution["active_root_match"] is True
        and contract_match
    )
    blocking_codes: list[str] = []
    if not active_match:
        blocking_codes.append("ACTIVE_POINTER_TARGET_MISMATCH")
    if not release_inspection.required_startup_stack_present:
        blocking_codes.extend(
            f"REQUIRED_STARTUP_STACK_FILE_MISSING:{rel}" for rel in release_inspection.missing_required_files
        )
    if live_execution["active_root_match"] is not True:
        blocking_codes.append("LIVE_EXECUTION_ROOT_NOT_ACTIVE_POINTER")
    if not contract_match:
        blocking_codes.append("ACTIVE_RUNTIME_CONTRACT_MISMATCH")
    return {
        "passed": passed,
        "release_id": release_inspection.release_id,
        "release_root": str(release_inspection.release_root),
        "active_symlink_path": str(ACTIVE_POINTER),
        "active_symlink_target": str(active_target),
        "active_pointer_matches_release": active_match,
        "required_startup_stack_files_present": release_inspection.required_startup_stack_present,
        "missing_required_startup_stack_files": list(release_inspection.missing_required_files),
        "service_unit_path": live_execution["service_unit_path"],
        "launcher_path": live_execution["launcher_path"],
        "resolved_execution_root": live_execution["resolved_execution_root"],
        "active_root_match": bool(live_execution["active_root_match"]),
        "active_runtime_contract_path": contract_path,
        "active_runtime_contract_match": contract_match,
        "blocking_codes": blocking_codes,
    }


def classify_deployment_decision(
    *,
    authoritative_cleanliness_status: str,
    release_present: bool,
    required_startup_stack_files_present: bool,
    active_root_match: bool,
    live_execution_root_match: bool,
    runtime_copy_direct_exec_detected: bool,
    internal_error: str = "",
) -> tuple[str, list[str], str]:
    if internal_error:
        return (
            "DEPLOY_BLOCKED_BY_DEFECT",
            [internal_error],
            internal_error,
        )
    blocking_codes: list[str] = []
    if authoritative_cleanliness_status != "CLEAN":
        blocking_codes.append("AUTHORITATIVE_WORKTREE_DIRTY_BUILD_BLOCKED")
    if not release_present:
        blocking_codes.append("RELEASE_MISSING")
    if not required_startup_stack_files_present:
        blocking_codes.append("RELEASE_REQUIRED_STARTUP_STACK_INCOMPLETE")
    if not active_root_match:
        blocking_codes.append("ACTIVE_POINTER_MISMATCH")
    if not live_execution_root_match:
        blocking_codes.append("LIVE_EXECUTION_NOT_ACTIVE_ROOT")
    if runtime_copy_direct_exec_detected:
        blocking_codes.append("RUNTIME_COPY_DIRECT_EXECUTION_STILL_PRESENT")
    if not blocking_codes:
        return ("DEPLOY_ACTIVE", [], "")
    return ("DEPLOY_BLOCKED_VALID", blocking_codes, blocking_codes[0])
