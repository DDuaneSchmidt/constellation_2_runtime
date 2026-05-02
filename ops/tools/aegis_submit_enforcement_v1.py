from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from ops.tools.aegis_runtime_mode_v1 import production_version_path_v1, read_production_version_v1, runtime_mode_from_truth_root_v1
from ops.tools.run_aegis_control_plane_v1 import control_plane_self_binding_issues_v1
from ops.tools.run_aegis_promotion_validation_ledger_v1 import promotion_validation_ledger_path

REPO_ROOT = Path(__file__).resolve().parents[2]
READY_FINAL_STATUSES = {
    "READY",
    "PRE_MARKET_READY",
    "PAPER_READY",
    "PAPER_READY_WITH_DELAYED_DATA",
    "TRADING_ACTIVE",
    "EOD_COMPLETE",
}
BOUNDARY_PASS_STATUSES = {"PASS", "READY", "AUTHORIZED"}
REQUIRED_FRESH_ARTIFACT_TYPES = {
    "aegis_day_run_v1",
    "submit_boundary_status_v1",
    "action_validity_v1",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _git_commit() -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return str(proc.stdout or "").strip() if proc.returncode == 0 else ""


def _git_dirty_status() -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "status", "--short"],
        capture_output=True,
        text=True,
        check=False,
    )
    return "DIRTY" if str(proc.stdout or "").strip() else "CLEAN"


def _report_path(truth_root: Path, family: str, day_utc: str, filename: str) -> Path:
    return (truth_root / "reports" / family / day_utc / filename).resolve()


def _packet_path(runtime_root: Path | None = None) -> Path:
    root = runtime_root or Path("/home/node/constellation_runtime_data")
    return (root / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md").resolve()


def _packet_metadata(path: Path) -> dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    meta: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[:80]:
        text = line.strip()
        if not text.startswith("- "):
            continue
        body = text[2:]
        if ":" not in body:
            continue
        key, value = body.split(":", 1)
        meta[key.strip()] = value.strip()
    return meta


def packet_currentness_v1(*, runtime_root: Path | None = None, runtime_mode: str | None = None) -> dict[str, Any]:
    path = _packet_path(runtime_root)
    meta = _packet_metadata(path)
    current_commit = _git_commit()
    packet_commit = str(meta.get("git_commit") or "").strip()
    packet_mode = str(meta.get("runtime_mode") or "").strip().upper()
    expected_mode = str(runtime_mode or "").strip().upper()
    dirty = _git_dirty_status()
    mode_mismatch = bool(expected_mode and packet_mode and packet_mode != expected_mode)
    missing_mode = bool(expected_mode and not packet_mode)
    stale = not path.exists() or not packet_commit or packet_commit != current_commit or dirty != "CLEAN" or mode_mismatch or missing_mode
    return {
        "path": str(path),
        "exists": path.exists(),
        "generated_at_utc": str(meta.get("generated_at_utc") or ""),
        "runtime_mode": packet_mode,
        "expected_runtime_mode": expected_mode,
        "packet_git_commit": packet_commit,
        "current_git_commit": current_commit,
        "current_git_dirty_status": dirty,
        "status": "STALE" if stale else "CURRENT",
        "canonical_blocker": "AEGIS_PACKET_STALE" if stale else "",
    }


def _kill_switch_inactive(payload: dict[str, Any]) -> bool:
    state = str(payload.get("state") or payload.get("status") or "").strip().upper()
    if payload.get("active") is True or payload.get("kill_switch_active") is True:
        return False
    return state in {"INACTIVE", "CLEAR", "PASS"}


def _action_status(action_validity: dict[str, Any], action_id: str) -> str:
    rows = action_validity.get("action_rules") if isinstance(action_validity.get("action_rules"), list) else []
    for row in rows:
        if isinstance(row, dict) and str(row.get("action_id") or "").strip() == action_id:
            return str(row.get("status") or "UNKNOWN").strip().upper()
    return "UNKNOWN"


def _freshness_blockers(freshness: dict[str, Any]) -> list[dict[str, str]]:
    records = freshness.get("freshness_records") if isinstance(freshness.get("freshness_records"), list) else []
    by_type = {
        str(row.get("artifact_type") or ""): row
        for row in records
        if isinstance(row, dict) and str(row.get("artifact_type") or "").strip()
    }
    blockers: list[dict[str, str]] = []
    for artifact_type in sorted(REQUIRED_FRESH_ARTIFACT_TYPES):
        row = by_type.get(artifact_type)
        if not row:
            blockers.append({"code": "REQUIRED_AUTHORITY_FRESHNESS_MISSING", "artifact_type": artifact_type})
            continue
        status = str(row.get("freshness_status") or "UNKNOWN").strip().upper()
        if status not in {"FRESH", "NOT_REQUIRED"}:
            blockers.append(
                {
                    "code": "REQUIRED_AUTHORITY_NOT_FRESH",
                    "artifact_type": artifact_type,
                    "freshness_status": status,
                    "path": str(row.get("artifact_path") or ""),
                }
            )
    return blockers


def _promotion_validation_blockers(
    *,
    truth: Path,
    runtime_root: Path | None,
    day: str,
    current_commit: str,
    promoted_commit: str,
    packet: dict[str, Any],
    control_plane: dict[str, Any],
) -> tuple[Path, dict[str, Any], list[dict[str, str]]]:
    path = promotion_validation_ledger_path(truth_root=truth, day_utc=day)
    ledger = _read_json(path)
    blockers: list[dict[str, str]] = []
    if not ledger:
        blockers.append({"code": "PROMOTION_VALIDATION_LEDGER_MISSING", "path": str(path)})
        return path, ledger, blockers
    status = str(ledger.get("promotion_status") or "").strip().upper()
    ledger_commit = str(ledger.get("candidate_commit") or "").strip()
    if status not in {"APPROVED", "PROMOTED"}:
        blockers.append({"code": "PROMOTION_VALIDATION_LEDGER_NOT_APPROVED", "path": str(path), "promotion_status": status or "MISSING"})
    if ledger_commit != current_commit or ledger_commit != promoted_commit:
        blockers.append(
            {
                "code": "PROMOTION_VALIDATION_LEDGER_COMMIT_MISMATCH",
                "path": str(path),
                "ledger_candidate_commit": ledger_commit,
                "current_git_commit": current_commit,
                "promoted_commit": promoted_commit,
            }
        )
    if ledger.get("repo_clean") is not True:
        blockers.append({"code": "PROMOTION_VALIDATION_REPO_NOT_CLEAN", "path": str(path)})
    for field, code in (
        ("import_preflight_result", "PROMOTION_VALIDATION_IMPORT_PREFLIGHT_FAILED"),
        ("focused_test_result", "PROMOTION_VALIDATION_TESTS_FAILED"),
        ("registry_validation_result", "PROMOTION_VALIDATION_REGISTRY_FAILED"),
        ("schema_validation_result", "PROMOTION_VALIDATION_SCHEMA_FAILED"),
        ("control_plane_result", "PROMOTION_VALIDATION_CONTROL_PLANE_FAILED"),
    ):
        result = ledger.get(field) if isinstance(ledger.get(field), dict) else {}
        if result.get("status") != "PASS":
            blockers.append({"code": code, "path": str(path)})
    ledger_truth_root = str(ledger.get("truth_root") or "").strip()
    if not ledger_truth_root or Path(ledger_truth_root).expanduser().resolve() != truth.resolve():
        blockers.append({"code": "PROMOTION_VALIDATION_TRUTH_ROOT_MISMATCH", "path": str(path), "truth_root": ledger_truth_root})
    expected_runtime_root = Path(runtime_root).expanduser().resolve() if runtime_root is not None else truth.resolve().parent
    ledger_runtime_root = str(ledger.get("runtime_root") or "").strip()
    if not ledger_runtime_root or Path(ledger_runtime_root).expanduser().resolve() != expected_runtime_root:
        blockers.append(
            {
                "code": "PROMOTION_VALIDATION_RUNTIME_ROOT_MISMATCH",
                "path": str(path),
                "runtime_root": ledger_runtime_root,
                "expected_runtime_root": str(expected_runtime_root),
            }
        )
    if truth.resolve() != expected_runtime_root and truth.resolve().parent != expected_runtime_root:
        blockers.append(
            {
                "code": "PRODUCTION_TRUTH_RUNTIME_ROOT_MISMATCH",
                "truth_root": str(truth),
                "runtime_root": str(expected_runtime_root),
            }
        )
    packet_commit = str(packet.get("packet_git_commit") or "").strip()
    if packet_commit and promoted_commit and packet_commit != promoted_commit:
        blockers.append({"code": "PACKET_PROMOTED_COMMIT_MISMATCH", "path": str(packet.get("path") or ""), "packet_git_commit": packet_commit, "promoted_commit": promoted_commit})
    contract = control_plane.get("producer_contract_v1") if isinstance(control_plane.get("producer_contract_v1"), dict) else {}
    control_commit = str(contract.get("code_version_git_commit") or control_plane.get("source_git_commit") or "").strip()
    if not control_commit or control_commit != promoted_commit:
        blockers.append({"code": "CONTROL_PLANE_COMMIT_MISMATCH", "path": str(path), "control_plane_commit": control_commit or "MISSING", "promoted_commit": promoted_commit})
    dirty_status = str(contract.get("source_dirty_status") or control_plane.get("source_dirty_status") or "").strip().upper()
    if dirty_status and dirty_status != "CLEAN":
        blockers.append({"code": "CONTROL_PLANE_DIRTY_SOURCE", "source_dirty_status": dirty_status})
    outputs = contract.get("output_artifacts") if isinstance(contract.get("output_artifacts"), list) else []
    output_paths = [str(row.get("path") or "") for row in outputs if isinstance(row, dict)]
    if output_paths and not any(str(Path(item).expanduser().resolve()).startswith(str(truth.resolve()) + "/") for item in output_paths if item):
        blockers.append({"code": "CONTROL_PLANE_TRUTH_ROOT_MISMATCH", "truth_root": str(truth), "control_plane_outputs": ",".join(output_paths)})
    return path, ledger, blockers


def evaluate_submit_enforcement_v1(
    *,
    truth_root: Path,
    execution_root: Path,
    day_utc: str,
    action_id: str = "submit_paper_order",
    runtime_root: Path | None = None,
    runtime_mode: str | None = None,
) -> dict[str, Any]:
    truth = Path(truth_root).resolve()
    execution = Path(execution_root).resolve()
    day = str(day_utc).strip()
    mode = runtime_mode_from_truth_root_v1(truth, runtime_mode)
    paths = {
        "control_plane": _report_path(truth, "aegis_control_plane_v1", day, "control_plane.v1.json"),
        "day_run": _report_path(truth, "aegis_day_run_v1", day, "day_run.v1.json"),
        "submit_boundary": _report_path(truth, "submit_boundary_status_v1", day, "submit_boundary_status.v1.json"),
        "action_validity": _report_path(truth, "action_validity_v1", day, "action_validity.v1.json"),
        "truth_freshness": _report_path(truth, "truth_freshness_v1", day, "truth_freshness.v1.json"),
        "kill_switch": (execution / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json").resolve(),
    }
    control_plane = _read_json(paths["control_plane"])
    ledger = _read_json(paths["day_run"])
    boundary = _read_json(paths["submit_boundary"])
    action_validity = _read_json(paths["action_validity"])
    freshness = _read_json(paths["truth_freshness"])
    kill_switch = _read_json(paths["kill_switch"])
    blockers: list[dict[str, str]] = []
    for issue in control_plane_self_binding_issues_v1(control_plane, actual_path=paths["control_plane"]):
        blockers.append({key: str(value) for key, value in issue.items()})

    if mode != "PRODUCTION":
        blockers.append(
            {
                "code": "CANDIDATE_RUNTIME_SUBMIT_DISABLED",
                "runtime_mode": mode,
                "path": str(truth),
            }
        )
    elif truth.name != "production_truth" and "production_truth" not in set(truth.parts):
        blockers.append(
            {
                "code": "PRODUCTION_SUBMIT_REQUIRES_PRODUCTION_TRUTH",
                "path": str(truth),
            }
        )

    production_version_path = production_version_path_v1(truth)
    production_version = read_production_version_v1(truth)
    promoted_commit = str(production_version.get("promoted_commit") or "").strip()
    current_commit = _git_commit()
    if mode == "PRODUCTION":
        if not production_version:
            blockers.append({"code": "PRODUCTION_VERSION_MISSING", "path": str(production_version_path)})
        elif str(production_version.get("status") or "").strip().upper() != "ACTIVE":
            blockers.append(
                {
                    "code": "PRODUCTION_VERSION_NOT_ACTIVE",
                    "path": str(production_version_path),
                    "status": str(production_version.get("status") or "MISSING"),
                }
            )
        elif current_commit != promoted_commit:
            blockers.append(
                {
                    "code": "UNPROMOTED_PRODUCTION_COMMIT",
                    "path": str(production_version_path),
                    "current_git_commit": current_commit,
                    "promoted_commit": promoted_commit,
                }
            )

    control_final_status = str(control_plane.get("final_status") or "").strip().upper()
    control_submit_allowed = control_plane.get("submit_allowed") is True
    if control_final_status != "READY" or not control_submit_allowed:
        blockers.append(
            {
                "code": "CONTROL_PLANE_NOT_READY",
                "path": str(paths["control_plane"]),
                "final_status": control_final_status or "MISSING",
                "submit_allowed": str(control_submit_allowed),
                "canonical_blocker": str(control_plane.get("canonical_blocker") or ""),
            }
        )

    final_status = str(ledger.get("final_status") or "").strip().upper()
    if final_status not in READY_FINAL_STATUSES or str(ledger.get("canonical_blocker") or "").strip():
        blockers.append(
            {
                "code": "DAY_RUN_LEDGER_NOT_READY",
                "path": str(paths["day_run"]),
                "final_status": final_status or "MISSING",
                "canonical_blocker": str(ledger.get("canonical_blocker") or ""),
            }
        )

    boundary_status = str(boundary.get("status") or boundary.get("boundary_status") or "").strip().upper()
    boundary_allowed = boundary.get("submission_authorized") is True or boundary.get("submit_allowed") is True
    if boundary_status not in BOUNDARY_PASS_STATUSES or not boundary_allowed:
        blockers.append(
            {
                "code": "SUBMIT_BOUNDARY_NOT_PASS",
                "path": str(paths["submit_boundary"]),
                "status": boundary_status or "MISSING",
            }
        )

    if not kill_switch or not _kill_switch_inactive(kill_switch):
        blockers.append(
            {
                "code": "KILL_SWITCH_NOT_INACTIVE",
                "path": str(paths["kill_switch"]),
                "state": str(kill_switch.get("state") or kill_switch.get("status") or "MISSING"),
            }
        )

    action_status = _action_status(action_validity, action_id)
    if action_status != "ALLOWED":
        blockers.append(
            {
                "code": "ACTION_VALIDITY_FORBIDS_SUBMIT",
                "path": str(paths["action_validity"]),
                "action_id": action_id,
                "action_status": action_status,
            }
        )

    packet = packet_currentness_v1(runtime_root=runtime_root, runtime_mode=mode)
    if packet["status"] != "CURRENT":
        blockers.append(
            {
                "code": "AEGIS_PACKET_STALE",
                "path": str(packet["path"]),
                "packet_git_commit": str(packet["packet_git_commit"]),
                "current_git_commit": str(packet["current_git_commit"]),
            }
        )

    promotion_ledger_path = promotion_validation_ledger_path(truth_root=truth, day_utc=day)
    promotion_ledger: dict[str, Any] = {}
    if mode == "PRODUCTION" and promoted_commit:
        promotion_ledger_path, promotion_ledger, promotion_blockers = _promotion_validation_blockers(
            truth=truth,
            runtime_root=runtime_root,
            day=day,
            current_commit=current_commit,
            promoted_commit=promoted_commit,
            packet=packet,
            control_plane=control_plane,
        )
        blockers.extend(promotion_blockers)

    blockers.extend(_freshness_blockers(freshness))
    return {
        "ok": not blockers,
        "status": "PASS" if not blockers else "BLOCKED",
        "canonical_blocker": str(blockers[0]["code"]) if blockers else "",
        "blockers": blockers,
        "checked_paths": {key: str(path) for key, path in paths.items()},
        "runtime_mode": mode,
        "production_version_path": str(production_version_path),
        "production_promoted_commit": promoted_commit,
        "promotion_validation_ledger_path": str(promotion_ledger_path),
        "promotion_validation_status": str(promotion_ledger.get("promotion_status") or ""),
        "current_git_commit": current_commit,
        "packet_currentness": packet,
        "action_id": action_id,
    }


def require_submit_enforcement_v1(**kwargs: Any) -> dict[str, Any]:
    result = evaluate_submit_enforcement_v1(**kwargs)
    if not result["ok"]:
        raise SystemExit("FAIL_CLOSED: AEGIS_SUBMIT_ENFORCEMENT_BLOCKED " + json.dumps(result, sort_keys=True))
    return result
