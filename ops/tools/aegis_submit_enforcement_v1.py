from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


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


def packet_currentness_v1(*, runtime_root: Path | None = None) -> dict[str, Any]:
    path = _packet_path(runtime_root)
    meta = _packet_metadata(path)
    current_commit = _git_commit()
    packet_commit = str(meta.get("git_commit") or "").strip()
    dirty = _git_dirty_status()
    stale = not path.exists() or not packet_commit or packet_commit != current_commit or dirty != "CLEAN"
    return {
        "path": str(path),
        "exists": path.exists(),
        "generated_at_utc": str(meta.get("generated_at_utc") or ""),
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


def evaluate_submit_enforcement_v1(
    *,
    truth_root: Path,
    execution_root: Path,
    day_utc: str,
    action_id: str = "submit_paper_order",
    runtime_root: Path | None = None,
) -> dict[str, Any]:
    truth = Path(truth_root).resolve()
    execution = Path(execution_root).resolve()
    day = str(day_utc).strip()
    paths = {
        "day_run": _report_path(truth, "aegis_day_run_v1", day, "day_run.v1.json"),
        "submit_boundary": _report_path(truth, "submit_boundary_status_v1", day, "submit_boundary_status.v1.json"),
        "action_validity": _report_path(truth, "action_validity_v1", day, "action_validity.v1.json"),
        "truth_freshness": _report_path(truth, "truth_freshness_v1", day, "truth_freshness.v1.json"),
        "kill_switch": (execution / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json").resolve(),
    }
    ledger = _read_json(paths["day_run"])
    boundary = _read_json(paths["submit_boundary"])
    action_validity = _read_json(paths["action_validity"])
    freshness = _read_json(paths["truth_freshness"])
    kill_switch = _read_json(paths["kill_switch"])
    blockers: list[dict[str, str]] = []

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

    packet = packet_currentness_v1(runtime_root=runtime_root)
    if packet["status"] != "CURRENT":
        blockers.append(
            {
                "code": "AEGIS_PACKET_STALE",
                "path": str(packet["path"]),
                "packet_git_commit": str(packet["packet_git_commit"]),
                "current_git_commit": str(packet["current_git_commit"]),
            }
        )

    blockers.extend(_freshness_blockers(freshness))
    return {
        "ok": not blockers,
        "status": "PASS" if not blockers else "BLOCKED",
        "canonical_blocker": str(blockers[0]["code"]) if blockers else "",
        "blockers": blockers,
        "checked_paths": {key: str(path) for key, path in paths.items()},
        "packet_currentness": packet,
        "action_id": action_id,
    }


def require_submit_enforcement_v1(**kwargs: Any) -> dict[str, Any]:
    result = evaluate_submit_enforcement_v1(**kwargs)
    if not result["ok"]:
        raise SystemExit("FAIL_CLOSED: AEGIS_SUBMIT_ENFORCEMENT_BLOCKED " + json.dumps(result, sort_keys=True))
    return result
