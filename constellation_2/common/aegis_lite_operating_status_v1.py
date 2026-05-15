from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RELEASE_METADATA_PATH = Path("/home/node/constellation_runtime_data/truth/releases/current_release.v1.json")
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_lite_operating_status.v1.schema.json"
LITE_TIMER_NAME = "aegis-lite-eod-report-v1.timer"
LITE_SERVICE_NAME = "aegis-lite-eod-report-v1.service"
LITE_EOD_TARGET_TIME_ET = "15:50"
LITE_EOD_ON_CALENDAR = "OnCalendar=Mon..Fri *-*-* 15:50:00 America/New_York"
LEGACY_PAPER_TIMER_NAMES = (
    "c2-paper-day-orchestrator.timer",
    "aegis-paper-ready-kernel-v1.timer",
    "aegis-paper-ready-kernel-v1-after-orchestrator.timer",
    "c2-paper-auto-repair-controller.timer",
    "c2-paper-auto-repair-eod-final.timer",
)
LEGACY_DEFERRED_MARKER = "LEGACY_PAPER_AUTOMATION_DEFERRED"


def aegis_lite_operating_status_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_lite_operating_status_v1"
        / day_utc
        / "aegis_lite_operating_status.v1.json"
    )


def validate_aegis_lite_operating_status_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)


def write_aegis_lite_operating_status_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_aegis_lite_operating_status_v1(payload)
    path = aegis_lite_operating_status_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_aegis_lite_operating_status_v1(
    *,
    day_utc: str,
    generated_at_utc: str,
    truth_root: Path,
    report: dict[str, Any] | None = None,
    report_path: Path | None = None,
    queue_path: Path | None = None,
    repo_root: Path = REPO_ROOT,
    release_metadata_path: Path = DEFAULT_RELEASE_METADATA_PATH,
    source_artifact_lineage: list[dict[str, Any]] | None = None,
    repo_head_commit: str | None = None,
    active_release_commit: str | None = None,
    release_integrity_status: dict[str, Any] | None = None,
    legacy_paper_runtime_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    report_path_text = str(Path(report_path).resolve()) if report_path else ""
    queue_path_text = str(Path(queue_path).resolve()) if queue_path else ""
    repo_commit = repo_head_commit if repo_head_commit is not None else str((release_integrity_status or {}).get("repo_head_commit") or _repo_head_commit_v1(repo_root))
    release_commit = active_release_commit if active_release_commit is not None else str((release_integrity_status or {}).get("active_release_commit") or _active_release_commit_v1(release_metadata_path))
    match_status = str((release_integrity_status or {}).get("release_match_status") or _release_repo_match_status(repo_commit=repo_commit, release_commit=release_commit))
    timer_status = _lite_timer_status_v1(repo_root)
    legacy_status = _legacy_paper_timers_status_v1(repo_root)
    current_blockers = _current_blockers_v1(report=report, report_path=report_path, queue_path=queue_path)
    readiness = str((report or {}).get("readiness_classification") or "NOT_READY")
    if not report_path or not Path(report_path).exists():
        readiness = "NOT_READY"
    elif not queue_path or not Path(queue_path).exists():
        readiness = "NOT_READY" if readiness == "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING" else readiness
    if match_status == "MISMATCH" and readiness == "READY_FOR_SUPERVISED_MANUAL_PAPER_TRADING":
        readiness = "ADVISORY_ONLY"
    if match_status == "MISMATCH":
        current_blockers = _dedupe([*current_blockers, "ACTIVE_RELEASE_REPO_MISMATCH"])
    warnings = _dedupe(
        [
            *[str(item) for item in (report or {}).get("warnings", []) if str(item)],
            *[str(item) for item in (release_integrity_status or {}).get("reason_codes", []) if str(item)],
            *(["ACTIVE_RELEASE_REPO_MISMATCH"] if match_status == "MISMATCH" else []),
            *(["LITE_EOD_TIMER_NOT_CONFIGURED"] if timer_status.get("status") != "CONFIGURED" else []),
            *[
                f"LEGACY_TIMER_NOT_DEFERRED:{row['timer_name']}"
                for row in legacy_status
                if row.get("status") != "DEFERRED"
            ],
        ]
    )
    payload = {
        "schema_id": "aegis_lite_operating_status",
        "schema_version": "v1",
        "artifact_id": "aegis_lite_operating_status_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "truth_root": str(Path(truth_root).resolve()),
        "lite_eod_timer_status": timer_status,
        "lite_eod_latest_report_path": report_path_text,
        "lite_eod_latest_queue_path": queue_path_text,
        "legacy_paper_timers_status": legacy_status,
        "broker_submit_required": False,
        "manual_execution_only": True,
        "readiness_classification": readiness,
        "current_blockers": current_blockers,
        "active_release_commit": release_commit,
        "repo_head_commit": repo_commit,
        "release_repo_match_status": match_status,
        "working_repo_changes_inactive": match_status == "MISMATCH",
        "release_integrity_status": release_integrity_status or {},
        "legacy_paper_runtime_status": legacy_paper_runtime_status or {},
        "warnings": warnings,
        "source_artifact_lineage": source_artifact_lineage or [],
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def _lite_timer_status_v1(repo_root: Path) -> dict[str, Any]:
    timer_path = repo_root / "ops/systemd/user" / LITE_TIMER_NAME
    service_path = repo_root / "ops/systemd/user" / LITE_SERVICE_NAME
    timer_text = timer_path.read_text(encoding="utf-8") if timer_path.exists() else ""
    service_text = service_path.read_text(encoding="utf-8") if service_path.exists() else ""
    configured = (
        timer_path.exists()
        and service_path.exists()
        and LITE_EOD_ON_CALENDAR in timer_text
        and "manual-only" in service_text
        and "broker submit" in service_text.lower()
    )
    return {
        "timer_name": LITE_TIMER_NAME,
        "service_name": LITE_SERVICE_NAME,
        "status": "CONFIGURED" if configured else "MISSING_OR_MISCONFIGURED",
        "target_time_et": LITE_EOD_TARGET_TIME_ET,
        "calendar": LITE_EOD_ON_CALENDAR.replace("OnCalendar=", ""),
        "unit_path": str(timer_path),
        "service_path": str(service_path),
        "manual_execution_only": True,
        "broker_submit_required": False,
        "runs_from_active_release": "run_current_release_tool_v1.sh" in service_text,
        "no_transmit_automation": "transmit automation" in service_text.lower(),
    }


def _legacy_paper_timers_status_v1(repo_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for timer_name in LEGACY_PAPER_TIMER_NAMES:
        timer_path = repo_root / "ops/systemd/user" / timer_name
        service_name = timer_name.replace(".timer", ".service")
        service_path = repo_root / "ops/systemd/user" / service_name
        timer_text = timer_path.read_text(encoding="utf-8") if timer_path.exists() else ""
        service_text = service_path.read_text(encoding="utf-8") if service_path.exists() else ""
        deferred = (
            LEGACY_DEFERRED_MARKER in timer_text
            or LEGACY_DEFERRED_MARKER in service_text
            or "ConditionEnvironment=AEGIS_ENABLE_LEGACY_PAPER_AUTOMATION=1" in service_text
        )
        rows.append(
            {
                "timer_name": timer_name,
                "service_name": service_name,
                "status": "DEFERRED" if deferred else "ACTIVE_OR_UNDEFERRED",
                "timer_path": str(timer_path),
                "service_path": str(service_path),
                "timer_file_exists": timer_path.exists(),
                "service_file_exists": service_path.exists(),
                "legacy_paper_automation_deferred": deferred,
            }
        )
    return rows


def _current_blockers_v1(*, report: dict[str, Any] | None, report_path: Path | None, queue_path: Path | None) -> list[str]:
    blockers = [str(item) for item in (report or {}).get("do_not_trade_blockers", []) if str(item)]
    if not report_path or not Path(report_path).exists():
        blockers.append("NO_CURRENT_LITE_REPORT")
    if not queue_path or not Path(queue_path).exists():
        blockers.append("NO_CURRENT_LITE_QUEUE")
    return _dedupe(blockers)


def _repo_head_commit_v1(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _active_release_commit_v1(path: Path) -> str:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("commit") or "").strip()


def _release_repo_match_status(*, repo_commit: str, release_commit: str) -> str:
    if not repo_commit or not release_commit:
        return "UNKNOWN"
    return "MATCH" if repo_commit == release_commit else "MISMATCH"


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out
