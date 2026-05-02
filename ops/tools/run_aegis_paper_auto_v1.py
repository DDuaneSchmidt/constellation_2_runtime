#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_authority_graph_v1 import build_aegis_authority_graph_v1, write_aegis_authority_graph_v1
from constellation_2.common.aegis_daily_operator_summary_v1 import (
    build_aegis_daily_operator_summary_v1,
    write_aegis_daily_operator_summary_v1,
)
from constellation_2.common.aegis_day_evidence_ledger_v1 import (
    finalize_aegis_day_evidence_ledger_v1,
    new_aegis_day_evidence_ledger_v1,
    record_cycle_v1,
    utc_now_iso,
    write_aegis_day_evidence_ledger_v1,
)
from constellation_2.common.aegis_operating_contract_v1 import build_aegis_operating_contract_v1, write_aegis_operating_contract_v1
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_market_calendar_record_v1
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from ops.tools.aegis_submit_enforcement_v1 import evaluate_submit_enforcement_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry


BLOCKING_LIFECYCLE_STATES = {"LINEAGE_GAP", "FAILED", "REJECTED"}
SUBMIT_BOUNDARY_READY_STATES = {"READY", "AUTHORIZED"}
MARKET_TZ = ZoneInfo("America/New_York")
REGULAR_SESSION_OPEN_ET = "09:30:00"
REGULAR_SESSION_CLOSE_ET = "16:00:00"
TERMINAL_DAILY_OUTCOMES = {"SUCCESS_DRY_RUN", "SUCCESS_TRANSMITTED", "DRY_RUN_CLOSED"}


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _utc_now_dt() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _et_iso(dt: datetime) -> str:
    return dt.astimezone(MARKET_TZ).replace(microsecond=0).isoformat()


def _session_dt_from_record(*, day_utc: str, record: dict[str, Any], field_names: tuple[str, ...], default_time: str) -> datetime:
    for name in field_names:
        raw = str(record.get(name) or "").strip()
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        return parsed.astimezone(MARKET_TZ)
    return datetime.fromisoformat(f"{day_utc}T{default_time}").replace(tzinfo=MARKET_TZ)


def _default_day_utc_from_now(now_utc: datetime | None = None) -> str:
    now = now_utc or _utc_now_dt()
    return now.astimezone(MARKET_TZ).date().isoformat()


def _truth_path(truth_root: Path, family: str, day_utc: str, filename: str) -> Path:
    return (truth_root / "reports" / family / day_utc / filename).resolve()


def _runtime_service_status_path(runtime_root: Path) -> Path:
    return (runtime_root / "process_state" / "service_status.json").resolve()


def _dry_run_reset_marker_path_v1(*, execution_root: Path, day_utc: str) -> Path:
    return (
        execution_root
        / "execution_evidence_v1"
        / "dry_run_submission_reset_v1"
        / day_utc
        / "latest_reset.v1.json"
    ).resolve()


def _active_dry_run_reset_marker_v1(*, execution_root: Path, day_utc: str) -> dict[str, Any]:
    marker_path = _dry_run_reset_marker_path_v1(execution_root=execution_root, day_utc=day_utc)
    payload = _read_json(marker_path) or {}
    if str(payload.get("mode") or "").strip().upper() != "PAPER":
        return {}
    if str(payload.get("day_utc") or "").strip() != day_utc:
        return {}
    if str(payload.get("status") or "").strip().upper() != "ACTIVE":
        return {}
    if str(payload.get("scope") or "").strip().upper() != "NEXT_AUTO_PREFLIGHT_ONLY":
        return {}
    payload["path"] = str(marker_path)
    return payload


def _consume_dry_run_reset_marker_v1(
    *,
    execution_root: Path,
    day_utc: str,
    cycle_id: str,
) -> dict[str, Any]:
    marker_path = _dry_run_reset_marker_path_v1(execution_root=execution_root, day_utc=day_utc)
    payload = _read_json(marker_path) or {}
    if not payload:
        return {}
    payload["status"] = "CONSUMED"
    payload["consumed_at_utc"] = utc_now_iso()
    payload["consumed_by_cycle_id"] = cycle_id
    _write_json(marker_path, payload)
    payload["path"] = str(marker_path)
    return payload


def register_auto_runner_v1(*, runtime_root: Path, day_utc: str, active: bool = True) -> Path:
    path = _runtime_service_status_path(runtime_root)
    payload = _read_json(path) or {}
    services = payload.get("services") if isinstance(payload.get("services"), list) else []
    kept = [row for row in services if not (isinstance(row, dict) and str(row.get("name") or "") == "aegis_paper_auto_runner")]
    kept.append(
        {
            "name": "aegis_paper_auto_runner",
            "required": True,
            "state": "RUNNING" if active else "STOPPED",
            "pid": os.getpid() if active else None,
            "pid_running": bool(active),
            "day_utc": day_utc,
            "updated_utc": utc_now_iso(),
        }
    )
    payload["services"] = kept
    _write_json(path, payload)
    return path


def _run_command(name: str, cmd: list[str], *, env: dict[str, str]) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env, check=False)
    return {
        "name": name,
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip()[-4000:],
        "stderr": str(proc.stderr or "").strip()[-4000:],
    }


def _market_session_state(*, truth_root: Path, day_utc: str, now_utc: datetime | None = None) -> dict[str, Any]:
    now = now_utc or _utc_now_dt()
    calendar = resolve_market_calendar_record_v1(truth_root=truth_root, day_utc=day_utc)
    record = calendar.get("record") if isinstance(calendar.get("record"), dict) else {}
    active_session_path = (truth_root / "active_session_v1" / "current.json").resolve()
    paper_session_path = _truth_path(truth_root, "paper_session_authority_v1", day_utc, "paper_session_authority.v1.json")
    active_session = _read_json(active_session_path) or {}
    paper_session = _read_json(paper_session_path) or {}

    calendar_open = calendar.get("status") == "OK" and record.get("is_trading_session") is True
    paper_open = (
        str(paper_session.get("authority_status") or "").strip().upper() == "GRANTED"
        and paper_session.get("paper_open_allowed") is True
    )
    active_open = (
        not active_session
        or (
            str(active_session.get("active_day") or "").strip() == day_utc
            and str(active_session.get("rollover_status") or "").strip().upper()
            in {"ACTIVE_SESSION_CONFIRMED", "ROLLOVER_COMPLETED", ""}
        )
    )

    session_open_et = _session_dt_from_record(
        day_utc=day_utc,
        record=record,
        field_names=("market_open_utc", "session_open_utc", "open_utc", "market_open_et", "session_open_et", "open_et"),
        default_time=REGULAR_SESSION_OPEN_ET,
    )
    session_close_et = _session_dt_from_record(
        day_utc=day_utc,
        record=record,
        field_names=("market_close_utc", "session_close_utc", "close_utc", "market_close_et", "session_close_et", "close_et"),
        default_time=REGULAR_SESSION_CLOSE_ET,
    )
    now_et = now.astimezone(MARKET_TZ)
    if not calendar_open:
        window_state = "NON_TRADING_DAY"
        state = "MARKET_CLOSED"
        reason = str(calendar.get("reason_code") or "MARKET_CALENDAR_NOT_TRADING_SESSION")
    elif now_et < session_open_et:
        window_state = "BEFORE_OPEN"
        state = "MARKET_CLOSED"
        reason = "BEFORE_REGULAR_SESSION_OPEN"
    elif now_et > session_close_et:
        window_state = "AFTER_CLOSE"
        state = "MARKET_CLOSED"
        reason = "AFTER_REGULAR_SESSION_CLOSE"
    else:
        window_state = "IN_REGULAR_SESSION"
        state = "MARKET_OPEN"
        reason = "REGULAR_SESSION_OPEN"
    return {
        "evaluated_day_utc": day_utc,
        "state": state,
        "market_session_state": state,
        "reason": reason,
        "current_time_et": _et_iso(now),
        "session_open_et": _et_iso(session_open_et),
        "session_close_et": _et_iso(session_close_et),
        "calendar_status": calendar.get("status"),
        "calendar_reason_code": calendar.get("reason_code"),
        "calendar_is_trading_session": bool(record.get("is_trading_session") is True),
        "paper_session_authority_status": str(paper_session.get("authority_status") or ""),
        "paper_open_allowed": bool(paper_session.get("paper_open_allowed") is True),
        "paper_session_allows_submit": paper_open,
        "active_session_active_day": str(active_session.get("active_day") or ""),
        "active_session_rollover_status": str(active_session.get("rollover_status") or ""),
        "active_session_allows_day": active_open,
        "window_state": window_state,
        "input_paths": {
            "market_calendar_manifest": str(calendar.get("manifest_path") or ""),
            "market_calendar_year": str(calendar.get("year_path") or ""),
            "active_session_v1": str(active_session_path),
            "paper_session_authority_v1": str(paper_session_path),
        },
    }


def _released_candidates(execution_root: Path, day_utc: str) -> list[Path]:
    root = (execution_root / "phaseC_preflight_v1" / day_utc).resolve()
    if not root.exists() or not root.is_dir():
        return []
    out: list[Path] = []
    for attempt_dir in sorted([path for path in root.iterdir() if path.is_dir()], reverse=True):
        for candidate in sorted([path for path in attempt_dir.iterdir() if path.is_dir()]):
            identity = _read_json(candidate / "execution_identity_record.v1.json") or {}
            decision = _read_json(candidate / "submit_preflight_decision.v1.json") or {}
            if identity and str(decision.get("decision") or "").strip().upper() in {"ALLOW", "RELEASE", "RELEASED"}:
                out.append(candidate.resolve())
        if out:
            break
    return out


def _candidate_submission_id(candidate: Path) -> str:
    for name in ("execution_identity_record.v1.json", "binding_record.v1.json", "binding_record.v2.json"):
        payload = _read_json(candidate / name) or {}
        value = str(payload.get("submission_id") or "").strip()
        if value:
            return value
    return candidate.name


def _submission_evidence(execution_root: Path, day_utc: str, submission_id: str) -> dict[str, Any]:
    subdir = execution_root / "execution_evidence_v1" / "submissions" / day_utc / submission_id
    broker = _read_json(subdir / "broker_submission_record.v2.json") or {}
    attempt = _read_json(subdir / "broker_submit_attempt_v1.json") or {}
    error = broker.get("error") if isinstance(broker.get("error"), dict) else {}
    broker_ids = broker.get("broker_ids") if isinstance(broker.get("broker_ids"), dict) else {}
    dry_run_complete = attempt.get("dry_run") is True or str(error.get("code") or "").strip().upper() == "DRY_RUN_NO_BROKER_ID"
    transmitted = (
        broker.get("broker_transmitted") is True
        or bool(broker_ids.get("order_id"))
        or bool(broker_ids.get("perm_id"))
    )
    return {
        "submission_id": submission_id,
        "submission_dir": str(subdir.resolve()),
        "dry_run_complete": dry_run_complete,
        "transmitted": transmitted,
        "exists": bool(broker or attempt),
    }


def _duplicate_guard(execution_root: Path, day_utc: str, submission_ids: list[str]) -> dict[str, Any]:
    matches = [
        evidence
        for evidence in (_submission_evidence(execution_root, day_utc, sid) for sid in submission_ids)
        if evidence["dry_run_complete"] or evidence["transmitted"]
    ]
    return {"duplicate": bool(matches), "matches": matches}


def _blocking_lifecycle_guard(execution_root: Path, day_utc: str, submission_ids: list[str]) -> dict[str, Any]:
    path = execution_root / "reports" / "execution_lifecycle_authority_v1" / day_utc / "execution_lifecycle_authority.v1.json"
    payload = _read_json(path) or {}
    rows = payload.get("submissions") if isinstance(payload.get("submissions"), list) else []
    blocking = []
    wanted = set(submission_ids)
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("submission_id") or "") not in wanted:
            continue
        state = str(row.get("current_lifecycle_state") or "").strip().upper()
        if state in BLOCKING_LIFECYCLE_STATES:
            blocking.append({"submission_id": str(row.get("submission_id") or ""), "state": state})
    return {"blocked": bool(blocking), "blocking": blocking, "path": str(path.resolve())}


def evaluate_auto_submit_guard_v1(
    *,
    truth_root: Path,
    execution_root: Path,
    day_utc: str,
    market_session: dict[str, Any],
    dry_run_only: bool,
    runtime_root: Path | None = None,
) -> dict[str, Any]:
    blockers: list[dict[str, Any]] = []
    aegis_gate = evaluate_submit_enforcement_v1(
        truth_root=truth_root,
        execution_root=execution_root,
        day_utc=day_utc,
        action_id="submit_paper_order",
        runtime_root=runtime_root,
    )
    if not aegis_gate["ok"]:
        blockers.extend(dict(row) for row in aegis_gate["blockers"])
    if market_session.get("state") != "MARKET_OPEN":
        blockers.append({"code": "MARKET_SESSION_NOT_OPEN", "state": str(market_session.get("state") or "")})

    day_authority_path = _truth_path(truth_root, "paper_trading_day_authority_v1", day_utc, "paper_trading_day_authority.v1.json")
    day_authority = _read_json(day_authority_path) or {}
    if str(day_authority.get("state") or "").strip().upper() != "OPEN_READY":
        blockers.append({"code": "PAPER_DAY_AUTHORITY_NOT_OPEN_READY", "path": str(day_authority_path)})
    if day_authority.get("can_submit_paper_orders") is not True:
        blockers.append({"code": "PAPER_DAY_AUTHORITY_CANNOT_SUBMIT", "path": str(day_authority_path)})

    boundary_path = _truth_path(truth_root, "submit_boundary_status_v1", day_utc, "submit_boundary_status.v1.json")
    boundary = _read_json(boundary_path) or {}
    boundary_status = str(boundary.get("status") or boundary.get("boundary_status") or "").strip().upper()
    boundary_authorized = bool(
        boundary.get("submission_authorized") is True
        or boundary.get("submit_allowed") is True
        or str(boundary.get("decision") or boundary.get("authorization_status") or "").strip().upper() == "AUTHORIZED"
    )
    if boundary_status not in SUBMIT_BOUNDARY_READY_STATES or not boundary_authorized:
        blockers.append({"code": "SUBMIT_BOUNDARY_NOT_READY_AUTHORIZED", "path": str(boundary_path), "status": boundary_status})

    execution_mode_path = _truth_path(truth_root, "execution_mode_authority_v1", day_utc, "execution_mode_authority.v1.json")
    execution_mode = _read_json(execution_mode_path) or {}
    mode_state = str(execution_mode.get("mode_state") or execution_mode.get("mode") or "").strip().upper()
    if mode_state == "PAPER_TRANSMIT_ENABLED" and dry_run_only:
        blockers.append({"code": "AUTO_DRY_RUN_ONLY_BLOCKS_PAPER_TRANSMIT", "path": str(execution_mode_path)})
    elif mode_state not in {"DRY_RUN_LOCKED", "PAPER_READY_NOT_TRANSMITTED", "PAPER_TRANSMIT_ENABLED"}:
        blockers.append({"code": "EXECUTION_MODE_NOT_EXPLICIT_SAFE_PAPER_STATE", "path": str(execution_mode_path), "mode_state": mode_state})

    kill_path = execution_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json"
    kill = _read_json(kill_path) or {}
    if kill.get("active") is True or str(kill.get("state") or kill.get("status") or "").strip().upper() not in {"INACTIVE", "CLEAR", "PASS"}:
        blockers.append({"code": "GLOBAL_KILL_SWITCH_ACTIVE", "path": str(kill_path)})

    runtime_path = _truth_path(truth_root, "runtime_service_authority_v1", day_utc, "runtime_service_authority.v1.json")
    runtime = _read_json(runtime_path) or {}
    if str(runtime.get("status") or "").strip().upper() != "PASS" or runtime.get("auto_runner_running") is not True:
        blockers.append({"code": "RUNTIME_SERVICE_AUTHORITY_AUTO_NOT_READY", "path": str(runtime_path)})

    candidates = _released_candidates(execution_root, day_utc)
    submission_ids = [_candidate_submission_id(path) for path in candidates]
    if not candidates:
        blockers.append({"code": "PHASEC_RELEASED_CANDIDATE_MISSING", "path": str(execution_root / "phaseC_preflight_v1" / day_utc)})

    duplicate = _duplicate_guard(execution_root, day_utc, submission_ids)
    if duplicate["duplicate"]:
        blockers.insert(0, {"code": "AUTO_SKIPPED_ALREADY_SUBMITTED", "matches": duplicate["matches"]})

    lifecycle = _blocking_lifecycle_guard(execution_root, day_utc, submission_ids)
    if lifecycle["blocked"]:
        blockers.append({"code": "UNRESOLVED_BLOCKING_LIFECYCLE_STATE", "blocking": lifecycle["blocking"], "path": lifecycle["path"]})

    return {
        "ok": not blockers,
        "blockers": blockers,
        "aegis_submit_enforcement": aegis_gate,
        "candidate_paths": [str(path) for path in candidates],
        "submission_ids": submission_ids,
        "duplicate_guard": duplicate,
        "lifecycle_guard": lifecycle,
        "authorities": {
            "paper_trading_day_authority_v1": str(day_authority_path),
            "submit_boundary_status_v1": str(boundary_path),
            "execution_mode_authority_v1": str(execution_mode_path),
            "runtime_service_authority_v1": str(runtime_path),
            "global_kill_switch_state_v1": str(kill_path),
        },
    }


def _load_or_new_ledger(path: Path, *, day_utc: str, mode: str) -> dict[str, Any]:
    payload = _read_json(path)
    if payload:
        payload.setdefault("cycles", [])
        payload["mode"] = mode
        payload["run_style"] = "AUTO"
        return payload
    return new_aegis_day_evidence_ledger_v1(day_utc=day_utc, mode=mode, run_style="AUTO")


def _final_outcome(
    *,
    execution_root: Path,
    day_utc: str,
    market_state: str,
    guard: dict[str, Any],
    submit_attempted: bool,
    existing_outcome: str = "",
) -> str:
    existing = str(existing_outcome or "").strip().upper()
    if existing in TERMINAL_DAILY_OUTCOMES:
        return existing
    submit_mode = classify_paper_submit_mode_status_v1(execution_root=execution_root, day_utc=day_utc)
    if str(submit_mode.get("submit_mode_status") or "").strip().upper() == "DRY_RUN_COMPLETE":
        return "SUCCESS_DRY_RUN"
    if submit_mode.get("broker_order_transmitted") is True:
        return "SUCCESS_TRANSMITTED"
    if market_state == "MARKET_CLOSED":
        return "NO_INTENT_EXPECTED"
    if submit_attempted:
        return "FAILED_WITH_OWNER"
    if any(str(row.get("code") or "") == "AUTO_SKIPPED_ALREADY_SUBMITTED" for row in guard.get("blockers", [])):
        return "SUCCESS_DRY_RUN"
    return "BLOCKED_WITH_REASON"


def _refresh_operator_outputs(*, truth_root: Path, execution_root: Path, day_utc: str, contract: dict[str, Any], ledger: dict[str, Any]) -> dict[str, str]:
    graph = build_aegis_authority_graph_v1(day_utc=day_utc, truth_root=truth_root, execution_root=execution_root, repo_root=REPO_ROOT)
    graph_path = write_aegis_authority_graph_v1(truth_root=truth_root, day_utc=day_utc, payload=graph)
    summary = build_aegis_daily_operator_summary_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        execution_root=execution_root,
        operating_contract=contract,
        authority_graph=graph,
        evidence_ledger=ledger,
    )
    summary_path = write_aegis_daily_operator_summary_v1(truth_root=truth_root, day_utc=day_utc, payload=summary)
    return {"aegis_authority_graph_v1": str(graph_path), "aegis_daily_operator_summary_v1": str(summary_path)}


def _cycle(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    runtime_root: Path,
    mode: str,
    dry_run_only: bool,
    poll_seconds: int,
    cycle_number: int,
    env: dict[str, str],
) -> dict[str, Any]:
    cycle_start = _utc_now_dt()
    cycle_id = f"AUTO-{day_utc}-{cycle_number:04d}"
    actions: list[str] = []
    command_results: list[dict[str, Any]] = []
    ledger_path = truth_root / "reports" / "aegis_day_evidence_ledger_v1" / day_utc / "aegis_day_evidence_ledger.v1.json"
    ledger = _load_or_new_ledger(ledger_path, day_utc=day_utc, mode=mode)

    contract = build_aegis_operating_contract_v1(day_utc=day_utc, mode=mode, run_style="AUTO")
    contract_path = write_aegis_operating_contract_v1(truth_root=truth_root, day_utc=day_utc, payload=contract)
    actions.append("REFRESHED_OPERATING_CONTRACT")

    market = _market_session_state(truth_root=truth_root, day_utc=day_utc)
    if market["state"] == "MARKET_CLOSED":
        next_wake = _iso(cycle_start + timedelta(seconds=poll_seconds))
        outcome = _final_outcome(
            execution_root=execution_root,
            day_utc=day_utc,
            market_state="MARKET_CLOSED",
            guard={},
            submit_attempted=False,
            existing_outcome=str(ledger.get("final_no_silent_day_outcome") or ""),
        )
        record_cycle_v1(
            ledger,
            cycle_id=cycle_id,
            started_at_utc=_iso(cycle_start),
            ended_at_utc=utc_now_iso(),
            market_session_state="MARKET_CLOSED",
            authorities_before=market,
            actions_taken=actions,
            submit_attempted=False,
            skip_reason="MARKET_CLOSED",
            outputs={
                "aegis_operating_contract_v1": str(contract_path),
                "cycle_outcome": "MARKET_CLOSED",
                "final_daily_outcome": outcome,
            },
            blockers=[],
            next_wake_at_utc=next_wake,
        )
        ledger = finalize_aegis_day_evidence_ledger_v1(
            ledger,
            final_daily_outcome=outcome,
            diagnostics=[{"code": "MARKET_CLOSED", "market_session": market}],
            artifact_paths={"aegis_operating_contract_v1": str(contract_path), "aegis_day_evidence_ledger_v1": str(ledger_path)},
        )
        operator_outputs = _refresh_operator_outputs(truth_root=truth_root, execution_root=execution_root, day_utc=day_utc, contract=contract, ledger=ledger)
        ledger["artifact_paths"].update(operator_outputs)
        write_aegis_day_evidence_ledger_v1(truth_root=truth_root, day_utc=day_utc, payload=ledger)
        return {
            "state": "MARKET_CLOSED",
            "cycle_id": cycle_id,
            "ledger_path": str(ledger_path),
            "submit_attempted": False,
            "submit_eligibility_evaluated": False,
            "actions": actions,
            "evaluated_day_utc": day_utc,
            "market_session_state": "MARKET_CLOSED",
            "reason": str(market.get("reason") or ""),
            "current_time_et": str(market.get("current_time_et") or ""),
            "session_open_et": str(market.get("session_open_et") or ""),
            "session_close_et": str(market.get("session_close_et") or ""),
            "latest_auto_cycle_outcome": "MARKET_CLOSED",
            "no_silent_day_outcome": outcome,
        }

    reset_marker = _active_dry_run_reset_marker_v1(execution_root=execution_root, day_utc=day_utc)
    preflight_cmd = ["npm", "run", "aegis:paper:preflight", "--", "--day_utc", day_utc, "--truth_root", str(truth_root)]
    if reset_marker:
        preflight_cmd.append("--reset-dry-run-submission-evidence")
        actions.append("AUTO_APPLIED_OPERATOR_DRY_RUN_RESET")
    pre_commands = [
        ("aegis_paper_preflight", preflight_cmd),
        (
            "runtime_service_authority_v1",
            [
                sys.executable,
                "ops/tools/run_runtime_service_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--runtime_root",
                str(runtime_root),
                "--expected_run_mode",
                "AUTOMATIC",
            ],
        ),
        (
            "execution_mode_authority_v1",
            [
                sys.executable,
                "ops/tools/run_execution_mode_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_root),
                "--environment",
                "PAPER",
            ],
        ),
    ]
    for name, cmd in pre_commands:
        command_results.append(_run_command(name, cmd, env=env))
        actions.append(f"RAN:{name}")

    guard = evaluate_auto_submit_guard_v1(
        truth_root=truth_root,
        execution_root=execution_root,
        day_utc=day_utc,
        market_session=market,
        dry_run_only=dry_run_only,
        runtime_root=runtime_root,
    )
    submit_attempted = False
    skip_reason = ""
    if guard["ok"]:
        submit_attempted = True
        command_results.append(_run_command("aegis_paper_submit", ["npm", "run", "aegis:paper:submit", "--", "--day_utc", day_utc, "--truth_root", str(truth_root)], env=env))
        actions.append("SUBMIT_ATTEMPTED")
    else:
        skip_reason = str((guard.get("blockers") or [{}])[0].get("code") or "AUTO_GUARD_BLOCKED")
        actions.append(skip_reason)

    post_commands = [
        (
            "execution_lifecycle_authority_v1",
            [
                sys.executable,
                "ops/tools/run_execution_lifecycle_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_root),
                "--environment",
                "PAPER",
            ],
        ),
        (
            "execution_mode_authority_v1.final",
            [
                sys.executable,
                "ops/tools/run_execution_mode_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--execution_root",
                str(execution_root),
                "--environment",
                "PAPER",
            ],
        ),
        (
            "runtime_service_authority_v1.final",
            [
                sys.executable,
                "ops/tools/run_runtime_service_authority_v1.py",
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--runtime_root",
                str(runtime_root),
                "--expected_run_mode",
                "AUTOMATIC",
            ],
        ),
        ("trading_day_closure_authority_v1", [sys.executable, "ops/tools/run_trading_day_closure_authority_v1.py", "--day_utc", day_utc, "--truth_root", str(truth_root), "--execution_root", str(execution_root)]),
        ("aegis_chatgpt_packet", ["npm", "run", "aegis:chatgpt:packet"]),
        ("aegis_chatgpt_show", ["npm", "run", "aegis:chatgpt:show"]),
    ]
    for name, cmd in post_commands:
        command_results.append(_run_command(name, cmd, env=env))
        actions.append(f"RAN:{name}")

    consumed_reset_marker = {}
    if reset_marker:
        consumed_reset_marker = _consume_dry_run_reset_marker_v1(
            execution_root=execution_root,
            day_utc=day_utc,
            cycle_id=cycle_id,
        )
        actions.append("CONSUMED_OPERATOR_DRY_RUN_RESET")

    outputs = {"aegis_operating_contract_v1": str(contract_path), "commands": command_results}
    if reset_marker:
        outputs["operator_dry_run_reset"] = {
            "marker_path": str(reset_marker.get("path") or ""),
            "consumed_marker_path": str(consumed_reset_marker.get("path") or ""),
        }
    outcome = _final_outcome(
        execution_root=execution_root,
        day_utc=day_utc,
        market_state=str(market["state"]),
        guard=guard,
        submit_attempted=submit_attempted,
        existing_outcome=str(ledger.get("final_no_silent_day_outcome") or ""),
    )
    next_wake = _iso(cycle_start + timedelta(seconds=poll_seconds))
    record_cycle_v1(
        ledger,
        cycle_id=cycle_id,
        started_at_utc=_iso(cycle_start),
        ended_at_utc=utc_now_iso(),
        market_session_state=str(market["state"]),
        authorities_before=guard.get("authorities", {}),
        actions_taken=actions,
        submit_attempted=submit_attempted,
        skip_reason=skip_reason,
        outputs=outputs,
        blockers=list(guard.get("blockers") or []),
        next_wake_at_utc=next_wake,
    )
    ledger = finalize_aegis_day_evidence_ledger_v1(
        ledger,
        final_daily_outcome=outcome,
        blockers=list(guard.get("blockers") or []),
        diagnostics=[{"code": "AUTO_CYCLE", "cycle_id": cycle_id}],
        artifact_paths={"aegis_operating_contract_v1": str(contract_path), "aegis_day_evidence_ledger_v1": str(ledger_path)},
    )
    operator_outputs = _refresh_operator_outputs(truth_root=truth_root, execution_root=execution_root, day_utc=day_utc, contract=contract, ledger=ledger)
    ledger["artifact_paths"].update(operator_outputs)
    write_aegis_day_evidence_ledger_v1(truth_root=truth_root, day_utc=day_utc, payload=ledger)
    return {
        "state": str(market["state"]),
        "cycle_id": cycle_id,
        "ledger_path": str(ledger_path),
        "submit_attempted": submit_attempted,
        "submit_eligibility_evaluated": True,
        "skip_reason": skip_reason,
        "actions": actions,
        "evaluated_day_utc": day_utc,
        "market_session_state": str(market["state"]),
        "reason": str(market.get("reason") or ""),
        "current_time_et": str(market.get("current_time_et") or ""),
        "session_open_et": str(market.get("session_open_et") or ""),
        "session_close_et": str(market.get("session_close_et") or ""),
        "latest_auto_cycle_outcome": "SUBMIT_ATTEMPTED" if submit_attempted else skip_reason,
        "duplicate_guard": guard.get("duplicate_guard", {}),
        "broker_transmit_enabled": mode == "PAPER_TRANSMIT" and not dry_run_only,
        "no_silent_day_outcome": outcome,
    }


def _bool_env(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_aegis_paper_auto_v1")
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc) if str(args.day_utc or "").strip() else _default_day_utc_from_now()
    run_style = str(os.environ.get("AEGIS_RUN_STYLE") or "").strip().upper()
    if run_style != "AUTO":
        print(json.dumps({"state": "AUTO_NOT_ENABLED", "required": "AEGIS_RUN_STYLE=AUTO"}, sort_keys=True))
        return 2

    dry_run_only = _bool_env("AEGIS_AUTO_DRY_RUN_ONLY", True)
    requested_mode = str(os.environ.get("AEGIS_OPERATING_MODE") or "DRY_RUN").strip().upper()
    mode = "PAPER_TRANSMIT" if requested_mode == "PAPER_TRANSMIT" and not dry_run_only else "DRY_RUN"
    poll_seconds = int(str(os.environ.get("AEGIS_AUTO_POLL_SECONDS") or "300").strip())
    max_cycles_raw = str(os.environ.get("AEGIS_AUTO_MAX_CYCLES") or "").strip()
    max_cycles = int(max_cycles_raw) if max_cycles_raw else 0
    truth_root = resolve_decision_truth_root_bridge_v1(args.truth_root or "", repo_root=REPO_ROOT, caller="ops/tools/run_aegis_paper_auto_v1.py")
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else (REPO_ROOT / "runtime").resolve()
    account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_root = resolve_sleeve_execution_root_v1(repo_root=REPO_ROOT, environment="PAPER", ib_account=account, sleeve_id="PRIMARY").execution_root_path.resolve()
    env = dict(os.environ)
    env["AEGIS_RUN_STYLE"] = "AUTO"
    if mode == "DRY_RUN":
        env["C2_GOVERNED_SUBMIT_DRY_RUN"] = "YES"
        env["C2_EXECUTION_MODE"] = "DRY_RUN"
    else:
        env["C2_GOVERNED_SUBMIT_DRY_RUN"] = "NO"
        env["C2_EXECUTION_MODE"] = "PAPER_TRANSMIT"

    register_auto_runner_v1(runtime_root=runtime_root, day_utc=day_utc, active=True)
    cycle_count = 0
    last_result: dict[str, Any] = {}
    try:
        while True:
            cycle_count += 1
            last_result = _cycle(
                day_utc=day_utc,
                truth_root=truth_root,
                execution_root=execution_root,
                runtime_root=runtime_root,
                mode=mode,
                dry_run_only=dry_run_only,
                poll_seconds=poll_seconds,
                cycle_number=cycle_count,
                env=env,
            )
            if last_result.get("state") == "MARKET_CLOSED":
                break
            if max_cycles and cycle_count >= max_cycles:
                break
            time.sleep(poll_seconds)
    finally:
        register_auto_runner_v1(runtime_root=runtime_root, day_utc=day_utc, active=False)

    report = {
        "state": last_result.get("state", "UNKNOWN"),
        "evaluated_day_utc": str(last_result.get("evaluated_day_utc") or day_utc),
        "run_style": "AUTO",
        "service_model": "foreground_loop_only",
        "cycle_count": cycle_count,
        "current_time_et": str(last_result.get("current_time_et") or ""),
        "session_open_et": str(last_result.get("session_open_et") or ""),
        "session_close_et": str(last_result.get("session_close_et") or ""),
        "market_session_state": str(last_result.get("market_session_state") or last_result.get("state") or "UNKNOWN"),
        "reason": str(last_result.get("reason") or ""),
        "actions_taken": last_result.get("actions", []),
        "submit_eligibility_evaluated": bool(last_result.get("submit_eligibility_evaluated") is True),
        "submit_attempted": bool(last_result.get("submit_attempted") is True),
        "duplicate_guard": last_result.get("duplicate_guard", {}),
        "broker_transmit_enabled": bool(last_result.get("broker_transmit_enabled") is True),
        "evidence_ledger_path": str(last_result.get("ledger_path") or ""),
        "latest_auto_cycle_outcome": str(last_result.get("latest_auto_cycle_outcome") or ""),
        "final_no_silent_day_outcome": str(last_result.get("no_silent_day_outcome") or ""),
    }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
