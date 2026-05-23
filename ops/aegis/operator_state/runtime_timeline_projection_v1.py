from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ops.aegis.market_data.freshness_policy_v1 import resolve_market_session_v1, vendor_lag_minutes_from_env_v1
from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1, domain_certification_report_path_v1
from ops.aegis.operator_state.schedule_event_ledger_v1 import (
    DIAGNOSTIC_EVENT,
    DIAGNOSTICS_ONLY,
    HIDDEN,
    INTERNAL_INFRASTRUCTURE_EVENT,
    OPERATOR_WORKFLOW_EVENT,
    PRIMARY_OPERATOR_TIMELINE,
    build_schedule_event_ledger_v1,
    schedule_event_ledger_path_v1,
)

SCHEMA_ID = "aegis_runtime_timeline_projection"
SCHEMA_VERSION = "v1"
NY_TZ = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROVIDER_SLA_MINUTES = 60
DEFAULT_ESCALATION_THRESHOLD_MINUTES = 120
DEFAULT_RETRY_EXHAUSTION_THRESHOLD = 3


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).replace(microsecond=0)
    except ValueError:
        return None


def _iso(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if path.exists() and path.is_file():
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
    except Exception:
        return {}
    return {}


def _artifact_ref(path: Path, artifact_id: str) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": exists,
        "sha256": _sha256_file(path) if exists else "",
    }


def _artifact_timestamp(path: Path, payload: dict[str, Any]) -> str:
    for key in (
        "generated_at_utc",
        "produced_at_utc",
        "created_at_utc",
        "updated_at_utc",
        "certified_at_utc",
        "final_eod_certified_at_utc",
    ):
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    if path.exists() and path.is_file():
        return _iso(datetime.fromtimestamp(path.stat().st_mtime, tz=UTC))
    return ""


def _status_from_artifact(path: Path, payload: dict[str, Any]) -> str:
    if not path.exists():
        return "WAITING"
    text = " ".join(
        str(payload.get(key) or "")
        for key in ("status", "validation_status", "final_eod_certification_status", "certification_state")
    ).upper()
    if any(token in text for token in ("FAIL", "FAILED", "INVALID", "REJECTED", "BLOCKED")):
        return "BLOCKED"
    if any(token in text for token in ("VALID", "READY", "CURRENT", "PASS", "SELECTED", "PENDING", "PROMOTED")):
        return "COMPLETE"
    return "COMPLETE"


def _timer_on_calendar(timer_path: Path) -> list[str]:
    if not timer_path.exists():
        return []
    out: list[str] = []
    for line in timer_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("OnCalendar="):
            out.append(line.split("=", 1)[1].strip())
    return out


def _calendar_timezone(raw: str) -> ZoneInfo:
    if "America/New_York" in raw:
        return NY_TZ
    return UTC


def _calendar_time(raw: str) -> time | None:
    match = re.search(r"(\d{2}):(\d{2})(?::(\d{2}))?", raw)
    if not match:
        return None
    return time(int(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def _next_on_calendar(on_calendar: list[str], *, now_utc: datetime) -> str:
    candidates: list[datetime] = []
    for raw in on_calendar:
        parsed_time = _calendar_time(raw)
        if parsed_time is None:
            continue
        tz = _calendar_timezone(raw)
        local_today = now_utc.astimezone(tz).date()
        for offset in range(0, 8):
            day = local_today + timedelta(days=offset)
            # Simple daily timers are the only source used by this projection.
            # Weekday-gated timers remain visible as source text even when this
            # coarse next-run estimate skips their exact weekday grammar.
            local_dt = datetime.combine(day, parsed_time, tzinfo=tz)
            utc_dt = local_dt.astimezone(UTC).replace(microsecond=0)
            if utc_dt > now_utc:
                candidates.append(utc_dt)
    return _iso(min(candidates) if candidates else None)


def _latest_under(base: Path, pattern: str) -> Path | None:
    if not base.exists() or not base.is_dir():
        return None
    paths = sorted(base.glob(pattern), key=lambda path: (path.stat().st_mtime_ns, str(path)))
    return paths[-1].resolve() if paths else None


def _stage(
    *,
    stage_id: str,
    label: str,
    path: Path,
    artifact_id: str,
    status: str | None = None,
    blocker: str = "",
    waiting_reason: str = "",
    next_stage: str = "",
) -> dict[str, Any]:
    payload = _read_json(path)
    computed = status or _status_from_artifact(path, payload)
    return {
        "stage_id": stage_id,
        "label": label,
        "status": computed,
        "last_success_at_utc": _artifact_timestamp(path, payload) if computed == "COMPLETE" else "",
        "blocked_reason": blocker if computed == "BLOCKED" else "",
        "waiting_reason": waiting_reason if computed == "WAITING" else "",
        "next_stage": next_stage,
        "artifact": _artifact_ref(path, artifact_id),
    }


def _job(
    *,
    job_id: str,
    name: str,
    purpose: str,
    trigger_type: str,
    timer_path: Path | None,
    artifact_path: Path | None,
    artifact_id: str,
    now_utc: datetime,
    last_result: str = "",
    failure_reason: str = "",
    retry_count: int = 0,
    average_duration: str = "not recorded",
) -> dict[str, Any]:
    timer_lines = _timer_on_calendar(timer_path) if timer_path is not None else []
    payload = _read_json(artifact_path) if artifact_path is not None else {}
    status = _status_from_artifact(artifact_path, payload) if artifact_path is not None else "WAITING"
    if failure_reason:
        status = "BLOCKED"
    return {
        "job_id": job_id,
        "job_name": name,
        "purpose": purpose,
        "trigger_type": trigger_type,
        "schedule_source": str(timer_path or ""),
        "on_calendar": timer_lines,
        "last_run_at_utc": _artifact_timestamp(artifact_path, payload) if artifact_path is not None else "",
        "next_scheduled_run_utc": _next_on_calendar(timer_lines, now_utc=now_utc) if timer_lines else "",
        "current_status": status,
        "average_duration": average_duration,
        "last_result": last_result or str(payload.get("status") or payload.get("validation_status") or payload.get("final_eod_certification_status") or status),
        "retry_count": int(retry_count),
        "failure_reason": failure_reason,
        "source_artifact": _artifact_ref(artifact_path, artifact_id) if artifact_path is not None else {"artifact_id": artifact_id, "path": "", "exists": False, "sha256": ""},
    }


def _is_replay_mode(snapshot: dict[str, Any]) -> bool:
    mode = " ".join(
        str(value or "")
        for value in (
            snapshot.get("runtime_mode"),
            snapshot.get("authority_lane"),
            (snapshot.get("current_day_status") if isinstance(snapshot.get("current_day_status"), dict) else {}).get("runtime_mode"),
        )
    ).upper()
    return any(token in mode for token in ("REPLAY", "HISTORICAL_FALLBACK", "HISTORICAL_ONLY"))


def _current_phase(*, now_utc: datetime, session: dict[str, Any], final_pending: bool, intraday_ready: bool) -> str:
    if not session.get("equity_trading_session"):
        return "MARKET_CLOSED_NON_TRADING_DAY"
    close_dt = _parse_dt(session.get("official_close_local"))
    expected_after = _parse_dt(session.get("expected_current_data_after_utc"))
    open_dt = _parse_dt(str(session.get("market_open_local") or ""))
    if open_dt and now_utc < open_dt:
        return "PRE_MARKET"
    if close_dt and now_utc <= close_dt:
        return "INTRADAY_OPERATIONAL" if intraday_ready else "WAITING_FOR_INTRADAY_DATA"
    if expected_after and now_utc < expected_after:
        return "VENDOR_LAG_WINDOW"
    if final_pending:
        return "FINAL_EOD_CERTIFICATION_PENDING"
    return "FINAL_EOD_CERTIFICATION_COMPLETE"



def _load_domain_certification_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str) -> dict[str, Any]:
    path = domain_certification_report_path_v1(truth_root=truth_root, day_utc=day_utc)
    existing = _read_json(path)
    if isinstance(existing.get("domains"), list):
        return existing
    return build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)


def _load_schedule_event_ledger_v1(
    *,
    truth_root: Path,
    day_utc: str,
    operator_snapshot: dict[str, Any],
    now_utc: datetime,
    repo_root: Path,
) -> dict[str, Any]:
    path = schedule_event_ledger_path_v1(truth_root=truth_root, day_utc=day_utc)
    existing = _read_json(path)
    existing_events = existing.get("events") if isinstance(existing.get("events"), list) else []
    if existing_events and all(isinstance(row, dict) and row.get("event_category") and row.get("visibility_surface") for row in existing_events):
        return existing
    return build_schedule_event_ledger_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        operator_snapshot=operator_snapshot,
        now_utc=now_utc,
        repo_root=repo_root,
        write_artifact=False,
    )


def _dependency_next_retry(row: dict[str, Any], active_dependencies: list[dict[str, Any]]) -> str:
    event_key = str(row.get("event_key") or row.get("event_id") or "").lower()
    for dep in active_dependencies:
        if not isinstance(dep, dict):
            continue
        text = json.dumps(dep, sort_keys=True, default=str).lower()
        if event_key and event_key in text:
            return str(dep.get("next_retry_utc") or "")
        if ("certification" in event_key or "final_eod" in event_key) and any(token in text for token in ("final_eod", "vendor", "market_data")):
            return str(dep.get("next_retry_utc") or "")
        if "market_data" in event_key and any(token in text for token in ("market_data", "vendor")):
            return str(dep.get("next_retry_utc") or "")
    return ""


def _provider_feed_name(row: dict[str, Any]) -> str:
    expected = str(row.get("expected_artifact_type") or "").lower()
    event_key = str(row.get("event_key") or "").lower()
    if "final_eod" in expected or "certification" in event_key:
        return "final EOD market-data vendor"
    if "market_data" in expected or "market_data" in event_key:
        return "intraday market-data provider"
    return str(row.get("owning_job") or row.get("source_job_id") or "runtime provider")


def _derive_escalation_state(row: dict[str, Any], *, now_utc: datetime, active_dependencies: list[dict[str, Any]]) -> dict[str, Any]:
    scheduled = _parse_dt(row.get("scheduled_at_utc") or row.get("scheduled_at"))
    grace_minutes = int(row.get("grace_window_minutes") or 0)
    grace_end = scheduled + timedelta(minutes=grace_minutes) if scheduled else None
    age_minutes = max(0, int((now_utc - scheduled).total_seconds() // 60)) if scheduled and now_utc >= scheduled else 0
    minutes_past_grace = max(0, int((now_utc - grace_end).total_seconds() // 60)) if grace_end and now_utc > grace_end else 0
    operator_status = str(row.get("operator_status") or "")
    run_status = str(row.get("run_status") or "").upper()
    reason = str(row.get("status_reason") or "").upper()
    retry_count = int(row.get("retry_count") or 0)
    active_dependency = _event_has_active_dependency(row, active_dependencies)
    retry_exhausted = retry_count >= DEFAULT_RETRY_EXHAUSTION_THRESHOLD
    terminal_failure = operator_status == "Failed" or run_status == "FAILED" or ("FAILED" in reason and operator_status != "Completed")

    if operator_status in {"Completed", "Superseded"}:
        state = "NORMAL_WAIT"
        message = operator_status
        action = "No action required"
    elif terminal_failure:
        state = "FAILED"
        message = "Certification failed"
        action = "Repair required"
    elif operator_status == "Missed" and not active_dependency:
        state = "FAILED"
        message = "Scheduled runtime event missed"
        action = "Repair required"
    elif retry_exhausted or minutes_past_grace >= DEFAULT_ESCALATION_THRESHOLD_MINUTES:
        state = "ESCALATED"
        message = "Vendor dependency exceeded operational threshold"
        action = "Operator review recommended"
    elif minutes_past_grace > 0:
        state = "OVERDUE"
        message = "Vendor data overdue"
        action = "Diagnostics recommended"
    elif scheduled and now_utc >= scheduled and operator_status == "Running":
        state = "DELAYED"
        message = "Runtime step delayed"
        action = "Automatic retries continuing"
    elif operator_status == "Waiting on data" or active_dependency:
        state = "NORMAL_WAIT"
        message = "Waiting on vendor data"
        action = "No action required"
    else:
        state = "NORMAL_WAIT"
        message = "Waiting on vendor data" if operator_status in {"Scheduled", "Waiting"} else str(operator_status or "Scheduled")
        action = "No action required" if state == "NORMAL_WAIT" else "Monitor"

    expected_artifact_type = str(row.get("expected_artifact_type") or "")
    observed_count = len(row.get("observed_artifact_ids") or []) if isinstance(row.get("observed_artifact_ids"), list) else 0
    missing_artifacts = [expected_artifact_type] if expected_artifact_type and observed_count == 0 and operator_status not in {"Completed", "Superseded"} else []
    return {
        "escalation_state": state,
        "escalation_message": message,
        "operator_next_action": action,
        "dependency_age_minutes": age_minutes,
        "minutes_past_grace": minutes_past_grace,
        "provider_sla_minutes": DEFAULT_PROVIDER_SLA_MINUTES,
        "escalation_threshold_minutes": DEFAULT_ESCALATION_THRESHOLD_MINUTES,
        "retry_exhaustion_threshold": DEFAULT_RETRY_EXHAUSTION_THRESHOLD,
        "retry_exhausted": retry_exhausted,
        "next_retry_utc": _dependency_next_retry(row, active_dependencies),
        "provider_feed_name": _provider_feed_name(row),
        "missing_artifacts": missing_artifacts,
        "last_successful_artifact": "observed" if observed_count else "not reported",
        "diagnostics_recommended": state in {"OVERDUE", "ESCALATED", "FAILED"},
    }


def _event_category(row: dict[str, Any]) -> str:
    explicit = str(row.get("event_category") or "").strip().upper()
    if explicit in {OPERATOR_WORKFLOW_EVENT, INTERNAL_INFRASTRUCTURE_EVENT, DIAGNOSTIC_EVENT}:
        return explicit
    event_key = str(row.get("event_key") or row.get("event_id") or "").lower()
    source_job = str(row.get("source_job_id") or row.get("owning_job") or "").lower()
    if event_key in {"market_data_refresh", "candidate_scheduler_timer", "retry"} or "refresh" in event_key or "retry" in event_key:
        return INTERNAL_INFRASTRUCTURE_EVENT
    if source_job in {"market_data_refresh", "observability_aggregation", "suppression_diagnostics", "historical_snapshot_archival"}:
        return INTERNAL_INFRASTRUCTURE_EVENT
    return OPERATOR_WORKFLOW_EVENT


def _visibility_surface(row: dict[str, Any]) -> str:
    explicit = str(row.get("visibility_surface") or "").strip().upper()
    if explicit in {PRIMARY_OPERATOR_TIMELINE, DIAGNOSTICS_ONLY, HIDDEN}:
        return explicit
    category = _event_category(row)
    if category == OPERATOR_WORKFLOW_EVENT:
        return PRIMARY_OPERATOR_TIMELINE
    if category in {INTERNAL_INFRASTRUCTURE_EVENT, DIAGNOSTIC_EVENT}:
        return DIAGNOSTICS_ONLY
    return HIDDEN


def _operator_relevance_reason(row: dict[str, Any]) -> str:
    explicit = str(row.get("operator_relevance_reason") or "").strip()
    if explicit:
        return explicit
    surface = _visibility_surface(row)
    category = _event_category(row)
    if surface == PRIMARY_OPERATOR_TIMELINE:
        return "Governed operator workflow event; primary timeline eligible."
    if surface == DIAGNOSTICS_ONLY and category == INTERNAL_INFRASTRUCTURE_EVENT:
        return "Internal infrastructure job; visible only in diagnostics."
    if surface == DIAGNOSTICS_ONLY:
        return "Diagnostic event; visible only in diagnostics."
    return "Hidden from Runtime Timeline primary and diagnostics surfaces."


def _project_schedule_ledger_event(row: dict[str, Any], *, now_utc: datetime, replay_mode: bool) -> dict[str, Any]:
    scheduled = _parse_dt(row.get("scheduled_at_utc") or row.get("scheduled_at"))
    operator_status = str(row.get("operator_status") or "").strip()
    if scheduled and scheduled > now_utc:
        status = "UPCOMING"
    elif replay_mode and scheduled and scheduled <= now_utc and operator_status not in {"Completed", "Superseded"}:
        status = "HISTORICAL_REPLAY_PAST_EVENT"
    elif operator_status in {"Completed", "Superseded"}:
        status = "COMPLETED"
    elif operator_status in {"Failed", "Missed", "Blocked"}:
        status = "MISSED" if operator_status in {"Failed", "Missed"} else "BLOCKED"
    elif operator_status in {"Waiting on data", "Running"}:
        status = "PAST_WINDOW"
    else:
        status = "PAST_WINDOW" if scheduled and scheduled <= now_utc else "UNSCHEDULED"
    projected = dict(row)
    projected["ledger_event_id"] = str(row.get("event_id") or "")
    projected["event_id"] = str(row.get("event_key") or row.get("event_id") or "")
    projected["event_category"] = _event_category(projected)
    projected["visibility_surface"] = _visibility_surface(projected)
    projected["operator_relevance_reason"] = _operator_relevance_reason(projected)
    projected["operator_surface"] = "primary" if projected["visibility_surface"] == PRIMARY_OPERATOR_TIMELINE else "diagnostics"
    projected["status"] = status
    return projected


def _event_has_active_dependency(row: dict[str, Any], active_dependencies: list[dict[str, Any]]) -> bool:
    if not active_dependencies:
        return False
    text = json.dumps(active_dependencies, sort_keys=True, default=str).lower()
    event_key = str(row.get("event_key") or row.get("event_id") or "").lower()
    if event_key and event_key in text:
        return True
    if "certification" in event_key or "final_eod" in event_key:
        return any(token in text for token in ("certification", "final_eod", "vendor", "market_data"))
    if "market_data" in event_key:
        return any(token in text for token in ("market_data", "vendor"))
    return False


def _status_reason_requires_attention(row: dict[str, Any]) -> bool:
    reason = str(row.get("status_reason") or "").upper()
    if not reason:
        return False
    if str(row.get("operator_status") or "") in {"Completed", "Superseded"}:
        return False
    return any(token in reason for token in ("FAIL", "MISSED", "BLOCK", "DEPENDENCY", "WAIT", "MISSING", "REJECT", "TIMEOUT"))


def _is_needs_attention_event(row: dict[str, Any], *, now_utc: datetime, active_dependencies: list[dict[str, Any]]) -> bool:
    scheduled = _parse_dt(row.get("scheduled_at_utc") or row.get("scheduled_at"))
    operator_status = str(row.get("operator_status") or "").strip()
    run_status = str(row.get("run_status") or "").strip().upper()
    is_past = bool(scheduled and scheduled <= now_utc)
    if operator_status in {"Failed", "Missed", "Blocked"} or run_status in {"FAILED", "MISSED"}:
        return True
    if operator_status == "Waiting on data" and (is_past or _event_has_active_dependency(row, active_dependencies)):
        return True
    if operator_status == "Running" and is_past:
        return True
    if _event_has_active_dependency(row, active_dependencies) and is_past and operator_status not in {"Completed", "Superseded"}:
        return True
    return is_past and _status_reason_requires_attention(row)


def _hero_priority(row: dict[str, Any]) -> tuple[int, str]:
    escalation_state = str(row.get("escalation_state") or "")
    operator_status = str(row.get("operator_status") or "")
    run_status = str(row.get("run_status") or "").upper()
    reason = str(row.get("status_reason") or "").upper()
    if escalation_state == "FAILED" or operator_status == "Failed" or run_status == "FAILED":
        priority = 0
    elif operator_status == "Missed" or run_status == "MISSED":
        priority = 1
    elif escalation_state == "ESCALATED" or operator_status == "Blocked" or "BLOCK" in reason:
        priority = 2
    elif escalation_state == "OVERDUE" or operator_status == "Waiting on data" or "DEPENDENCY" in reason or "WAIT" in reason:
        priority = 3
    elif escalation_state == "DELAYED" or operator_status == "Running":
        priority = 4
    else:
        priority = 5
    return priority, str(row.get("scheduled_at_utc") or row.get("scheduled_at") or "")


def _group_schedule_events(
    projected_events: list[dict[str, Any]],
    *,
    day_utc: str,
    now_utc: datetime,
    active_dependencies: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    needs_attention: list[dict[str, Any]] = []
    upcoming: list[dict[str, Any]] = []
    completed_today: list[dict[str, Any]] = []
    for row in projected_events:
        scheduled = _parse_dt(row.get("scheduled_at_utc") or row.get("scheduled_at"))
        operator_status = str(row.get("operator_status") or "").strip()
        if _is_needs_attention_event(row, now_utc=now_utc, active_dependencies=active_dependencies):
            grouped = dict(row)
            grouped["event_group"] = "NEEDS_ATTENTION"
            needs_attention.append(grouped)
            continue
        if scheduled and scheduled > now_utc and operator_status in {"Scheduled", "Waiting", ""}:
            grouped = dict(row)
            grouped["event_group"] = "UPCOMING"
            upcoming.append(grouped)
            continue
        if operator_status in {"Completed", "Superseded"} and str(row.get("operational_day") or day_utc) == day_utc:
            grouped = dict(row)
            grouped["event_group"] = "COMPLETED_TODAY"
            completed_today.append(grouped)
    return {
        "needs_attention": sorted(needs_attention, key=_hero_priority),
        "upcoming": sorted(upcoming, key=lambda row: str(row.get("scheduled_at_utc") or row.get("scheduled_at") or "")),
        "completed_today": sorted(completed_today, key=lambda row: str(row.get("scheduled_at_utc") or row.get("scheduled_at") or ""), reverse=True),
    }


def _select_runtime_hero(groups: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if groups.get("needs_attention"):
        hero = dict(groups["needs_attention"][0])
        hero["hero_kind"] = "NEEDS_ATTENTION"
        return hero
    if groups.get("upcoming"):
        hero = dict(groups["upcoming"][0])
        hero["hero_kind"] = "UPCOMING"
        return hero
    return {
        "event_name": "Runtime schedule clear",
        "event_key": "all_clear",
        "event_id": "all_clear",
        "operator_status": "Completed",
        "status": "COMPLETED",
        "hero_kind": "ALL_CLEAR",
        "purpose": "No unresolved or upcoming runtime events remain for the visible schedule.",
        "expected_result": "No operator action is required unless a new run is scheduled.",
    }


def build_runtime_timeline_projection_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    operator_snapshot: dict[str, Any] | None = None,
    now_utc: str | datetime | None = None,
    repo_root: Path | str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve() if repo_root is not None else REPO_ROOT
    now = now_utc if isinstance(now_utc, datetime) else _parse_dt(now_utc)
    now = (now or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    snapshot = operator_snapshot if isinstance(operator_snapshot, dict) else {}
    today = snapshot.get("operator_today_projection") if isinstance(snapshot.get("operator_today_projection"), dict) else {}
    current_status = snapshot.get("current_day_status") if isinstance(snapshot.get("current_day_status"), dict) else {}
    session = resolve_market_session_v1(truth_root=root, day_utc=day_utc, vendor_lag_minutes=vendor_lag_minutes_from_env_v1())
    day = datetime.strptime(day_utc, "%Y-%m-%d").date()
    market_open_local = datetime.combine(day, time(9, 30), tzinfo=NY_TZ)
    session["market_open_local"] = market_open_local.replace(microsecond=0).isoformat()
    session["market_open_utc"] = _iso(market_open_local)
    close_dt = _parse_dt(session.get("official_close_local"))
    session["market_close_utc"] = _iso(close_dt)
    final_pending = bool(today.get("final_eod_certification_pending") is True or current_status.get("final_eod_certification_pending") is True)
    intraday_ready = bool(today.get("intraday_operational_ready") is True or current_status.get("intraday_operational_ready") is True)
    certification_start = _parse_dt(session.get("expected_current_data_after_utc"))
    certification_targets = [
        datetime.combine(day, time(16, 30), tzinfo=NY_TZ).astimezone(UTC),
        datetime.combine(day, time(17, 0), tzinfo=NY_TZ).astimezone(UTC),
        datetime.combine(day, time(18, 0), tzinfo=NY_TZ).astimezone(UTC),
        datetime.combine(day + timedelta(days=1), time(8, 15), tzinfo=NY_TZ).astimezone(UTC),
    ]
    paths = {
        "market": root / "reports" / "market_data_intraday_operational_v1" / day_utc / "market_data_intraday_operational.v1.json",
        "inputs": root / "reports" / "market_data_inputs_v1" / day_utc / "market_data_inputs.v1.json",
        "readiness": root / "reports" / "market_data_readiness_v1" / day_utc / "market_data_readiness.v1.json",
        "manifest": _latest_under(root / "reports" / "candidate_generation_manifest_v1" / day_utc, "*/candidate_generation_manifest.v1.json") or root / "reports" / "candidate_generation_manifest_v1" / day_utc / "candidate_generation_manifest.v1.json",
        "scoring": root / "reports" / "portfolio_scoring_v1" / day_utc / "portfolio_scoring.v1.json",
        "arbitration": root / "reports" / "intent_arbitration_v1" / day_utc / "intent_arbitration.v1.json",
        "intent_plane": root / "reports" / "candidate_intent_plane_v1" / day_utc / "candidate_intent_plane.v1.json",
        "promotion": root / "reports" / "aegis_selected_intent_promotion_v1" / day_utc / "selected_intent_promotion.v1.json",
        "promotion_map": root / "reports" / "candidate_promotion_map_v1" / day_utc / "candidate_promotion_map.v1.json",
        "construction": root / "reports" / "paper_trade_construction_v1" / day_utc / "paper_trade_construction.v1.json",
        "operator_snapshot": root / "reports" / "operator_state_snapshot_v1" / day_utc / "operator_state_snapshot.v1.json",
        "replay": root / "reports" / "replay_certification_gate_v1" / day_utc / "replay_certification_gate.v1.json",
    }
    blocked_selected = int(today.get("blocked_selected_candidate_count") or current_status.get("blocked_selected_candidate_count") or 0)
    capture_ready = int(today.get("capture_ready_ticket_count") or current_status.get("capture_ready_ticket_count") or 0)
    final_status = str(today.get("final_eod_certification_status") or current_status.get("final_eod_certification_status") or "PENDING").upper()
    stages = [
        _stage(stage_id="market_data", label="Market Data", path=paths["market"], artifact_id="market_data_intraday_operational_v1", status="COMPLETE" if intraday_ready else None, next_stage="candidate_generation"),
        _stage(stage_id="candidate_generation", label="Candidate Generation", path=paths["manifest"], artifact_id="candidate_generation_manifest_v1", next_stage="qualification"),
        _stage(stage_id="qualification", label="Qualification", path=paths["readiness"], artifact_id="market_data_readiness_v1", next_stage="scoring"),
        _stage(stage_id="scoring", label="Scoring", path=paths["scoring"], artifact_id="portfolio_scoring_v1", next_stage="selection"),
        _stage(stage_id="selection", label="Selection", path=paths["arbitration"], artifact_id="intent_arbitration_v1", next_stage="provisional_intent_generation"),
        _stage(stage_id="provisional_intent_generation", label="Provisional Intent Generation", path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", next_stage="confidence_update"),
        _stage(stage_id="confidence_update", label="Confidence Update", path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", next_stage="stability_check"),
        _stage(stage_id="stability_check", label="Stability Check", path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", next_stage="preliminary_recommendation_window"),
        _stage(stage_id="preliminary_recommendation_window", label="Preliminary Recommendation Window", path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", status="COMPLETE" if paths["intent_plane"].exists() else "WAITING", waiting_reason="Intent confidence/stability snapshots are accumulating.", next_stage="certification"),
        _stage(stage_id="certification", label="EOD Certification", path=paths["market"], artifact_id="market_data_intraday_operational_v1", status="WAITING" if final_pending else ("COMPLETE" if final_status in {"VALID", "CERTIFIED", "PASS"} else "BLOCKED"), waiting_reason="Final EOD certification is pending.", next_stage="reconciliation"),
        _stage(stage_id="reconciliation", label="Reconciliation", path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", status="WAITING" if final_pending else ("COMPLETE" if paths["intent_plane"].exists() else "WAITING"), waiting_reason="Final certification must converge before reconciliation.", next_stage="final_recommendation_window"),
        _stage(stage_id="final_recommendation_window", label="Final Recommendation Window", path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", status="WAITING" if final_pending else ("COMPLETE" if paths["intent_plane"].exists() else "WAITING"), waiting_reason="Waiting for final certification convergence.", next_stage="promotion"),
        _stage(stage_id="promotion", label="Promotion", path=paths["promotion"], artifact_id="aegis_selected_intent_promotion_v1", status="COMPLETE" if paths["promotion"].exists() else "WAITING", next_stage="manual_capture_ready"),
        _stage(
            stage_id="manual_capture_ready",
            label="Manual IB Capture Ready",
            path=paths["promotion_map"],
            artifact_id="candidate_promotion_map_v1",
            status="COMPLETE" if capture_ready else ("BLOCKED" if blocked_selected else "WAITING"),
            blocker="Selected candidate is blocked before manual capture." if blocked_selected else "",
            waiting_reason="No selected candidate is ready for manual capture yet.",
        ),
    ]
    blocked_stage = next((stage for stage in stages if stage["status"] == "BLOCKED"), None)
    waiting_stage = next((stage for stage in stages if stage["status"] == "WAITING"), None)
    current_stage = blocked_stage or waiting_stage or stages[-1]
    retry_job = current_status.get("retry_job") if isinstance(current_status.get("retry_job"), dict) else {}
    scheduler_failure = current_status.get("scheduler_failure") if isinstance(current_status.get("scheduler_failure"), dict) else {}
    jobs = [
        _job(job_id="market_data_refresh", name="Market-data refresh", purpose="Fetch current-session intraday snapshots.", trigger_type="cron", timer_path=repo / "ops/systemd/user/aegis-market-data-refresh-v1.timer", artifact_path=paths["market"], artifact_id="market_data_intraday_operational_v1", now_utc=now),
        _job(job_id="candidate_generation", name="Candidate generation", purpose="Run sleeve evaluation and write candidate manifest.", trigger_type="cron", timer_path=repo / "ops/systemd/user/aegis-lite-eod-report-v1.timer", artifact_path=paths["manifest"], artifact_id="candidate_generation_manifest_v1", now_utc=now),
        _job(job_id="portfolio_scoring", name="Portfolio scoring", purpose="Score eligible candidates for arbitration.", trigger_type="event-driven", timer_path=None, artifact_path=paths["scoring"], artifact_id="portfolio_scoring_v1", now_utc=now),
        _job(job_id="intent_confidence_update", name="Intent confidence update", purpose="Update deterministic confidence, stability, convergence, and capture guidance.", trigger_type="event-driven", timer_path=None, artifact_path=paths["intent_plane"], artifact_id="candidate_intent_plane_v1", now_utc=now),
        _job(job_id="certification_promotion", name="Certification / promotion", purpose="Separate final EOD certification from intraday promotion.", trigger_type="cron", timer_path=repo / "ops/systemd/user/aegis-market-data-final-eod-v1.timer", artifact_path=paths["promotion"], artifact_id="aegis_selected_intent_promotion_v1", now_utc=now),
        _job(job_id="capture_ticket_generation", name="Capture-ticket generation", purpose="Build governed manual-capture ticket projections.", trigger_type="event-driven", timer_path=None, artifact_path=paths["construction"], artifact_id="paper_trade_construction_v1", now_utc=now),
        _job(job_id="observability_aggregation", name="Observability aggregation", purpose="Build the operator snapshot and runtime timeline.", trigger_type="event-driven", timer_path=None, artifact_path=paths["operator_snapshot"], artifact_id="operator_state_snapshot_v1", now_utc=now),
        _job(job_id="suppression_diagnostics", name="Suppression diagnostics", purpose="Explain selected, suppressed, blocked, and capture-ready candidate states.", trigger_type="event-driven", timer_path=None, artifact_path=paths["promotion_map"], artifact_id="candidate_promotion_map_v1", now_utc=now),
        _job(job_id="historical_snapshot_archival", name="Historical snapshot archival", purpose="Keep completed captures and replay evidence separate from current workflow.", trigger_type="event-driven", timer_path=None, artifact_path=paths["replay"], artifact_id="replay_certification_gate_v1", now_utc=now),
    ]
    retry_count = int(retry_job.get("retry_count") or retry_job.get("attempt_count") or 0)
    active_retries = []
    if retry_job or current_status.get("retry_action_available") is True:
        active_retries.append(
            {
                "job_id": str(retry_job.get("job_id") or "market_data_refresh"),
                "status": str(retry_job.get("status") or ("QUEUED" if current_status.get("retry_action_available") else "IDLE")),
                "retry_count": retry_count,
                "next_retry_utc": str(retry_job.get("next_retry_utc") or current_status.get("next_retry_utc") or ""),
                "backoff_state": str(retry_job.get("backoff_state") or ("exponential backoff not active" if retry_count == 0 else "exponential backoff active")),
                "failure_reason": str(retry_job.get("failure_reason") or current_status.get("blocker") or ""),
            }
        )
    domain_certification = _load_domain_certification_v1(truth_root=root, day_utc=day_utc, generated_at_utc=_iso(now))
    schedule_ledger = _load_schedule_event_ledger_v1(
        truth_root=root,
        day_utc=day_utc,
        operator_snapshot=snapshot,
        now_utc=now,
        repo_root=repo,
    )
    ledger_events = list(schedule_ledger.get("events") or [])
    active_dependencies = list(schedule_ledger.get("active_dependencies") or [])
    replay_mode = bool(schedule_ledger.get("replay_mode"))
    projected_events = []
    for row in ledger_events:
        if not isinstance(row, dict):
            continue
        projected = _project_schedule_ledger_event(row, now_utc=now, replay_mode=replay_mode)
        projected.update(_derive_escalation_state(projected, now_utc=now, active_dependencies=active_dependencies))
        projected_events.append(projected)
    primary_timeline_events = [row for row in projected_events if _visibility_surface(row) == PRIMARY_OPERATOR_TIMELINE]
    diagnostics_only_events = [row for row in projected_events if _visibility_surface(row) == DIAGNOSTICS_ONLY]
    hidden_events = [row for row in projected_events if _visibility_surface(row) == HIDDEN]
    operator_projected_events = [row for row in primary_timeline_events if _event_category(row) == OPERATOR_WORKFLOW_EVENT]
    infrastructure_projected_events = [row for row in diagnostics_only_events if _event_category(row) == INTERNAL_INFRASTRUCTURE_EVENT]
    diagnostic_projected_events = [row for row in diagnostics_only_events if _event_category(row) == DIAGNOSTIC_EVENT]
    event_groups = _group_schedule_events(primary_timeline_events, day_utc=day_utc, now_utc=now, active_dependencies=active_dependencies)
    schedule_projection = {
        "event_groups": event_groups,
        "primary_timeline_events": primary_timeline_events,
        "operator_workflow_events": operator_projected_events,
        "diagnostics_only_events": diagnostics_only_events,
        "infrastructure_events": infrastructure_projected_events,
        "diagnostic_events": diagnostic_projected_events,
        "hidden_events": hidden_events,
        "needs_attention_events": event_groups["needs_attention"],
        "upcoming_events": event_groups["upcoming"],
        "completed_today_events": event_groups["completed_today"],
        "next_scheduled_events": event_groups["upcoming"],
        "completed_past_events": event_groups["completed_today"],
        "missed_events": [row for row in event_groups["needs_attention"] if str(row.get("operator_status") or "") in {"Missed", "Failed", "Blocked", "Waiting on data"}],
        "active_dependencies": active_dependencies,
        "next_event": _select_runtime_hero(event_groups),
        "timeline_semantics": {
            "event_grouping": "operator_meaning",
            "primary_event_category": OPERATOR_WORKFLOW_EVENT,
            "primary_visibility_surface": PRIMARY_OPERATOR_TIMELINE,
            "diagnostics_visibility_surface": DIAGNOSTICS_ONLY,
            "hidden_visibility_surface": HIDDEN,
            "diagnostics_event_categories": [INTERNAL_INFRASTRUCTURE_EVENT, DIAGNOSTIC_EVENT],
            "needs_attention_definition": "unresolved_past_overdue_blocked_failed_missed_or_active_dependency_operator_workflow_events_only",
            "upcoming_definition": "future_operator_workflow_events_only",
            "completed_today_default_visibility": "visible",
            "diagnostics_default_visibility": "collapsed",
            "next_scheduled_events_order": "schedule_event_ledger_chronological_datetime_ascending_future_only",
            "pipeline_stages_order": "logical_workflow_order",
            "past_events_default_visibility": "collapsed",
            "status_clock": "historical_replay_clock" if replay_mode else "wall_clock_utc",
            "status_authority": "schedule_event_ledger_v1",
            "grouping_authority": "schedule_event_ledger_v1.operator_workflow_event_groups",
        },
    }
    alerts: list[dict[str, Any]] = []
    for event in event_groups["needs_attention"]:
        state = str(event.get("escalation_state") or "")
        if state in {"ESCALATED", "FAILED"}:
            alerts.append({
                "severity": "HIGH" if state == "FAILED" else "MEDIUM",
                "alert_type": "runtime_dependency_escalated" if state == "ESCALATED" else "runtime_event_failed",
                "message": str(event.get("escalation_message") or event.get("status_reason") or "Runtime event requires attention."),
                "event_id": str(event.get("ledger_event_id") or event.get("event_id") or ""),
                "event_name": str(event.get("event_name") or "Runtime event"),
                "operator_next_action": str(event.get("operator_next_action") or "Review diagnostics."),
            })
    for domain in domain_certification.get("domains", []) if isinstance(domain_certification.get("domains"), list) else []:
        if not isinstance(domain, dict):
            continue
        status = str(domain.get("certification_status") or "")
        blockers = domain.get("blocking_sleeves") if isinstance(domain.get("blocking_sleeves"), list) else []
        if status in {"DELAYED", "CONFLICTED", "FAILED"} and blockers:
            alerts.append({
                "severity": "HIGH" if status in {"CONFLICTED", "FAILED"} else "MEDIUM",
                "alert_type": "domain_certification_blocker",
                "message": f"{domain.get('domain_id')} is {status.lower()} and blocks {len(blockers)} sleeve(s).",
                "domain_id": str(domain.get("domain_id") or ""),
                "blocking_sleeves": blockers,
                "operator_next_action": "Resolve required domain data or isolate affected sleeves.",
            })
    if scheduler_failure and str(scheduler_failure.get("status") or "").upper() not in {"", "OK", "NONE"}:
        alerts.append({"severity": "HIGH", "alert_type": "missed_scheduled_job", "message": str(scheduler_failure.get("message") or scheduler_failure.get("reason") or "Scheduler failure detected.")})
    if blocked_stage:
        alerts.append({"severity": "MEDIUM", "alert_type": "pipeline_stage_blocked", "message": str(blocked_stage.get("blocked_reason") or f"{blocked_stage['label']} is blocked.")})
    if final_pending and certification_start and now > certification_start + timedelta(hours=2):
        alerts.append({"severity": "MEDIUM", "alert_type": "certification_timeout", "message": "Final EOD certification is still pending after the certification window."})
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": _iso(now),
        "runtime_clock_semantics": {
            "ui_render_timestamp": "",
            "operator_snapshot_timestamp": str(snapshot.get("generated_at_utc") or snapshot.get("generated_at") or ""),
            "market_data_timestamp": str(today.get("market_data_last_updated_at") or current_status.get("market_data_last_updated_at") or _artifact_timestamp(paths["market"], _read_json(paths["market"]))),
            "candidate_snapshot_timestamp": str(today.get("candidate_snapshot_generated_at") or current_status.get("candidate_snapshot_generated_at") or _artifact_timestamp(paths["manifest"], _read_json(paths["manifest"]))),
            "certification_timestamp": str(today.get("final_eod_certification_completed_at") or current_status.get("final_eod_certification_completed_at") or ""),
        },
        "market_session_timeline": {
            "market_open_utc": session.get("market_open_utc") if session.get("equity_trading_session") else "",
            "market_close_utc": session.get("market_close_utc") if session.get("equity_trading_session") else "",
            "early_close": bool(session.get("equity_early_close") or session.get("sifma_early_close")),
            "vendor_lag_window": f"{session.get('vendor_lag_minutes')} minutes",
            "vendor_lag_ends_utc": str(session.get("expected_current_data_after_utc") or ""),
            "certification_window_start_utc": _iso(certification_start),
            "certification_window_end_utc": _iso(certification_targets[2] if certification_targets else None),
            "final_eod_certification_target_utc": _iso(certification_targets[0] if certification_targets else None),
            "current_operational_phase": _current_phase(now_utc=now, session=session, final_pending=final_pending, intraday_ready=intraday_ready),
            "calendar": session,
        },
        "scheduled_jobs": jobs,
        "schedule_event_ledger": {
            "artifact_path": str(schedule_event_ledger_path_v1(truth_root=root, day_utc=day_utc)),
            "content_hash": str(schedule_ledger.get("content_hash") or ""),
            "event_count": len(ledger_events),
        },
        "domain_certification": domain_certification,
        "domain_certification_grid": list(domain_certification.get("domains") or []),
        "domain_repair_actions": list(domain_certification.get("domain_repair_actions") or []),
        "sleeve_domain_statuses": list(domain_certification.get("sleeve_domain_statuses") or []),
        "schedule_event_ledger_events": ledger_events,
        "primary_timeline_events": schedule_projection["primary_timeline_events"],
        "operator_workflow_events": schedule_projection["operator_workflow_events"],
        "diagnostics_only_events": schedule_projection["diagnostics_only_events"],
        "infrastructure_events": schedule_projection["infrastructure_events"],
        "diagnostic_events": schedule_projection["diagnostic_events"],
        "hidden_events": schedule_projection["hidden_events"],
        "today_events": [row for row in schedule_projection["operator_workflow_events"] if (_parse_dt(row.get("scheduled_at_utc")) or now).astimezone(NY_TZ).date() == now.astimezone(NY_TZ).date()],
        "tomorrow_events": [row for row in schedule_projection["operator_workflow_events"] if (_parse_dt(row.get("scheduled_at_utc")) or now).astimezone(NY_TZ).date() == now.astimezone(NY_TZ).date() + timedelta(days=1)],
        "event_groups": schedule_projection["event_groups"],
        "needs_attention_events": schedule_projection["needs_attention_events"],
        "upcoming_events": schedule_projection["upcoming_events"],
        "completed_today_events": schedule_projection["completed_today_events"],
        "next_scheduled_events": schedule_projection["next_scheduled_events"],
        "completed_past_events": schedule_projection["completed_past_events"],
        "missed_events": schedule_projection["missed_events"],
        "active_dependencies": schedule_projection["active_dependencies"],
        "next_event": schedule_projection["next_event"],
        "timeline_semantics": schedule_projection["timeline_semantics"],
        "current_pipeline_state": {
            "stages": stages,
            "current_stage": current_stage["stage_id"],
            "blocked_stage": str(blocked_stage.get("stage_id") if blocked_stage else ""),
            "waiting_stage": str(waiting_stage.get("stage_id") if waiting_stage else ""),
            "last_successful_stage": next((stage["stage_id"] for stage in reversed(stages) if stage["status"] == "COMPLETE"), ""),
            "next_scheduled_stage": str(waiting_stage.get("stage_id") if waiting_stage else ""),
        },
        "certification_timing": {
            "status": "certified complete" if not final_pending and final_status in {"VALID", "CERTIFIED", "PASS"} else ("waiting on vendor data" if final_pending else "certification not started"),
            "final_eod_certification_status": final_status or "PENDING",
            "certification_not_started": not paths["market"].exists(),
            "waiting_on_vendor_data": final_pending,
            "validating": False,
            "promotion_pending": final_pending and paths["promotion"].exists(),
            "certified_complete": not final_pending and final_status in {"VALID", "CERTIFIED", "PASS"},
        },
        "retry_visibility": {
            "active_retries": active_retries,
            "retry_queue_depth": len(active_retries),
            "queued_remediation_jobs": [row for row in active_retries if str(row.get("status") or "").upper() in {"QUEUED", "RETRYING"}],
        },
        "alerts": alerts,
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
