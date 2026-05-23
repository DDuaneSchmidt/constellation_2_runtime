from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ops.aegis.market_data.freshness_policy_v1 import resolve_market_session_v1, vendor_lag_minutes_from_env_v1

SCHEMA_ID = "aegis_schedule_event_ledger"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "schedule_event_ledger_v1"
NY_TZ = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parents[3]

RUN_STATUSES = {"PLANNED", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED", "SUPERSEDED", "MISSED", "UNKNOWN"}
OPERATOR_WORKFLOW_EVENT = "OPERATOR_WORKFLOW_EVENT"
INTERNAL_INFRASTRUCTURE_EVENT = "INTERNAL_INFRASTRUCTURE_EVENT"
DIAGNOSTIC_EVENT = "DIAGNOSTIC_EVENT"
PRIMARY_OPERATOR_TIMELINE = "PRIMARY_OPERATOR_TIMELINE"
DIAGNOSTICS_ONLY = "DIAGNOSTICS_ONLY"
HIDDEN = "HIDDEN"



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


def _stable_hash(payload: dict[str, Any]) -> str:
    clean = {k: v for k, v in payload.items() if k not in {"content_hash", "output_hash"}}
    blob = json.dumps(clean, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


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


def _artifact_timestamp(path: Path, payload: dict[str, Any]) -> str:
    for key in (
        "generated_at_utc",
        "produced_at_utc",
        "created_at_utc",
        "updated_at_utc",
        "certified_at_utc",
        "final_eod_certified_at_utc",
        "completed_at_utc",
    ):
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    if path.exists() and path.is_file():
        return _iso(datetime.fromtimestamp(path.stat().st_mtime, tz=UTC))
    return ""


def _artifact_ref(path: Path, artifact_type: str) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    payload = _read_json(path)
    return {
        "artifact_type": artifact_type,
        "artifact_id": str(payload.get("artifact_id") or payload.get("schema_id") or artifact_type),
        "path": str(path),
        "exists": exists,
        "sha256": _sha256_file(path) if exists else "",
        "observed_at_utc": _artifact_timestamp(path, payload),
        "status_text": " ".join(str(payload.get(key) or "") for key in ("status", "validation_status", "final_eod_certification_status", "certification_state")).strip(),
    }


def _artifact_failed(ref: dict[str, Any]) -> bool:
    text = str(ref.get("status_text") or "").upper()
    return any(token in text for token in ("FAIL", "FAILED", "INVALID", "REJECTED", "BLOCKED"))


def _artifact_success(ref: dict[str, Any]) -> bool:
    if not ref.get("exists"):
        return False
    if _artifact_failed(ref):
        return False
    text = str(ref.get("status_text") or "").upper()
    if not text:
        return True
    return any(token in text for token in ("VALID", "READY", "CURRENT", "PASS", "SELECTED", "PROMOTED", "CERTIFIED", "FINAL"))


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


def _calendar_events_for_days(timer_path: Path, *, days: list[Any]) -> list[datetime]:
    events: list[datetime] = []
    for raw in _timer_on_calendar(timer_path):
        parsed = _calendar_time(raw)
        if parsed is None:
            continue
        tz = _calendar_timezone(raw)
        for day in days:
            events.append(datetime.combine(day, parsed, tzinfo=tz).astimezone(UTC).replace(microsecond=0))
    return sorted(set(events))


def _manifest_scalar(value: str) -> str:
    text = value.strip()
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    return text


def _runtime_manifest_operator_events(repo_root: Path) -> list[dict[str, str]]:
    path = repo_root / "ops/runtime/runtime_manifest.yaml"
    if not path.exists():
        return []
    events: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    in_section = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if raw_line.startswith("operator_workflow_events:"):
            in_section = True
            continue
        if in_section and raw_line and not raw_line.startswith(" "):
            break
        if not in_section:
            continue
        stripped = raw_line.strip()
        if stripped.startswith("- "):
            if current:
                events.append(current)
            current = {}
            remainder = stripped[2:].strip()
            if ":" in remainder:
                key, value = remainder.split(":", 1)
                current[key.strip()] = _manifest_scalar(value)
            continue
        if current is not None and ":" in stripped:
            key, value = stripped.split(":", 1)
            current[key.strip()] = _manifest_scalar(value)
    if current:
        events.append(current)
    return events


def _workflow_event_time(day: Any, row: dict[str, str]) -> datetime | None:
    local_time = _calendar_time(str(row.get("local_time") or ""))
    if local_time is None:
        return None
    try:
        tz = ZoneInfo(str(row.get("timezone") or "America/New_York"))
    except Exception:
        tz = NY_TZ
    try:
        offset = int(row.get("day_offset") or 0)
    except ValueError:
        offset = 0
    return datetime.combine(day + timedelta(days=offset), local_time, tzinfo=tz).astimezone(UTC).replace(microsecond=0)


def _visibility_contract_for_category(event_category: str) -> tuple[str, str]:
    category = str(event_category or "").strip().upper()
    if category == OPERATOR_WORKFLOW_EVENT:
        return PRIMARY_OPERATOR_TIMELINE, "Governed operator workflow event; primary timeline eligible."
    if category == INTERNAL_INFRASTRUCTURE_EVENT:
        return DIAGNOSTICS_ONLY, "Internal infrastructure job; visible only in diagnostics."
    if category == DIAGNOSTIC_EVENT:
        return DIAGNOSTICS_ONLY, "Diagnostic event; visible only in diagnostics unless explicitly hidden."
    return HIDDEN, "Unknown event category; hidden from operator timeline."


def schedule_event_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "schedule_event_ledger.v1.json"


def _is_replay_mode(snapshot: dict[str, Any]) -> bool:
    text = json.dumps(
        {
            "runtime_mode": snapshot.get("runtime_mode"),
            "authority_lane": snapshot.get("authority_lane"),
            "current_day_status": snapshot.get("current_day_status") if isinstance(snapshot.get("current_day_status"), dict) else {},
        },
        sort_keys=True,
        default=str,
    ).upper()
    return any(token in text for token in ("REPLAY", "HISTORICAL_FALLBACK", "HISTORICAL_ONLY"))


def _active_dependencies(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    current = snapshot.get("current_day_status") if isinstance(snapshot.get("current_day_status"), dict) else {}
    retry = current.get("retry_job") if isinstance(current.get("retry_job"), dict) else {}
    deps: list[dict[str, Any]] = []
    if retry or current.get("retry_action_available") is True:
        deps.append(
            {
                "dependency_id": str(retry.get("job_id") or "market_data_refresh"),
                "status": str(retry.get("status") or "QUEUED"),
                "reason": str(retry.get("failure_reason") or current.get("blocker") or "PENDING_VENDOR_DATA"),
                "next_retry_utc": str(retry.get("next_retry_utc") or current.get("next_retry_utc") or ""),
            }
        )
    if current.get("final_eod_certification_pending") is True:
        deps.append(
            {
                "dependency_id": "final_eod_vendor_data",
                "status": "WAITING_ON_DATA",
                "reason": "FINAL_EOD_CERTIFICATION_PENDING",
                "next_retry_utc": str(current.get("next_retry_utc") or retry.get("next_retry_utc") or ""),
            }
        )
    for row in current.get("active_jobs") or []:
        if isinstance(row, dict):
            deps.append({"dependency_id": str(row.get("job_id") or row.get("id") or "active_job"), "status": str(row.get("status") or "RUNNING"), "reason": str(row.get("reason") or "ACTIVE_JOB"), "next_retry_utc": str(row.get("next_retry_utc") or "")})
    return deps


def _has_active_dependency_for_event(event_key: str, dependencies: list[dict[str, Any]]) -> bool:
    if not dependencies:
        return False
    key = event_key.lower()
    dep_text = json.dumps(dependencies, sort_keys=True).lower()
    if key in dep_text:
        return True
    if "certification" in key or "final_eod" in key:
        return "final_eod" in dep_text or "vendor" in dep_text or "market_data" in dep_text
    if "market_data" in key:
        return "market_data" in dep_text or "vendor" in dep_text
    return False


def _planned_event(
    *,
    event_key: str,
    event_name: str,
    event_type: str,
    operational_day: str,
    scheduled_at: datetime,
    planned_order: int,
    trigger_type: str,
    owning_job: str,
    expected_artifact_type: str,
    expected_artifact_path: Path,
    grace_window_minutes: int,
    purpose: str,
    expected_result: str,
    event_category: str = OPERATOR_WORKFLOW_EVENT,
    diagnostics_visibility: str = "primary",
) -> dict[str, Any]:
    scheduled_iso = _iso(scheduled_at)
    event_id_basis = f"{operational_day}:{event_key}:{scheduled_iso}"
    event_id = f"schedule_event:{hashlib.sha256(event_id_basis.encode('utf-8')).hexdigest()[:20]}"
    visibility_surface, operator_relevance_reason = _visibility_contract_for_category(event_category)
    return {
        "event_id": event_id,
        "event_key": event_key,
        "event_name": event_name,
        "event_type": event_type,
        "event_category": event_category,
        "visibility_surface": visibility_surface,
        "operator_relevance_reason": operator_relevance_reason,
        "diagnostics_visibility": diagnostics_visibility,
        "operator_surface": "primary" if visibility_surface == PRIMARY_OPERATOR_TIMELINE else "diagnostics",
        "operational_day": operational_day,
        "scheduled_at": scheduled_iso,
        "scheduled_at_utc": scheduled_iso,
        "timezone": "America/New_York",
        "planned_order": planned_order,
        "trigger_type": trigger_type,
        "owning_job": owning_job,
        "source_job_id": owning_job,
        "expected_artifact_type": expected_artifact_type,
        "expected_artifact_id_pattern": str(expected_artifact_path),
        "expected_artifact_path": str(expected_artifact_path),
        "grace_window_minutes": int(grace_window_minutes),
        "purpose": purpose,
        "expected_result": expected_result,
    }


def _planned_events(*, truth_root: Path, repo_root: Path, day_utc: str, session: dict[str, Any]) -> list[dict[str, Any]]:
    day = datetime.strptime(day_utc, "%Y-%m-%d").date()
    next_day = day + timedelta(days=1)
    paths = {
        "market": truth_root / "reports" / "market_data_intraday_operational_v1" / day_utc / "market_data_intraday_operational.v1.json",
        "candidate": truth_root / "reports" / "candidate_generation_manifest_v1" / day_utc / "run-1" / "candidate_generation_manifest.v1.json",
        "final_eod": truth_root / "reports" / "market_data_final_eod_v1" / day_utc / "market_data_final_eod.v1.json",
        "intent": truth_root / "reports" / "candidate_intent_plane_v1" / day_utc / "candidate_intent_plane.v1.json",
        "promotion": truth_root / "reports" / "candidate_promotion_map_v1" / day_utc / "candidate_promotion_map.v1.json",
        "research_pipeline": truth_root / "reports" / "research_pipeline_v1" / day_utc / "research_pipeline.v1.json",
    }
    candidate_base = truth_root / "reports" / "candidate_generation_manifest_v1" / day_utc
    if candidate_base.exists():
        manifests = sorted(candidate_base.glob("*/candidate_generation_manifest.v1.json"), key=lambda p: (p.stat().st_mtime_ns, str(p)))
        if manifests:
            paths["candidate"] = manifests[-1]
    planned: list[dict[str, Any]] = []
    order = 1
    for dt in _calendar_events_for_days(repo_root / "ops/systemd/user/aegis-market-data-refresh-v1.timer", days=[day, next_day]):
        planned.append(_planned_event(event_key="market_data_refresh", event_name="Market-data refresh", event_type="MARKET_DATA", operational_day=day_utc, scheduled_at=dt, planned_order=order, trigger_type="cron", owning_job="market_data_refresh", expected_artifact_type="market_data_intraday_operational_v1", expected_artifact_path=paths["market"], grace_window_minutes=20, purpose="Refresh current-session market data.", expected_result="Intraday market data becomes current or reports a precise provider blocker.", event_category=INTERNAL_INFRASTRUCTURE_EVENT, diagnostics_visibility="diagnostics"))
        order += 1
    for dt in _calendar_events_for_days(repo_root / "ops/systemd/user/aegis-lite-eod-report-v1.timer", days=[day, next_day]):
        planned.append(_planned_event(event_key="candidate_scheduler_timer", event_name="Candidate scheduler timer", event_type="CANDIDATE_GENERATION", operational_day=day_utc, scheduled_at=dt, planned_order=order, trigger_type="cron", owning_job="candidate_generation", expected_artifact_type="candidate_generation_manifest_v1", expected_artifact_path=paths["candidate"], grace_window_minutes=20, purpose="Internal candidate-generation scheduler timer.", expected_result="Candidate scheduler job records its diagnostic state.", event_category=INTERNAL_INFRASTRUCTURE_EVENT, diagnostics_visibility="diagnostics"))
        order += 1
    manifest_rows = _runtime_manifest_operator_events(repo_root)
    if not manifest_rows:
        manifest_rows = [
            {"event_key": "morning_ai_run", "event_name": "Morning AI Run", "event_type": "CANDIDATE_GENERATION", "local_time": "09:50", "day_offset": "0", "trigger_type": "governed-runtime-manifest", "owning_job": "candidate_generation", "expected_artifact_type": "candidate_generation_manifest_v1", "expected_artifact_path_key": "candidate", "grace_window_minutes": "20", "purpose": "Run the governed morning sleeve and AI workflow.", "expected_result": "Morning candidates, scoring inputs, and operator projection are refreshed."},
            {"event_key": "afternoon_ai_run", "event_name": "Afternoon AI Run", "event_type": "CANDIDATE_GENERATION", "local_time": "14:50", "day_offset": "0", "trigger_type": "governed-runtime-manifest", "owning_job": "candidate_generation", "expected_artifact_type": "candidate_generation_manifest_v1", "expected_artifact_path_key": "candidate", "grace_window_minutes": "20", "purpose": "Run the governed afternoon sleeve and AI workflow.", "expected_result": "Afternoon candidates, scoring inputs, and operator projection are refreshed."},
            {"event_key": "final_eod_certification", "event_name": "Final Certification", "event_type": "CERTIFICATION", "local_time": "18:00", "day_offset": "0", "trigger_type": "governed-runtime-manifest", "owning_job": "certification_promotion", "expected_artifact_type": "market_data_final_eod_v1", "expected_artifact_path_key": "final_eod", "grace_window_minutes": "30", "purpose": "Certify final daily market data and reconcile provisional candidates.", "expected_result": "Final manual IB capture recommendations may become available."},
            {"event_key": "overnight_research", "event_name": "Overnight Research", "event_type": "RESEARCH", "local_time": "05:50", "day_offset": "1", "trigger_type": "governed-runtime-manifest", "owning_job": "overnight_research", "expected_artifact_type": "research_pipeline_snapshot_v1", "expected_artifact_path_key": "research_pipeline", "grace_window_minutes": "60", "purpose": "Run queued overnight hypothesis research.", "expected_result": "Overnight research findings are ready for operator review."},
            {"event_key": "reconciliation", "event_name": "Reconciliation", "event_type": "RECONCILIATION", "local_time": "08:15", "day_offset": "1", "trigger_type": "governed-runtime-manifest", "owning_job": "certification_promotion", "expected_artifact_type": "candidate_intent_plane_v1", "expected_artifact_path_key": "intent", "grace_window_minutes": "60", "purpose": "Confirm final certification, intent convergence, and promotion evidence agree.", "expected_result": "Preliminary and final intent states are reconciled for the next operator session."},
        ]
    for row in manifest_rows:
        scheduled = _workflow_event_time(day, row)
        if scheduled is None:
            continue
        path_key = str(row.get("expected_artifact_path_key") or "")
        expected_path = paths.get(path_key, paths.get(str(row.get("event_key") or ""), paths["candidate"]))
        planned.append(_planned_event(
            event_key=str(row.get("event_key") or "operator_workflow_event"),
            event_name=str(row.get("event_name") or row.get("event_key") or "Operator workflow event"),
            event_type=str(row.get("event_type") or "OPERATOR_WORKFLOW"),
            operational_day=day_utc,
            scheduled_at=scheduled,
            planned_order=order,
            trigger_type=str(row.get("trigger_type") or "governed-runtime-manifest"),
            owning_job=str(row.get("owning_job") or row.get("source_job_id") or "operator_workflow"),
            expected_artifact_type=str(row.get("expected_artifact_type") or "operator_workflow_artifact"),
            expected_artifact_path=expected_path,
            grace_window_minutes=int(row.get("grace_window_minutes") or 30),
            purpose=str(row.get("purpose") or "Advance the governed operator workflow."),
            expected_result=str(row.get("expected_result") or "The operator workflow advances."),
            event_category=str(row.get("event_category") or OPERATOR_WORKFLOW_EVENT),
            diagnostics_visibility="primary",
        ))
        order += 1
    return sorted(planned, key=lambda row: (str(row.get("scheduled_at_utc")), int(row.get("planned_order") or 0)))


def _reconcile_event(
    event: dict[str, Any], *, now_utc: datetime, dependencies: list[dict[str, Any]], replay_mode: bool, superseded: bool) -> dict[str, Any]:
    scheduled = _parse_dt(event.get("scheduled_at_utc"))
    grace = timedelta(minutes=int(event.get("grace_window_minutes") or 0))
    expected_type = str(event.get("expected_artifact_type") or "artifact")
    ref = _artifact_ref(Path(str(event.get("expected_artifact_path") or "")), expected_type)
    observed = [ref] if ref.get("exists") else []
    active_dependency = _has_active_dependency_for_event(str(event.get("event_key") or ""), dependencies)
    status_text = str(ref.get("status_text") or "").upper()
    artifact_success = _artifact_success(ref) or (ref.get("exists") and "PENDING" in status_text and expected_type != "market_data_final_eod_v1" and not _artifact_failed(ref))
    run_status = "UNKNOWN"
    operator_status = "Not scheduled today"
    reason = "NO_SCHEDULED_TIME"
    actual_completed = ""
    if scheduled and scheduled > now_utc:
        run_status = "PLANNED"
        operator_status = "Scheduled"
        reason = "FUTURE_EVENT"
    elif superseded:
        run_status = "SUPERSEDED"
        operator_status = "Superseded"
        reason = "LATER_SUCCESSFUL_EVENT_REPLACED_THIS_SCHEDULED_EVENT"
    elif ref.get("exists") and _artifact_failed(ref):
        run_status = "FAILED"
        operator_status = "Failed"
        reason = "OBSERVED_ARTIFACT_FAILED"
        actual_completed = str(ref.get("observed_at_utc") or "")
    elif artifact_success:
        run_status = "SUCCEEDED"
        operator_status = "Completed"
        reason = "EXPECTED_ARTIFACT_OBSERVED"
        actual_completed = str(ref.get("observed_at_utc") or "")
    elif scheduled and now_utc <= scheduled + grace:
        if active_dependency:
            run_status = "QUEUED"
            operator_status = "Waiting on data"
            reason = "ACTIVE_DEPENDENCY_RECORDED_WITHIN_GRACE_WINDOW"
        else:
            run_status = "QUEUED"
            operator_status = "Running"
            reason = "INSIDE_GRACE_WINDOW_NO_ARTIFACT_YET"
    elif active_dependency:
        run_status = "QUEUED"
        operator_status = "Waiting on data"
        reason = "ACTIVE_DEPENDENCY_RECORDED_AFTER_GRACE_WINDOW"
    elif replay_mode and scheduled and scheduled <= now_utc:
        run_status = "UNKNOWN"
        operator_status = "Historical status unavailable"
        reason = "HISTORICAL_REPLAY_NO_OBSERVED_ARTIFACT"
    elif scheduled and now_utc > scheduled + grace:
        run_status = "MISSED"
        operator_status = "Missed"
        reason = "NO_RUN_OR_EXPECTED_ARTIFACT_AFTER_GRACE_WINDOW"
    event = {
        **event,
        "actual_started_at": actual_completed,
        "actual_completed_at": actual_completed,
        "observed_artifact_ids": observed,
        "run_status": run_status if run_status in RUN_STATUSES else "UNKNOWN",
        "operator_status": operator_status,
        "status_reason": reason,
        "retry_count": len(dependencies),
        "superseded_by_event_id": "",
        "lineage_metadata": {
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "status_source": "schedule_event_ledger_v1",
            "planned_schedule_source": event.get("trigger_type"),
            "expected_artifact_path": event.get("expected_artifact_path"),
        },
    }
    event["content_hash"] = _stable_hash(event)
    return event


def _apply_supersession(planned: list[dict[str, Any]], now_utc: datetime) -> set[str]:
    superseded: set[str] = set()
    by_path: dict[str, list[dict[str, Any]]] = {}
    for row in planned:
        path_key = str(row.get("expected_artifact_path") or "")
        category_key = str(row.get("event_category") or "")
        by_path.setdefault(f"{category_key}:{path_key}", []).append(row)
    for path_group_key, rows in by_path.items():
        path_text = path_group_key.split(":", 1)[1] if ":" in path_group_key else path_group_key
        if not path_text:
            continue
        expected_type = str(rows[0].get("expected_artifact_type") or "artifact")
        ref = _artifact_ref(Path(path_text), expected_type)
        observed_at = _parse_dt(ref.get("observed_at_utc"))
        status_text = str(ref.get("status_text") or "").upper()
        artifact_success = _artifact_success(ref) or (ref.get("exists") and "PENDING" in status_text and expected_type != "market_data_final_eod_v1" and not _artifact_failed(ref))
        if not (artifact_success and observed_at):
            continue
        past_rows = [row for row in rows if (_parse_dt(row.get("scheduled_at_utc")) or now_utc) <= observed_at]
        if len(past_rows) <= 1:
            continue
        latest = max(past_rows, key=lambda row: str(row.get("scheduled_at_utc") or ""))
        for row in past_rows:
            if row is not latest:
                superseded.add(str(row.get("event_id") or ""))
    return superseded


def _event_day_bucket(event: dict[str, Any], *, now_utc: datetime) -> str:
    scheduled = _parse_dt(event.get("scheduled_at_utc"))
    if scheduled is None:
        return "unscheduled"
    date_key = scheduled.astimezone(NY_TZ).date()
    today_key = now_utc.astimezone(NY_TZ).date()
    if date_key == today_key:
        return "today"
    if date_key == today_key + timedelta(days=1):
        return "tomorrow"
    if date_key < today_key:
        return "past"
    return "future"


def build_schedule_event_ledger_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    operator_snapshot: dict[str, Any] | None = None,
    now_utc: str | datetime | None = None,
    repo_root: Path | str | None = None,
    write_artifact: bool = True,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve() if repo_root is not None else REPO_ROOT
    now = now_utc if isinstance(now_utc, datetime) else _parse_dt(now_utc)
    now = (now or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    snapshot = operator_snapshot if isinstance(operator_snapshot, dict) else {}
    replay_mode = _is_replay_mode(snapshot)
    session = resolve_market_session_v1(truth_root=root, day_utc=day_utc, vendor_lag_minutes=vendor_lag_minutes_from_env_v1())
    planned = _planned_events(truth_root=root, repo_root=repo, day_utc=day_utc, session=session)
    deps = _active_dependencies(snapshot)
    superseded_ids = _apply_supersession(planned, now)
    events = [_reconcile_event(row, now_utc=now, dependencies=deps, replay_mode=replay_mode, superseded=str(row.get("event_id") or "") in superseded_ids) for row in planned]
    events = sorted(events, key=lambda row: (str(row.get("scheduled_at_utc") or ""), int(row.get("planned_order") or 0)))
    unresolved = [row for row in events if row.get("operator_status") in {"Missed", "Failed", "Waiting on data", "Running"}]
    future = [row for row in events if row.get("operator_status") == "Scheduled"]
    next_candidates = sorted(unresolved, key=lambda row: str(row.get("scheduled_at_utc") or "")) or sorted(future, key=lambda row: str(row.get("scheduled_at_utc") or ""))
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": f"{REPORT_FAMILY}:{day_utc}",
        "operational_day": day_utc,
        "day_utc": day_utc,
        "generated_at_utc": _iso(now),
        "timezone": "America/New_York",
        "replay_mode": replay_mode,
        "events": events,
        "today_events": [row for row in events if _event_day_bucket(row, now_utc=now) == "today"],
        "tomorrow_events": [row for row in events if _event_day_bucket(row, now_utc=now) == "tomorrow"],
        "completed_past_events": [row for row in events if row.get("operator_status") in {"Completed", "Superseded"} and (_parse_dt(row.get("scheduled_at_utc")) or now) <= now],
        "missed_events": [row for row in events if row.get("operator_status") in {"Missed", "Failed"}],
        "active_dependencies": deps,
        "next_event": next_candidates[0] if next_candidates else {},
        "status_rules": {
            "past_without_artifact_after_grace": "Missed",
            "past_with_expected_artifact": "Completed",
            "past_with_active_dependency": "Waiting on data",
            "future": "Scheduled",
            "later_success_replaces_prior_attempt": "Superseded",
            "hard_rule": "No event older than scheduled_at + grace_window may be Scheduled or Waiting unless an active dependency exists.",
        },
    }
    payload["content_hash"] = _stable_hash(payload)
    payload["output_hash"] = payload["content_hash"]
    if write_artifact:
        path = schedule_event_ledger_path_v1(truth_root=root, day_utc=day_utc)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        payload["artifact_path"] = str(path)
    return payload
