from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1

REPORT_FAMILY = "aegis_candidate_state_v1"
ACTIVE_STATES = {"AWAITING_REVIEW", "APPROVED_FOR_PAPER", "PAPER_POSITION_OPEN"}
TERMINAL_STATES = {"PAPER_POSITION_CLOSED", "REJECTED_BY_OPERATOR", "EXPIRED", "INVALIDATED_BY_RUNTIME"}
DEFAULT_MAX_AGE_HOURS = 72


def candidate_state_global_path_v1(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / "candidate_state.v1.json"


def candidate_state_snapshot_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / "candidate_state.v1.json"


def read_candidate_state_v1(*, truth_root: Path) -> dict[str, Any]:
    path = candidate_state_global_path_v1(truth_root=truth_root)
    return _read_json(path) if path.exists() else {}


def write_candidate_state_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).resolve()
    global_path = write_json_v1(candidate_state_global_path_v1(truth_root=root), payload)
    snapshot_path = write_json_v1(candidate_state_snapshot_path_v1(truth_root=root, day_utc=day_utc), payload)
    return {"json": str(global_path), "snapshot": str(snapshot_path)}


def roll_candidate_state_v1(*, truth_root: Path, day_utc: str, max_age_hours: int = DEFAULT_MAX_AGE_HOURS) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    prior = read_candidate_state_v1(truth_root=root)
    by_id: dict[str, dict[str, Any]] = {}
    for row in _safe_list(prior.get("candidates")):
        if isinstance(row, dict) and str(row.get("candidate_id") or ""):
            by_id[str(row.get("candidate_id"))] = dict(row)

    source_paths: list[str] = []
    for source_day in _candidate_source_days(root, day_utc):
        packet = _read_report(root, "aegis_candidate_review_packet_v1", source_day, "candidate_review_packet.v1.json")
        queue = _read_report(root, "aegis_paper_review_queue_v1", source_day, "paper_review_queue.v1.json")
        outcomes = _read_report(root, "aegis_paper_trade_outcomes_v1", source_day, "paper_trade_outcomes.v1.json")
        for family, filename in [
            ("aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json"),
            ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json"),
            ("aegis_paper_trade_outcomes_v1", "paper_trade_outcomes.v1.json"),
        ]:
            path = root / "reports" / family / source_day / filename
            if path.exists():
                source_paths.append(str(path))
        review_by_id = {str(row.get("candidate_id") or ""): row for row in _safe_list(packet.get("review_candidates")) if isinstance(row, dict)}
        open_by_id = {str(row.get("candidate_id") or row.get("linked_candidate_id") or ""): row for row in _safe_list(outcomes.get("open_trades")) if isinstance(row, dict)}
        closed_by_id = {str(row.get("candidate_id") or row.get("linked_candidate_id") or ""): row for row in _safe_list(outcomes.get("closed_trades")) if isinstance(row, dict)}
        for queue_row in _safe_list(queue.get("rows")):
            if not isinstance(queue_row, dict):
                continue
            candidate_id = str(queue_row.get("candidate_id") or "")
            if not candidate_id:
                continue
            review_row = review_by_id.get(candidate_id, {})
            current = _state_from_rows(queue_row, open_by_id.get(candidate_id, {}), closed_by_id.get(candidate_id, {}))
            existing = by_id.get(candidate_id, {})
            originating_day = str(existing.get("originating_day") or source_day)
            expires_at = str(queue_row.get("expires_at_utc") or existing.get("expires_at") or _expiry_for_day(originating_day, max_age_hours))
            if current in {"AWAITING_REVIEW", "APPROVED_FOR_PAPER"} and _is_expired(expires_at):
                current = "EXPIRED"
            invalidation_reason = str(existing.get("invalidation_reason") or "")
            if current == "INVALIDATED_BY_RUNTIME" and not invalidation_reason:
                invalidation_reason = "RUNTIME_INVALIDATION"
            by_id[candidate_id] = {
                **existing,
                "candidate_id": candidate_id,
                "symbol": str(queue_row.get("symbol") or review_row.get("symbol") or existing.get("symbol") or ""),
                "originating_day": originating_day,
                "current_state": current,
                "expires_at": expires_at,
                "review_status": _review_status(current),
                "paper_position_status": _paper_position_status(current),
                "invalidation_reason": invalidation_reason,
                "latest_projection_day": day_utc,
                "rollover_status": "CURRENT_DAY" if originating_day == day_utc else "CARRIED_FORWARD",
                "age_days": _age_days(originating_day, day_utc),
                "source_queue_day": source_day,
                "source_queue_status": str(queue_row.get("status") or ""),
                "operator_decision_required": True,
                "operator_review_required": True,
                "paper_trade_eligible": bool(queue_row.get("paper_trade_eligible", True)),
                "live_trade_eligible": False,
                "paper_only": True,
                "sleeve_id": str(review_row.get("sleeve_id") or existing.get("sleeve_id") or ""),
                "raw_signal_id": str(review_row.get("raw_signal_id") or existing.get("raw_signal_id") or ""),
                "direction": str(review_row.get("direction") or existing.get("direction") or ""),
                "entry_reference_price": str(review_row.get("entry_reference_price") or existing.get("entry_reference_price") or ""),
                "thesis_reason_codes": _safe_list(review_row.get("thesis_reason_codes")) or _safe_list(existing.get("thesis_reason_codes")),
                "evidence_paths": _safe_list(review_row.get("evidence_paths")) or _safe_list(existing.get("evidence_paths")),
                "warnings": _safe_list(review_row.get("disqualifying_warnings")) or _safe_list(existing.get("warnings")),
                "open_paper_position": open_by_id.get(candidate_id, existing.get("open_paper_position") if isinstance(existing.get("open_paper_position"), dict) else {}),
                "closed_paper_position": closed_by_id.get(candidate_id, existing.get("closed_paper_position") if isinstance(existing.get("closed_paper_position"), dict) else {}),
                "updated_at_utc": now_utc_v1(),
            }

    candidates = sorted(by_id.values(), key=lambda row: (str(row.get("originating_day") or ""), str(row.get("symbol") or ""), str(row.get("candidate_id") or "")))
    active = [row for row in candidates if str(row.get("current_state") or "") in ACTIVE_STATES]
    payload = {
        "schema_id": "aegis_candidate_state",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_state_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "max_age_hours": max_age_hours,
        "candidate_count": len(candidates),
        "active_candidate_count": len(active),
        "candidates": candidates,
        "active_candidates": active,
        "status_counts": _count_statuses(candidates),
        "source_artifacts": sorted(set(source_paths)),
        "safety": {
            "paper_only": True,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "live_trade_eligible": False,
        },
    }
    write_candidate_state_v1(truth_root=root, day_utc=day_utc, payload=payload)
    return payload


def active_candidate_state_rows_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    state = roll_candidate_state_v1(truth_root=truth_root, day_utc=day_utc)
    return [row for row in _safe_list(state.get("active_candidates")) if isinstance(row, dict)]


def queue_rows_from_candidate_state_v1(rows: list[dict[str, Any]], *, day_utc: str) -> list[dict[str, Any]]:
    projected = []
    for row in rows:
        state = str(row.get("current_state") or "AWAITING_REVIEW")
        queue_status = "APPROVED_FOR_PAPER" if state == "APPROVED_FOR_PAPER" else state
        projected.append({
            "candidate_id": str(row.get("candidate_id") or ""),
            "symbol": str(row.get("symbol") or ""),
            "status": queue_status,
            "operator_decision_required": True,
            "decision_reason": str(row.get("invalidation_reason") or ""),
            "timestamp": str(row.get("updated_at_utc") or ""),
            "paper_trade_eligible": bool(row.get("paper_trade_eligible", True)),
            "live_trade_eligible": False,
            "operator_review_required": True,
            "expires_at_utc": str(row.get("expires_at") or ""),
            "originating_day": str(row.get("originating_day") or day_utc),
            "latest_projection_day": day_utc,
            "rollover_status": str(row.get("rollover_status") or "CURRENT_DAY"),
            "age_days": row.get("age_days", 0),
            "expiration_status": "EXPIRED" if state == "EXPIRED" else "ACTIVE",
            "current_state": state,
            "sleeve_id": str(row.get("sleeve_id") or ""),
            "raw_signal_id": str(row.get("raw_signal_id") or ""),
            "direction": str(row.get("direction") or ""),
            "entry_reference_price": str(row.get("entry_reference_price") or ""),
            "thesis_reason_codes": _safe_list(row.get("thesis_reason_codes")),
            "evidence_paths": _safe_list(row.get("evidence_paths")),
            "warnings": _safe_list(row.get("warnings")),
            "open_paper_position": row.get("open_paper_position") if isinstance(row.get("open_paper_position"), dict) else {},
            "closed_paper_position": row.get("closed_paper_position") if isinstance(row.get("closed_paper_position"), dict) else {},
        })
    return projected


def _candidate_source_days(root: Path, day_utc: str) -> list[str]:
    days = {day_utc}
    for family in ["aegis_paper_review_queue_v1", "aegis_candidate_review_packet_v1", "aegis_paper_trade_outcomes_v1"]:
        base = root / "reports" / family
        if base.exists():
            for child in base.iterdir():
                if child.is_dir() and child.name <= day_utc:
                    days.add(child.name)
    return sorted(days)


def _read_report(root: Path, family: str, day: str, filename: str) -> dict[str, Any]:
    return _read_json(root / "reports" / family / day / filename)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _state_from_rows(queue_row: dict[str, Any], open_row: dict[str, Any], closed_row: dict[str, Any]) -> str:
    status = str(queue_row.get("status") or "AWAITING_REVIEW").upper()
    if closed_row:
        return "PAPER_POSITION_CLOSED"
    if open_row:
        return "PAPER_POSITION_OPEN"
    if status in {"REJECTED_BY_OPERATOR", "EXPIRED", "INVALIDATED_BY_RUNTIME", "APPROVED_FOR_PAPER", "AWAITING_REVIEW"}:
        return status
    return "AWAITING_REVIEW"


def _review_status(state: str) -> str:
    if state == "REJECTED_BY_OPERATOR":
        return "REJECTED"
    if state in {"APPROVED_FOR_PAPER", "PAPER_POSITION_OPEN", "PAPER_POSITION_CLOSED"}:
        return "APPROVED"
    if state in {"EXPIRED", "INVALIDATED_BY_RUNTIME"}:
        return state
    return "AWAITING_REVIEW"


def _paper_position_status(state: str) -> str:
    if state == "PAPER_POSITION_OPEN":
        return "OPEN"
    if state == "PAPER_POSITION_CLOSED":
        return "CLOSED"
    return "NONE"


def _expiry_for_day(day: str, hours: int) -> str:
    base = _parse_day(day) or datetime.now(UTC)
    return (base + timedelta(hours=hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_expired(value: str) -> bool:
    ts = _parse_ts(value)
    return bool(ts and datetime.now(UTC) > ts)


def _parse_day(day: str) -> datetime | None:
    try:
        return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)
    except Exception:
        return None


def _parse_ts(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return _parse_day(raw[:10])


def _age_days(originating_day: str, day_utc: str) -> int:
    start = _parse_day(originating_day)
    end = _parse_day(day_utc)
    if not start or not end:
        return 0
    return max(0, (end.date() - start.date()).days)


def _count_statuses(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        state = str(row.get("current_state") or "UNKNOWN")
        counts[state] = counts.get(state, 0) + 1
    return counts
