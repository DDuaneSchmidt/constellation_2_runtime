from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.market_data.freshness_policy_v1 import (
    FRESHNESS_STATES,
    PARTIAL_DATA_AVAILABLE,
    PENDING_VENDOR_DATA,
    READ_ONLY_PRIOR_DAY_FALLBACK,
    VALIDATED_CURRENT_DAY,
    CERTIFICATION_PENDING,
    CERTIFIED,
    INVALID,
    PROVISIONAL_INTRADAY,
    STALE,
    classify_market_data_freshness_v1,
)
from ops.aegis.candidate_intent_plane_v1 import (
    build_candidate_intent_plane_v1,
    candidate_intent_plane_path_v1,
)

SCHEMA_ID = "current_operator_truth"
SCHEMA_VERSION = "v1"
ALT_RUNTIME_TRUTH_ROOT = Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth")
CURRENT_OK = "CURRENT"
LEGACY_CURRENT = "CURRENT"
ACTIVE_INTRADAY_CANDIDATE_STATUSES = {"CANDIDATE_CREATED", "INTENT_CREATED", "CAPTURE_READY", "ACTIVE_CURRENT"}
MARKET_DATA_VALIDATED = "MARKET_DATA_VALIDATED"
MARKET_DATA_PENDING = "MARKET_DATA_PENDING"
MARKET_DATA_PARTIAL = "MARKET_DATA_PARTIAL"
MARKET_DATA_INVALID = "MARKET_DATA_INVALID"
CANDIDATES_PROVISIONAL = "CANDIDATES_PROVISIONAL"
CANDIDATES_CERTIFICATION_PENDING = "CANDIDATES_CERTIFICATION_PENDING"
CANDIDATES_CERTIFIED = "CANDIDATES_CERTIFIED"
CANDIDATES_CERTIFICATION_FAILED = "CANDIDATES_CERTIFICATION_FAILED"
EXECUTION_LOCKED_NON_CERTIFIED = "EXECUTION_LOCKED_NON_CERTIFIED"
EXECUTION_ELIGIBLE_CERTIFIED_ONLY = "EXECUTION_ELIGIBLE_CERTIFIED_ONLY"
ANALYTICAL_QUALIFIED = "QUALIFIED"
ANALYTICAL_REVIEWABLE = "REVIEWABLE"
ANALYTICAL_BLOCKED = "BLOCKED"
ANALYTICAL_SUPPRESSED = "SUPPRESSED"
NO_USER_ACTION = "NO_USER_ACTION"
MONITOR_ONLY = "MONITOR_ONLY"
MANUAL_IB_CAPTURE_READY = "MANUAL_IB_CAPTURE_READY"
SYSTEM_REPAIR_REQUIRED = "SYSTEM_REPAIR_REQUIRED"
NON_EXECUTABLE = "NON_EXECUTABLE"
EXECUTION_LOCKED_NON_CERTIFIED_ROW = "EXECUTION_LOCKED_NON_CERTIFIED"
EXECUTION_ELIGIBLE_ROW = "EXECUTION_ELIGIBLE"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path, errors: list[dict[str, Any]], artifact_id: str) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        errors.append({"code": f"{artifact_id.upper()}_MISSING", "message": f"Artifact missing: {path}", "source_path": str(path), "recoverable": True})
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        errors.append({"code": f"{artifact_id.upper()}_MALFORMED", "message": f"Artifact JSON is malformed: {exc}", "source_path": str(path), "recoverable": True})
        return {}
    except Exception as exc:  # noqa: BLE001
        errors.append({"code": f"{artifact_id.upper()}_READ_FAILED", "message": f"Artifact read failed: {type(exc).__name__}: {exc}", "source_path": str(path), "recoverable": True})
        return {}
    if not isinstance(payload, dict):
        errors.append({"code": f"{artifact_id.upper()}_TOP_LEVEL_NOT_OBJECT", "message": "Artifact top level is not an object.", "source_path": str(path), "recoverable": True})
        return {}
    return payload


def _source_ref(path: Path | None, artifact_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    exists = bool(path and path.exists() and path.is_file())
    return {
        "artifact_id": artifact_id,
        "path": str(path or ""),
        "exists": exists,
        "sha256": _sha256_file(path) if exists and path is not None else "",
        "schema_id": str((payload or {}).get("schema_id") or ""),
    }




def _runtime_mode_from_status(status: str) -> str:
    status = str(status or "").upper()
    if status in {PROVISIONAL_INTRADAY, "INTRADAY_OPERATIONAL_READY"}:
        return "OPEN_INTRADAY"
    if status in {VALIDATED_CURRENT_DAY, CERTIFICATION_PENDING}:
        return "CERTIFICATION_PENDING"
    if status in {CERTIFIED, "FINAL_EOD_READY"}:
        return "CERTIFIED_READY"
    if status in {PENDING_VENDOR_DATA, PARTIAL_DATA_AVAILABLE, READ_ONLY_PRIOR_DAY_FALLBACK, STALE, "HISTORICAL_ONLY", "STALE_FALLBACK"}:
        return "DEGRADED_READ_ONLY"
    if status in {INVALID, "CURRENT_DAY_FAILED"}:
        return "CERTIFICATION_FAILED"
    if status in {"CURRENT_DAY_BLOCKED"}:
        return status
    return status or "UNKNOWN"



def _market_data_state_from_status(*values: Any) -> str:
    states = {str(value or "").strip().upper() for value in values if str(value or "").strip()}
    if states & {INVALID, "CURRENT_DAY_FAILED", "MARKET_DATA_FETCH_FAILED", "INTRADAY_OPERATIONAL_FAILED", "FINAL_EOD_BLOCKED", STALE, "CURRENT_DAY_DATA_STALE"}:
        return MARKET_DATA_INVALID
    if states & {PARTIAL_DATA_AVAILABLE, "PARTIAL", "PARTIAL_PROVIDER_SUCCESS"}:
        return MARKET_DATA_PARTIAL
    if states & {VALIDATED_CURRENT_DAY, PROVISIONAL_INTRADAY, CERTIFICATION_PENDING, CERTIFIED, "INTRADAY_OPERATIONAL_READY", "FINAL_EOD_READY", "CURRENT", "READY", "VALID"}:
        return MARKET_DATA_VALIDATED
    if states & {PENDING_VENDOR_DATA, READ_ONLY_PRIOR_DAY_FALLBACK, "MARKET_NOT_FINALIZED_YET", "HISTORICAL_ONLY", "STALE_FALLBACK", "UNKNOWN"}:
        return MARKET_DATA_PENDING
    return MARKET_DATA_PENDING


def _candidate_certification_state_from_payload(payload: dict[str, Any]) -> str:
    rows = payload.get("candidate_rows") if isinstance(payload.get("candidate_rows"), list) else []
    row_states = {str(row.get("certification_state") or "").strip().upper() for row in rows if isinstance(row, dict)}
    row_lanes = {str(row.get("candidate_lane") or "").strip().upper() for row in rows if isinstance(row, dict)}
    certification_state = str(payload.get("certification_state") or "").strip().upper()
    candidate_lane = str(payload.get("candidate_lane") or "").strip().upper()
    final_eod_status = str(payload.get("final_eod_certification_status") or "").strip().upper()
    candidate_count = int(payload.get("candidate_count") or len(rows) or 0)
    execution_eligible_count = len([row for row in rows if isinstance(row, dict) and row.get("execution_eligible") is True])
    if {certification_state, final_eod_status, *row_states} & {INVALID, "FAILED", "FAIL", "CERTIFICATION_FAILED"}:
        return CANDIDATES_CERTIFICATION_FAILED
    if candidate_count and execution_eligible_count == candidate_count and (
        certification_state == CERTIFIED or final_eod_status in {"CERTIFIED", "VALID", "PASS"} or row_states == {CERTIFIED}
    ):
        return CANDIDATES_CERTIFIED
    if certification_state == CERTIFICATION_PENDING or final_eod_status == "PENDING" or payload.get("final_eod_certification_pending") is True or CERTIFICATION_PENDING in row_states:
        return CANDIDATES_CERTIFICATION_PENDING
    if candidate_lane == "PROVISIONAL" or "PROVISIONAL" in row_lanes or candidate_count:
        return CANDIDATES_PROVISIONAL
    return CANDIDATES_PROVISIONAL


def _with_operator_state_semantics(payload: dict[str, Any]) -> dict[str, Any]:
    market_data_state = _market_data_state_from_status(
        payload.get("market_data_state"),
        payload.get("freshness_state"),
        payload.get("validation_status"),
        payload.get("operator_market_data_state"),
        payload.get("status"),
    )
    candidate_certification_state = _candidate_certification_state_from_payload(payload)
    execution_eligibility_state = EXECUTION_ELIGIBLE_CERTIFIED_ONLY if candidate_certification_state == CANDIDATES_CERTIFIED else EXECUTION_LOCKED_NON_CERTIFIED
    payload["market_data_state"] = market_data_state
    payload["candidate_certification_state"] = candidate_certification_state
    payload["execution_eligibility_state"] = execution_eligibility_state
    payload["operator_state_semantics"] = {
        "market_data_state": market_data_state,
        "candidate_certification_state": candidate_certification_state,
        "execution_eligibility_state": execution_eligibility_state,
    }
    return payload


def _candidate_analytical_state(status: str) -> str:
    status = str(status or "").upper()
    if status == "BLOCKED":
        return ANALYTICAL_BLOCKED
    if status in {"SUPPRESSED", "SIGNAL_ONLY", "NO_SIGNAL"}:
        return ANALYTICAL_SUPPRESSED
    if status in ACTIVE_INTRADAY_CANDIDATE_STATUSES:
        return ANALYTICAL_QUALIFIED
    return ANALYTICAL_REVIEWABLE


def _candidate_execution_state(*, execution_eligible: bool, candidate_lane: str, certification_state: str, analytical_state: str) -> str:
    if execution_eligible:
        return EXECUTION_ELIGIBLE_ROW
    if analytical_state in {ANALYTICAL_BLOCKED, ANALYTICAL_SUPPRESSED}:
        return NON_EXECUTABLE
    if str(candidate_lane or "").upper() != "CERTIFIED" or str(certification_state or "").upper() != CERTIFIED:
        return EXECUTION_LOCKED_NON_CERTIFIED_ROW
    return NON_EXECUTABLE


def _candidate_requires_system_repair(*, status: str, reason_codes: list[str], lifecycle_reason_codes: list[str], blocker: str, certification_state: str, final_status: str) -> bool:
    text = " ".join([status, blocker, certification_state, final_status, *reason_codes, *lifecycle_reason_codes]).upper()
    repair_markers = {
        "CORRUPT",
        "MALFORMED",
        "IMPOSSIBLE_TIMESTAMP",
        "CALENDAR_RULE",
        "INTEGRITY_FAILURE",
        "CERTIFICATION_FAILED",
        "FAILED_CERTIFICATION",
        "MISSING_REQUIRED_FEED",
        "REQUIRED_FEED_MISSING",
        "PROVIDER_NOT_CONFIGURED",
        "UNRECOVERABLE",
        "VALIDATOR_FAILURE",
        "INVALID_PAYLOAD",
    }
    return any(marker in text for marker in repair_markers)


def _candidate_operator_task_state(*, execution_eligible: bool, capture_status: str, manual_capture_eligible: bool, analytical_state: str, selected_state: str, promotion_status: str, status: str, reason_codes: list[str], lifecycle_reason_codes: list[str], blocker: str, certification_state: str, final_status: str) -> str:
    if execution_eligible and manual_capture_eligible and str(capture_status or "").upper() == "CAPTURE_READY":
        return MANUAL_IB_CAPTURE_READY
    if _candidate_requires_system_repair(
        status=status,
        reason_codes=reason_codes,
        lifecycle_reason_codes=lifecycle_reason_codes,
        blocker=blocker,
        certification_state=certification_state,
        final_status=final_status,
    ):
        return SYSTEM_REPAIR_REQUIRED
    if analytical_state == ANALYTICAL_BLOCKED:
        return MONITOR_ONLY
    return NO_USER_ACTION


def _candidate_operator_state_label(analytical_state: str, operator_task_state: str, execution_state: str, selected_state: str) -> str:
    if operator_task_state == MANUAL_IB_CAPTURE_READY:
        return "Manual IB capture ready"
    if operator_task_state == SYSTEM_REPAIR_REQUIRED:
        return "System repair required"
    if operator_task_state == MONITOR_ONLY:
        return "Monitor only"
    if analytical_state == ANALYTICAL_QUALIFIED:
        return "Qualified candidate" if selected_state != "SELECTED" else "Selected qualified candidate"
    if analytical_state == ANALYTICAL_BLOCKED:
        return "Monitor only"
    if analytical_state == ANALYTICAL_SUPPRESSED:
        return "Suppressed"
    if execution_state == EXECUTION_LOCKED_NON_CERTIFIED_ROW:
        return "Certification pending"
    return "No user action"


def _candidate_semantic_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    analytical_counts: dict[str, int] = {}
    affordance_counts: dict[str, int] = {}
    execution_counts: dict[str, int] = {}
    for row in rows:
        analytical_state = str(row.get("analytical_state") or ANALYTICAL_REVIEWABLE)
        operator_task_state = str(row.get("operator_task_state") or row.get("operator_affordance") or NO_USER_ACTION)
        execution_state = str(row.get("execution_state") or NON_EXECUTABLE)
        analytical_counts[analytical_state] = analytical_counts.get(analytical_state, 0) + 1
        affordance_counts[operator_task_state] = affordance_counts.get(operator_task_state, 0) + 1
        execution_counts[execution_state] = execution_counts.get(execution_state, 0) + 1
    return {
        "analytical_state_counts": analytical_counts,
        "operator_task_state_counts": affordance_counts,
        "operator_affordance_counts": affordance_counts,
        "execution_state_counts": execution_counts,
        "user_task_available_count": sum(affordance_counts.get(state, 0) for state in {MANUAL_IB_CAPTURE_READY, SYSTEM_REPAIR_REQUIRED}),
        "operator_action_available_count": sum(affordance_counts.get(state, 0) for state in {MANUAL_IB_CAPTURE_READY, SYSTEM_REPAIR_REQUIRED}),
        "manual_ib_capture_ready_count": affordance_counts.get(MANUAL_IB_CAPTURE_READY, 0),
        "manual_capture_available_count": affordance_counts.get(MANUAL_IB_CAPTURE_READY, 0),
        "system_repair_required_count": affordance_counts.get(SYSTEM_REPAIR_REQUIRED, 0),
        "execution_eligible_row_count": execution_counts.get(EXECUTION_ELIGIBLE_ROW, 0),
        "execution_locked_non_certified_count": execution_counts.get(EXECUTION_LOCKED_NON_CERTIFIED_ROW, 0),
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _candidate_row_sleeve(row: dict[str, Any]) -> str:
    return str(row.get("sleeve_id") or row.get("engine_id") or "UNKNOWN_SLEEVE").strip() or "UNKNOWN_SLEEVE"


def _candidate_row_status(row: dict[str, Any]) -> str:
    return str(row.get("status") or row.get("lifecycle_decision") or "").strip().upper()


def _candidate_row_is_qualified(row: dict[str, Any]) -> bool:
    analytical = str(row.get("analytical_state") or "").strip().upper()
    return analytical == ANALYTICAL_QUALIFIED or _candidate_row_status(row) in ACTIVE_INTRADAY_CANDIDATE_STATUSES


def _candidate_row_is_suppressed(row: dict[str, Any]) -> bool:
    analytical = str(row.get("analytical_state") or "").strip().upper()
    return analytical == ANALYTICAL_SUPPRESSED or _candidate_row_status(row) in {"SUPPRESSED", "SIGNAL_ONLY", "NO_SIGNAL"}


def _candidate_row_is_blocked(row: dict[str, Any]) -> bool:
    analytical = str(row.get("analytical_state") or "").strip().upper()
    return analytical == ANALYTICAL_BLOCKED or _candidate_row_status(row) == "BLOCKED"


def _candidate_row_is_selected(row: dict[str, Any], selected_intent_id: str = "") -> bool:
    if str(row.get("selection_status") or "").strip().upper() == "SELECTED":
        return True
    raw_intent_id = str(row.get("raw_intent_id") or row.get("intent_id") or "").strip()
    if selected_intent_id and raw_intent_id == selected_intent_id:
        return True
    return str(row.get("portfolio_gate_decision") or "").strip().upper() == "ALLOW" and row.get("allowed_by_portfolio_gate") is True


def _candidate_row_is_certified(row: dict[str, Any]) -> bool:
    if row.get("execution_eligible") is True:
        return True
    lane = str(row.get("candidate_lane") or "").strip().upper()
    state = str(row.get("certification_state") or row.get("final_eod_certification_status") or "").strip().upper()
    return lane == "CERTIFIED" and state in {"CERTIFIED", "PASS", "VALID"}


def _candidate_row_is_capture_ready(row: dict[str, Any]) -> bool:
    task = str(row.get("operator_task_state") or row.get("operator_affordance") or "").strip().upper()
    capture = str(row.get("capture_eligibility_status") or row.get("manual_capture_status") or "").strip().upper()
    return task == MANUAL_IB_CAPTURE_READY or capture == "CAPTURE_READY" or (row.get("manual_capture_eligible") is True and _candidate_row_is_certified(row))


def _candidate_counts(rows: list[dict[str, Any]], *, selected_intent_id: str = "") -> dict[str, Any]:
    generated = len(rows)
    qualified = sum(1 for row in rows if _candidate_row_is_qualified(row))
    suppressed = sum(1 for row in rows if _candidate_row_is_suppressed(row))
    blocked = sum(1 for row in rows if _candidate_row_is_blocked(row))
    selected = sum(1 for row in rows if _candidate_row_is_selected(row, selected_intent_id))
    certified = sum(1 for row in rows if _candidate_row_is_certified(row))
    capture_ready = sum(1 for row in rows if _candidate_row_is_capture_ready(row))
    return {
        "candidates_generated": generated,
        "qualified_count": qualified,
        "qualified_rate": _rate(qualified, generated),
        "suppressed_count": suppressed,
        "suppressed_rate": _rate(suppressed, generated),
        "blocked_count": blocked,
        "blocked_rate": _rate(blocked, generated),
        "selected_count": selected,
        "selected_rate": _rate(selected, generated),
        "certified_count": certified,
        "certified_rate": _rate(certified, generated),
        "manual_ib_capture_ready_count": capture_ready,
        "manual_ib_capture_ready_rate": _rate(capture_ready, generated),
    }


def _candidate_counts_by_sleeve(rows: list[dict[str, Any]], *, selected_intent_id: str = "") -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_candidate_row_sleeve(row), []).append(row)
    return {sleeve: _candidate_counts(sleeve_rows, selected_intent_id=selected_intent_id) for sleeve, sleeve_rows in sorted(grouped.items())}


def _candidate_suppression_reason(row: dict[str, Any]) -> str:
    for key in ("promotion_blocker", "blocker_summary", "rejection_reason", "non_selection_reason", "score_unavailable_reason"):
        value = str(row.get(key) or "").strip().upper()
        if value and value not in {"-", "NONE", "NO USER ACTION"}:
            return value
    for key in ("reason_codes", "lifecycle_reason_codes"):
        values = row.get(key) if isinstance(row.get(key), list) else []
        for value in values:
            text = str(value or "").strip().upper()
            if text:
                return text
    status = _candidate_row_status(row)
    return status or "UNSPECIFIED"


def _top_counter_rows(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    return [{"reason": reason, "count": count} for reason, count in counter.most_common(limit)]


def _candidate_diagnostics(rows: list[dict[str, Any]], scoring: dict[str, Any] | None = None) -> dict[str, Any]:
    suppression_reasons: Counter[str] = Counter()
    governance_blockers: Counter[str] = Counter()
    portfolio_gate_rejections: Counter[str] = Counter()
    for row in rows:
        decision = str(row.get("portfolio_gate_decision") or "").strip().upper()
        if decision and decision != "ALLOW":
            portfolio_gate_rejections[decision] += 1
        if _candidate_row_is_suppressed(row):
            suppression_reasons[_candidate_suppression_reason(row)] += 1
        if _candidate_row_is_blocked(row) or str(row.get("operator_task_state") or "").strip().upper() == SYSTEM_REPAIR_REQUIRED:
            governance_blockers[_candidate_suppression_reason(row)] += 1
    scoring_rows = scoring.get("rankings") if isinstance(scoring, dict) and isinstance(scoring.get("rankings"), list) else []
    score_values = []
    unavailable: Counter[str] = Counter()
    for row in scoring_rows:
        if not isinstance(row, dict):
            continue
        if row.get("score_total") is not None:
            try:
                score_values.append(float(row.get("score_total") or 0.0))
            except Exception:
                pass
        reason = str(row.get("score_unavailable_reason") or "").strip().upper()
        if reason:
            unavailable[reason] += 1
    return {
        "top_suppression_reasons": _top_counter_rows(suppression_reasons),
        "top_governance_blockers": _top_counter_rows(governance_blockers),
        "portfolio_gate_rejection_counts": dict(sorted(portfolio_gate_rejections.items())),
        "scoring_cutoffs": {
            "policy_id": str((scoring or {}).get("scoring_policy_id") or ""),
            "policy_version": str((scoring or {}).get("scoring_policy_version") or ""),
            "selected_candidate_intent_id": str((scoring or {}).get("selected_candidate_intent_id") or ""),
            "intents_scored_count": _safe_int((scoring or {}).get("intents_scored_count")),
            "ranked_count": len(scoring_rows),
            "top_score": round(max(score_values), 6) if score_values else 0.0,
            "lowest_score": round(min(score_values), 6) if score_values else 0.0,
            "score_unavailable_reasons": dict(sorted(unavailable.items())),
        },
    }


def _candidate_manifest_paths_for_window(root: Path, day_utc: str, limit: int = 20) -> list[Path]:
    base = root / "reports" / "candidate_generation_manifest_v1"
    if not base.exists() or not base.is_dir():
        return []
    candidates: list[tuple[str, float, Path]] = []
    for path in base.glob("*/*/candidate_generation_manifest.v1.json"):
        day = path.parts[-3] if len(path.parts) >= 3 else ""
        if not day or day > day_utc:
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0.0
        candidates.append((day, mtime, path))
    latest_by_day: dict[str, tuple[float, Path]] = {}
    for day, mtime, path in candidates:
        if day not in latest_by_day or (mtime, str(path)) > (latest_by_day[day][0], str(latest_by_day[day][1])):
            latest_by_day[day] = (mtime, path)
    return [path for _day, (_mtime, path) in sorted(latest_by_day.items(), reverse=True)[:limit]]


def _latest_sleeve_invocation_path(root: Path, day_utc: str) -> Path | None:
    base = root / "reports" / "sleeve_invocation_ledger_v1" / day_utc
    if not base.exists() or not base.is_dir():
        return None
    paths = sorted(base.glob("**/sleeve_invocation_ledger.v1.json"), key=lambda path: (path.stat().st_mtime_ns, str(path)), reverse=True)
    return paths[0] if paths else None


def _rolling_candidate_pipeline_observability(
    *,
    root: Path,
    day_utc: str,
    current_rows: list[dict[str, Any]],
    manifest: dict[str, Any],
    scoring: dict[str, Any],
    selected_intent_id: str,
    candidate_certification_state: str,
) -> dict[str, Any]:
    daily_rows: list[dict[str, Any]] = []
    rows_by_day: dict[str, list[dict[str, Any]]] = {}
    for path in _candidate_manifest_paths_for_window(root, day_utc, limit=20):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        day = str(payload.get("day_utc") or path.parts[-3] or "")
        rows = [row for row in payload.get("candidate_rows", []) if isinstance(row, dict)] if isinstance(payload.get("candidate_rows"), list) else []
        if day == day_utc and current_rows:
            rows = current_rows
        rows_by_day[day] = rows
    if day_utc not in rows_by_day:
        rows_by_day[day_utc] = current_rows
    for day, rows in sorted(rows_by_day.items()):
        daily_rows.append({"trading_day": day, "metrics": _candidate_counts(rows, selected_intent_id=selected_intent_id if day == day_utc else "")})

    def aggregate(days: int) -> dict[str, Any]:
        selected_days = sorted(rows_by_day.keys(), reverse=True)[:days]
        rows = [row for day in selected_days for row in rows_by_day.get(day, [])]
        return {
            "trading_days": sorted(selected_days),
            "metrics": _candidate_counts(rows),
            "by_sleeve": _candidate_counts_by_sleeve(rows),
        }

    current_metrics = _candidate_counts(current_rows, selected_intent_id=selected_intent_id)
    by_sleeve = _candidate_counts_by_sleeve(current_rows, selected_intent_id=selected_intent_id)
    previous_days = [row for row in daily_rows if row["trading_day"] < day_utc]
    previous_5 = previous_days[-5:]
    previous_generated = sum(_safe_int(row["metrics"].get("candidates_generated")) for row in previous_5)
    previous_qualified = sum(_safe_int(row["metrics"].get("qualified_count")) for row in previous_5)
    previous_suppressed = sum(_safe_int(row["metrics"].get("suppressed_count")) for row in previous_5)
    previous_qualified_rate = _rate(previous_qualified, previous_generated)
    previous_suppressed_rate = _rate(previous_suppressed, previous_generated)
    alerts: list[dict[str, Any]] = []
    invocation_path = _latest_sleeve_invocation_path(root, day_utc)
    if invocation_path is not None:
        try:
            ledger = json.loads(invocation_path.read_text(encoding="utf-8"))
        except Exception:
            ledger = {}
        invocations = ledger.get("invocations") if isinstance(ledger, dict) and isinstance(ledger.get("invocations"), list) else []
        row_sleeves = {_candidate_row_sleeve(row) for row in current_rows}
        for invocation in invocations:
            if not isinstance(invocation, dict):
                continue
            sleeve = str(invocation.get("engine_id") or "").strip()
            status = str(invocation.get("status") or "").strip().upper()
            if sleeve and sleeve not in row_sleeves and status not in {"NO_INTENT", "DISABLED", "FILTERED_OUT"}:
                alerts.append({"code": "SLEEVE_ZERO_CANDIDATES_UNEXPECTED", "severity": "warning", "sleeve_id": sleeve, "message": "Sleeve invocation ran without a corresponding candidate manifest row."})
    if current_metrics["candidates_generated"] and previous_generated >= 5 and previous_qualified_rate >= 0.5 and current_metrics["qualified_rate"] <= 0.2:
        alerts.append({"code": "QUALIFICATION_RATE_COLLAPSE", "severity": "warning", "message": "Qualification rate collapsed versus the rolling 5-day baseline."})
    if str(candidate_certification_state or "").upper() == CANDIDATES_CERTIFICATION_FAILED or any(str(row.get("operator_task_state") or "").upper() == SYSTEM_REPAIR_REQUIRED for row in current_rows):
        alerts.append({"code": "CERTIFICATION_FAILURE", "severity": "critical", "message": "Candidate certification or repair state failed."})
    manifest_snapshot_ids = manifest.get("input_market_data_snapshot_ids") if isinstance(manifest.get("input_market_data_snapshot_ids"), list) else []
    row_snapshot_ids = [item for row in current_rows for item in (row.get("input_market_data_snapshot_ids") if isinstance(row.get("input_market_data_snapshot_ids"), list) else [])]
    if current_metrics["candidates_generated"] and not manifest_snapshot_ids and not row_snapshot_ids:
        alerts.append({"code": "MISSING_MARKET_DATA_SNAPSHOT_IDS", "severity": "critical", "message": "Candidate rows are missing input market-data snapshot lineage."})
    if current_metrics["candidates_generated"] >= 3 and current_metrics["suppressed_rate"] >= 0.8 and (previous_generated == 0 or current_metrics["suppressed_rate"] >= previous_suppressed_rate + 0.3):
        alerts.append({"code": "SUPPRESSION_SPIKE", "severity": "warning", "message": "Suppression rate spiked versus the rolling baseline."})
    if current_metrics["candidates_generated"] == 0:
        regime_activity = "NO_CURRENT_CANDIDATE_ROWS"
    elif current_metrics["qualified_count"] == 0 and current_metrics["blocked_count"] == 0 and not alerts:
        regime_activity = "LOW_OPPORTUNITY_HEALTHY"
    elif alerts:
        regime_activity = "DIAGNOSTIC_ATTENTION"
    else:
        regime_activity = "NORMAL_ACTIVITY"
    return {
        "schema_id": "candidate_pipeline_observability",
        "schema_version": "v1",
        "day_utc": day_utc,
        "metrics": current_metrics,
        "by_sleeve": by_sleeve,
        "daily": daily_rows,
        "rolling_windows": {"5d": aggregate(5), "20d": aggregate(20)},
        "suppression_diagnostics": _candidate_diagnostics(current_rows, scoring),
        "alerts": alerts,
        "regime_activity_level": regime_activity,
        "capture_ready_trend": [
            {
                "trading_day": row["trading_day"],
                "count": _safe_int(row["metrics"].get("manual_ib_capture_ready_count")),
                "rate": row["metrics"].get("manual_ib_capture_ready_rate", 0.0),
            }
            for row in daily_rows[-20:]
        ],
    }


def _score_rows_by_intent(scoring: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = scoring.get("rankings") if isinstance(scoring.get("rankings"), list) else scoring.get("ranked_intents")
    out: dict[str, dict[str, Any]] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "").strip()
        if intent_id:
            out[intent_id] = row
    return out


def _arbitration_rows_by_intent(arbitration: dict[str, Any]) -> tuple[str, dict[str, dict[str, Any]]]:
    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    selected_id = str(selected.get("intent_id") or "").strip()
    out: dict[str, dict[str, Any]] = {}
    for collection in ("raw_candidate_intents", "candidate_intents", "rejected_or_filtered_intents"):
        rows = arbitration.get(collection) if isinstance(arbitration.get(collection), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            intent_id = str(row.get("intent_id") or row.get("raw_intent_id") or "").strip()
            if intent_id:
                out[intent_id] = {**out.get(intent_id, {}), **row}
    if selected_id:
        out[selected_id] = {**out.get(selected_id, {}), **selected}
    return selected_id, out


def _promotion_rows_by_candidate(promotion_map: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rows = promotion_map.get("candidate_rows") if isinstance(promotion_map.get("candidate_rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("candidate_id", "raw_intent_id"):
            value = str(row.get(key) or "").strip()
            if value:
                out[value] = row
    return out


def _intent_plane_rows_by_intent(intent_plane: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rows = intent_plane.get("intent_snapshots") if isinstance(intent_plane.get("intent_snapshots"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = str(row.get("intent_id") or "").strip()
        if value:
            out[value] = row
    return out


def _operator_candidate_rows(rows: list[Any], scoring: dict[str, Any] | None = None, arbitration: dict[str, Any] | None = None, promotion_map: dict[str, Any] | None = None, intent_plane: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    score_by_intent = _score_rows_by_intent(scoring or {})
    selected_intent_id, arbitration_by_intent = _arbitration_rows_by_intent(arbitration or {})
    promotion_by_id = _promotion_rows_by_candidate(promotion_map or {})
    intent_plane_by_id = _intent_plane_rows_by_intent(intent_plane or {})
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or row.get("lifecycle_decision") or "").upper()
        decision = str(row.get("portfolio_gate_decision") or row.get("lifecycle_decision") or status or "").upper()
        reason_codes = [str(code or "") for code in row.get("reason_codes", [])] if isinstance(row.get("reason_codes"), list) else []
        lifecycle_reason_codes = [str(code or "") for code in row.get("lifecycle_reason_codes", [])] if isinstance(row.get("lifecycle_reason_codes"), list) else []
        raw_intent_id = str(row.get("raw_intent_id") or "").strip()
        score_row = score_by_intent.get(raw_intent_id, {}) if raw_intent_id else {}
        arbitration_row = arbitration_by_intent.get(raw_intent_id, {}) if raw_intent_id else {}
        promotion_row = promotion_by_id.get(raw_intent_id) or promotion_by_id.get(str(row.get("candidate_id") or "")) or {}
        intent_row = intent_plane_by_id.get(raw_intent_id) or intent_plane_by_id.get(str(row.get("candidate_id") or "")) or {}
        score_available = bool(score_row)
        selected_state = "SELECTED" if raw_intent_id and raw_intent_id == selected_intent_id else "NOT_SELECTED" if raw_intent_id else "NOT_APPLICABLE"
        non_selection_reason = "" if selected_state == "SELECTED" else str(arbitration_row.get("rejection_reason") or score_row.get("score_unavailable_reason") or "")
        candidate_lane = str(row.get("candidate_lane") or promotion_row.get("candidate_lane") or "").strip().upper()
        row_certification_state = str(row.get("certification_state") or promotion_row.get("certification_state") or "").strip().upper()
        final_status = str(row.get("final_eod_certification_status") or "").strip().upper()
        if not candidate_lane:
            candidate_lane = "CERTIFIED" if final_status in {"CERTIFIED", "VALID", "PASS"} and row.get("final_eod_certification_pending") is not True else "PROVISIONAL"
        if not row_certification_state:
            row_certification_state = "CERTIFIED" if candidate_lane == "CERTIFIED" and final_status in {"CERTIFIED", "VALID", "PASS"} else "CERTIFICATION_PENDING"
        execution_eligible = bool(row.get("execution_eligible") is True or promotion_row.get("execution_eligible") is True) and candidate_lane == "CERTIFIED" and row_certification_state == "CERTIFIED"
        read_only = not execution_eligible
        input_snapshot_ids = row.get("input_market_data_snapshot_ids") if isinstance(row.get("input_market_data_snapshot_ids"), list) else (promotion_row.get("input_market_data_snapshot_ids") if isinstance(promotion_row.get("input_market_data_snapshot_ids"), list) else [])
        certification_label = "CERTIFIED" if execution_eligible else "NON_CERTIFIED"
        capture_status = str(promotion_row.get("manual_capture_status") or "").strip().upper() or ("CAPTURE_READY" if execution_eligible and status in {"CAPTURE_READY", "ACTIVE_CURRENT"} and selected_state == "SELECTED" else "NOT_CAPTURE_READY")
        if not execution_eligible and selected_state == "SELECTED":
            capture_status = "READ_ONLY_NON_CERTIFIED"
        lifecycle_stage = str(promotion_row.get("final_operator_status") or "").strip().upper() or ("SELECTED_READ_ONLY_NON_CERTIFIED" if selected_state == "SELECTED" and not execution_eligible else "SELECTED_NOT_CAPTURE_READY" if selected_state == "SELECTED" and capture_status != "CAPTURE_READY" else capture_status)
        exact_blocker = str(promotion_row.get("exact_blocker") or "").strip()
        if selected_state == "SELECTED" and not execution_eligible:
            lifecycle_stage = "SELECTED_READ_ONLY_NON_CERTIFIED"
            exact_blocker = "NON_CERTIFIED_CANDIDATE_SNAPSHOT"
        manual_capture_eligible = bool(row.get("manual_capture_eligible") is True and execution_eligible)
        analytical_state = _candidate_analytical_state(status)
        execution_state = _candidate_execution_state(
            execution_eligible=execution_eligible,
            candidate_lane=candidate_lane,
            certification_state=row_certification_state,
            analytical_state=analytical_state,
        )
        blocker_for_task = str(exact_blocker or non_selection_reason or row.get("rejection_reason") or (reason_codes[0] if reason_codes else ""))
        operator_task_state = _candidate_operator_task_state(
            execution_eligible=execution_eligible,
            capture_status=capture_status,
            manual_capture_eligible=manual_capture_eligible,
            analytical_state=analytical_state,
            selected_state=selected_state,
            promotion_status=str(promotion_row.get("promotion_status") or ""),
            status=status,
            reason_codes=reason_codes,
            lifecycle_reason_codes=lifecycle_reason_codes,
            blocker=blocker_for_task,
            certification_state=row_certification_state,
            final_status=final_status,
        )
        operator_state = _candidate_operator_state_label(analytical_state, operator_task_state, execution_state, selected_state)
        if status in ACTIVE_INTRADAY_CANDIDATE_STATUSES:
            if operator_task_state == MANUAL_IB_CAPTURE_READY:
                next_action = "Manual IB capture ticket is ready; enter the trade in IB and record capture complete."
            elif selected_state == "SELECTED" and not execution_eligible:
                next_action = "Aegis is waiting for EOD certification before it can create a manual IB capture ticket."
            elif selected_state == "SELECTED":
                next_action = str(promotion_row.get("next_safe_action") or "Aegis is preparing governed conversion and submit-boundary evidence before capture ticket creation.")
            elif score_available and selected_state == "NOT_SELECTED":
                next_action = "No user action; candidate was not selected by portfolio scoring."
            elif raw_intent_id:
                next_action = "Aegis is missing portfolio score evidence; automatic scoring and arbitration should rerun."
            else:
                next_action = "No user action."
        elif status == "BLOCKED":
            next_action = "Monitor only; Aegis will continue governed repair unless a system repair task appears." if operator_task_state != SYSTEM_REPAIR_REQUIRED else "System repair required before Aegis can continue this candidate."
        elif status in {"SUPPRESSED", "SIGNAL_ONLY", "NO_SIGNAL"}:
            next_action = "No user action."
        else:
            next_action = "Monitor only."
        out.append({
            "candidate_id": str(row.get("candidate_id") or row.get("raw_intent_id") or ""),
            "raw_intent_id": raw_intent_id,
            "symbol": str(row.get("symbol_or_pair") or row.get("symbol") or "").upper(),
            "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or ""),
            "engine_id": str(row.get("engine_id") or row.get("sleeve_id") or ""),
            "status": status,
            "operator_state": operator_state,
            "analytical_state": analytical_state,
            "operator_task_state": operator_task_state,
            "operator_affordance": operator_task_state,
            "execution_state": execution_state,
            "portfolio_gate_decision": decision,
            "manual_capture_eligible": manual_capture_eligible,
            "capture_eligibility_status": capture_status,
            "candidate_lane": candidate_lane,
            "certification_state": row_certification_state,
            "certification_label": certification_label,
            "execution_eligible": execution_eligible,
            "read_only": read_only,
            "input_market_data_snapshot_ids": list(input_snapshot_ids),
            "execution_firewall_status": "PASS" if execution_eligible else "REJECTED_NON_CERTIFIED_INPUT",
            "promotion_lifecycle_stage": lifecycle_stage,
            "promotion_status": str(promotion_row.get("promotion_status") or ""),
            "construction_status": str(promotion_row.get("construction_status") or ""),
            "allocation_status": str(promotion_row.get("allocation_status") or ""),
            "risk_status": str(promotion_row.get("risk_status") or ""),
            "identity_status": str(promotion_row.get("identity_status") or ""),
            "economic_state_status": str(promotion_row.get("economic_state_status") or ""),
            "conversion_status": str(promotion_row.get("conversion_status") or ""),
            "submit_boundary_status": str(promotion_row.get("submit_boundary_status") or ""),
            "ticket_lineage_status": str(promotion_row.get("ticket_lineage_status") or ""),
            "promotion_blocker": exact_blocker,
            "portfolio_score_available": score_available,
            "portfolio_score_total": score_row.get("score_total") if score_available else None,
            "portfolio_score_rank": score_row.get("rank") if score_available else None,
            "portfolio_scoring_status": "SCORED" if score_available else ("NOT_APPLICABLE" if not raw_intent_id else "SCORE_UNAVAILABLE"),
            "score_unavailable_reason": "" if score_available else ("NO_INTENT_DECLARED" if not raw_intent_id else "PORTFOLIO_SCORING_MISSING_INTENT_SCORE"),
            "selection_status": selected_state,
            "non_selection_reason": non_selection_reason,
            "source_data_mode": str(row.get("source_data_mode") or ""),
            "candidate_data_status": str(row.get("candidate_data_status") or ""),
            "final_eod_certification_status": str(row.get("final_eod_certification_status") or ""),
            "final_eod_certification_pending": bool(row.get("final_eod_certification_pending") is True),
            "reason_codes": reason_codes,
            "lifecycle_reason_codes": lifecycle_reason_codes,
            "blocker_summary": str(exact_blocker or non_selection_reason or row.get("rejection_reason") or (reason_codes[0] if reason_codes else "")),
            "intent_id": str(intent_row.get("intent_id") or raw_intent_id),
            "intent_state": str(intent_row.get("intent_state") or "DISCOVERED"),
            "confidence_score": intent_row.get("confidence_score"),
            "stability_score": intent_row.get("stability_score"),
            "certification_convergence_score": intent_row.get("certification_convergence_score"),
            "data_completeness_score": intent_row.get("data_completeness_score"),
            "capture_guidance": str(intent_row.get("capture_guidance") or "NO_USER_ACTION"),
            "capture_guidance_reason": str(intent_row.get("capture_guidance_reason") or ""),
            "recommendation_mode": str(intent_row.get("recommendation_mode") or "NONE"),
            "intent_execution_eligibility_state": str(intent_row.get("execution_eligibility_state") or "BROKER_AUTOMATION_DISABLED"),
            "next_expected_lifecycle_transition": "final certification convergence" if str(intent_row.get("capture_guidance") or "") == "AWAIT_CERTIFICATION" else "operator may record manual IB capture" if str(intent_row.get("capture_guidance") or "") == "MANUAL_IB_CAPTURE_RECOMMENDED" else "continue observation",
            "next_action": str(intent_row.get("capture_guidance_reason") or next_action),
            "lineage_hash": str(row.get("lineage_hash") or ""),
            "raw_intent_path": str(row.get("raw_intent_path") or ""),
        })
    return out


def _report_path(root: Path, family: str, day_utc: str, filename: str) -> Path:
    return root / "reports" / family / day_utc / filename


def portfolio_gate_candidate_report_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "portfolio_gate_candidate_report_v1", day_utc, "portfolio_gate_candidate_report.v1.json")


def regime_bucket_ranking_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "regime_bucket_candidate_ranking_v1", day_utc, "regime_bucket_candidate_ranking.v1.json")


def selected_pointer_path_v1(root: Path) -> Path:
    return root / "pointers" / "selected_intent_pointer.v1.json"


def candidate_diagnostics_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "paper_intent_candidate_diagnostics_v1", day_utc, "paper_intent_candidate_diagnostics.v1.json")


def market_data_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")


def market_data_inputs_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")


def market_data_readiness_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "market_data_readiness_v1", day_utc, "market_data_readiness.v1.json")


def candidate_generation_manifest_path_v1(root: Path, day_utc: str) -> Path:
    base = root / "reports" / "candidate_generation_manifest_v1" / day_utc
    default_path = base / f"sleeve_evaluation_kernel_v1:{day_utc}" / "candidate_generation_manifest.v1.json"
    paths = sorted(base.glob("*/candidate_generation_manifest.v1.json")) if base.exists() else []
    if not paths:
        return default_path
    def _rank(path: Path) -> tuple[int, float, str]:
        parent = path.parent.name
        intraday = 1 if parent.startswith(f"aegis_intraday_sleeves_now:{day_utc}:") else 0
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0.0
        return (intraday, mtime, parent)
    return max(paths, key=_rank)


def intent_arbitration_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "intent_arbitration_v1", day_utc, "intent_arbitration.v1.json")


def portfolio_scoring_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "portfolio_scoring_v1", day_utc, "portfolio_scoring.v1.json")


def candidate_promotion_map_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "candidate_promotion_map_v1", day_utc, "candidate_promotion_map.v1.json")


def market_manifest_path_v1(root: Path) -> Path:
    return root / "market_data_snapshot_v1" / "dataset_manifest.json"




def _latest_market_data_retry_job(root: Path, day_utc: str) -> dict[str, Any]:
    base = root / "reports" / "aegis_data_remediation_jobs_v1" / day_utc
    if not base.exists() or not base.is_dir():
        return {}
    rows: list[tuple[float, Path, dict[str, Any]]] = []
    for path in base.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            rows.append((path.stat().st_mtime, path, payload))
        except Exception:
            continue
    if not rows:
        return {}
    _mtime, path, payload = max(rows, key=lambda row: (row[0], str(row[1])))
    return {
        "job_id": str(payload.get("job_id") or ""),
        "status": str(payload.get("status") or ""),
        "queued_at_utc": str(payload.get("queued_at_utc") or ""),
        "last_attempt": payload.get("last_attempt") if isinstance(payload.get("last_attempt"), dict) else {},
        "last_error": str(payload.get("last_error") or ""),
        "next_retry_utc": str(payload.get("next_retry_utc") or ""),
        "validation_status": str(payload.get("validation_status") or ""),
        "path": str(path),
    }

def _scheduler_failure_summary(root: Path, day_utc: str) -> dict[str, Any]:
    patterns = (
        root / "reports" / "aegis_day_run_v1" / day_utc,
        root / "reports" / "session_readiness_refresh_v1" / day_utc,
        root / "reports" / "aegis_lite_eod_report_v1" / day_utc,
        root / "reports" / "eod_run_manifest_v1" / day_utc,
    )
    needles = ("REFUSE_OVERWRITE_EXISTING_ARTIFACT", "REFUSE_OVERWRITE_EXISTING_FILE", "REFUSE_OVERWRITE")
    checked = 0
    for base in patterns:
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.json"), key=lambda item: str(item))[:50]:
            checked += 1
            try:
                text = path.read_text(encoding="utf-8", errors="replace")[:250000]
            except OSError:
                continue
            for needle in needles:
                if needle in text:
                    return {
                        "status": "FAILED",
                        "blocker": needle,
                        "message": "Scheduled run hit an artifact overwrite guard; the run was not silently treated as healthy.",
                        "source_path": str(path),
                        "next_action": "Rerun with the versioned/idempotent writer path, then refresh operator state.",
                    }
    return {"status": "OK", "checked_artifact_count": checked}


def research_status_path_v1(root: Path, day_utc: str) -> Path:
    return _report_path(root, "aegis_research_lab_execution_loop_v1", day_utc, "research_lab_execution_loop.v1.json")


def _day_from_gate_path(path: Path) -> str:
    try:
        return path.parent.name
    except Exception:
        return ""


def _gate_report_paths(root: Path, day_utc: str | None = None) -> list[Path]:
    if day_utc:
        path = portfolio_gate_candidate_report_path_v1(root, day_utc)
        return [path] if path.exists() else []
    base = root / "reports" / "portfolio_gate_candidate_report_v1"
    if not base.exists() or not base.is_dir():
        return []
    return sorted(base.glob("*/portfolio_gate_candidate_report.v1.json"), key=lambda p: (_day_from_gate_path(p), p.stat().st_mtime_ns, str(p)), reverse=True)


def _latest_historical_gate_summary(root: Path, requested_day: str) -> dict[str, Any]:
    for path in _gate_report_paths(root, None):
        source_day = _day_from_gate_path(path)
        if source_day >= requested_day:
            continue
        errors: list[dict[str, Any]] = []
        report = _read_json(path, errors, "historical_portfolio_gate_candidate_report")
        selected_id, selected_row = _selected_row_from_gate(report)
        if not report or not selected_id:
            continue
        return {
            "source_day": source_day,
            "artifact_path": str(path.resolve()),
            "selected_exposure_intent_id": selected_id,
            "symbol": str(selected_row.get("symbol") or "").upper(),
            "sleeve_id": str(selected_row.get("sleeve_id") or selected_row.get("engine_id") or ""),
            "candidate_count": int(report.get("candidate_count") or len(report.get("candidate_rows") or [])),
            "suppressed_count": int(report.get("suppressed_count") or 0),
            "current_truth_status": "STALE_FALLBACK",
        }
    return {}


def _current_day_status(root: Path, day_utc: str, errors: list[dict[str, Any]]) -> dict[str, Any]:
    refs: list[dict[str, Any]] = []
    market_path = market_data_path_v1(root, day_utc)
    inputs_path = market_data_inputs_path_v1(root, day_utc)
    readiness_path = market_data_readiness_path_v1(root, day_utc)
    manifest_path = candidate_generation_manifest_path_v1(root, day_utc)
    arbitration_path = intent_arbitration_path_v1(root, day_utc)
    scoring_path = portfolio_scoring_path_v1(root, day_utc)
    promotion_map_path = candidate_promotion_map_path_v1(root, day_utc)
    intent_plane_path = candidate_intent_plane_path_v1(truth_root=root, day_utc=day_utc)
    pointer_path = selected_pointer_path_v1(root)

    market = _read_json(market_path, errors, "aegis_market_data") if market_path.exists() else {}
    inputs = _read_json(inputs_path, errors, "market_data_inputs") if inputs_path.exists() else {}
    readiness = _read_json(readiness_path, errors, "market_data_readiness") if readiness_path.exists() else {}
    manifest = _read_json(manifest_path, errors, "candidate_generation_manifest") if manifest_path.exists() else {}
    arbitration = _read_json(arbitration_path, errors, "intent_arbitration") if arbitration_path.exists() else {}
    scoring = _read_json(scoring_path, errors, "portfolio_scoring") if scoring_path.exists() else {}
    promotion_map = _read_json(promotion_map_path, errors, "candidate_promotion_map") if promotion_map_path.exists() else {}
    intent_plane = _read_json(intent_plane_path, errors, "candidate_intent_plane") if intent_plane_path.exists() else {}
    if not intent_plane:
        intent_plane = build_candidate_intent_plane_v1(truth_root=root, day_utc=day_utc, write_histories=False)
    pointer = _read_json(pointer_path, errors, "selected_intent_pointer") if pointer_path.exists() else {}

    for ref_path, artifact_id, payload in (
        (market_path, "aegis_market_data_v1", market),
        (inputs_path, "market_data_inputs_v1", inputs),
        (readiness_path, "market_data_readiness_v1", readiness),
        (manifest_path, "candidate_generation_manifest_v1", manifest),
        (arbitration_path, "intent_arbitration_v1", arbitration),
        (scoring_path, "portfolio_scoring_v1", scoring),
        (promotion_map_path, "candidate_promotion_map_v1", promotion_map),
        (intent_plane_path, "candidate_intent_plane_v1", intent_plane),
        (pointer_path, "selected_intent_pointer_v1", pointer),
    ):
        refs.append(_source_ref(ref_path, artifact_id, payload))

    missing_symbols = sorted({
        str(symbol)
        for symbol in [
            *(market.get("missing_symbols") if isinstance(market.get("missing_symbols"), list) else []),
            *(market.get("provider_failed_symbols") if isinstance(market.get("provider_failed_symbols"), list) else []),
            *(inputs.get("missing_symbols") if isinstance(inputs.get("missing_symbols"), list) else []),
            *(inputs.get("stale_symbols") if isinstance(inputs.get("stale_symbols"), list) else []),
        ]
        if str(symbol or "").strip()
    })
    missing_input_ids = sorted({
        str(item)
        for item in [
            *(inputs.get("missing_input_ids") if isinstance(inputs.get("missing_input_ids"), list) else []),
            *(inputs.get("stale_input_ids") if isinstance(inputs.get("stale_input_ids"), list) else []),
            *(readiness.get("blocked_input_ids") if isinstance(readiness.get("blocked_input_ids"), list) else []),
        ]
        if str(item or "").strip()
    })
    provider_results = market.get("provider_results") if isinstance(market.get("provider_results"), list) else []
    finalization_window = market.get("finalization_window") if isinstance(market.get("finalization_window"), dict) else {}
    market_calendar = market.get("market_calendar") if isinstance(market.get("market_calendar"), dict) else {}
    retry_job_for_timing = _latest_market_data_retry_job(root, day_utc)
    market_data_last_updated_at = str(market.get("generated_at_utc") or inputs.get("generated_at_utc") or market.get("updated_at_utc") or "")
    candidate_snapshot_generated_at = str(manifest.get("produced_at_utc") or manifest.get("generated_at_utc") or "")
    last_certification_attempt_at = str(
        market.get("last_certification_attempt_at")
        or market.get("last_certification_attempt_at_utc")
        or market.get("final_eod_certification_attempted_at")
        or market.get("final_eod_certification_attempted_at_utc")
        or (market.get("last_attempt", {}) if isinstance(market.get("last_attempt"), dict) else {}).get("attempted_at_utc")
        or ""
    )
    final_eod_certification_completed_at = str(
        market.get("final_eod_certification_completed_at")
        or market.get("final_eod_certification_completed_at_utc")
        or market.get("certified_at_utc")
        or market.get("final_eod_certified_at_utc")
        or ""
    )
    market_close_at = str(
        finalization_window.get("market_close_at_utc")
        or finalization_window.get("market_close_utc")
        or market_calendar.get("market_close_at_utc")
        or market_calendar.get("market_close_utc")
        or ""
    )
    vendor_lag_window = str(
        finalization_window.get("vendor_lag_window")
        or finalization_window.get("vendor_lag_buffer")
        or finalization_window.get("vendor_lag_buffer_minutes")
        or market.get("vendor_lag_window")
        or ""
    )
    estimated_next_certification_attempt_at = str(
        market.get("estimated_next_certification_attempt_at")
        or market.get("estimated_next_certification_attempt_at_utc")
        or market.get("next_certification_attempt_at")
        or market.get("next_retry_utc")
        or retry_job_for_timing.get("next_retry_utc")
        or ""
    )
    final_certification_status = str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or manifest.get("final_eod_certification_status") or "")
    eod_certification_timing = {
        "market_close_at": market_close_at,
        "vendor_lag_window": vendor_lag_window,
        "certification_pending": bool(market.get("final_eod_certification_pending") is True or inputs.get("final_eod_certification_pending") is True or str(final_certification_status).upper() == "PENDING"),
        "estimated_next_certification_attempt_at": estimated_next_certification_attempt_at,
        "final_certification_status": final_certification_status,
    }
    selected_candidate_count = int(promotion_map.get("selected_candidate_count") or 0) if promotion_map else (1 if str(arbitration.get("status") or "").upper() == "SELECTED" else 0)
    capture_ready_count = int(promotion_map.get("capture_ready_count") or 0) if promotion_map else 0
    blocked_selected_count = int(promotion_map.get("blocked_selected_count") or 0) if promotion_map else 0
    scheduler_failure = _scheduler_failure_summary(root, day_utc)
    candidate_rows = manifest.get("candidate_rows") if isinstance(manifest.get("candidate_rows"), list) else []
    summary = manifest.get("summary") if isinstance(manifest.get("summary"), dict) else {}
    status_counts = summary.get("status_counts") if isinstance(summary.get("status_counts"), dict) else {}
    blocked_count = len([row for row in candidate_rows if isinstance(row, dict) and str(row.get("status") or "").upper() == "BLOCKED"])
    candidate_count = int(summary.get("candidate_count") or len(candidate_rows))
    current_intraday_candidate_count = len([
        row
        for row in candidate_rows
        if isinstance(row, dict) and str(row.get("status") or row.get("lifecycle_decision") or "").upper() in ACTIVE_INTRADAY_CANDIDATE_STATUSES
    ])
    if not current_intraday_candidate_count and status_counts:
        current_intraday_candidate_count = sum(int(status_counts.get(status) or 0) for status in ACTIVE_INTRADAY_CANDIDATE_STATUSES)

    critical_symbols = sorted({
        str(symbol).upper()
        for symbol in [
            *(inputs.get("missing_symbols") if isinstance(inputs.get("missing_symbols"), list) else []),
            *(inputs.get("stale_symbols") if isinstance(inputs.get("stale_symbols"), list) else []),
            *[
                str(item).split(".")[-1]
                for item in (readiness.get("blocked_input_ids") if isinstance(readiness.get("blocked_input_ids"), list) else [])
                if str(item or "").startswith(("market.price.", "market.volatility."))
            ],
        ]
        if str(symbol or "").strip()
    })
    operator_candidate_rows = _operator_candidate_rows(candidate_rows, scoring=scoring, arbitration=arbitration, promotion_map=promotion_map, intent_plane=intent_plane)
    candidate_semantic_counts = _candidate_semantic_counts(operator_candidate_rows)
    selected_intent = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    selected_intent_id = str(selected_intent.get("intent_id") or "").strip()
    candidate_pipeline_observability = _rolling_candidate_pipeline_observability(
        root=root,
        day_utc=day_utc,
        current_rows=operator_candidate_rows,
        manifest=manifest,
        scoring=scoring,
        selected_intent_id=selected_intent_id,
        candidate_certification_state=_candidate_certification_state_from_payload(manifest),
    )
    intent_rows = intent_plane.get("intent_snapshots") if isinstance(intent_plane.get("intent_snapshots"), list) else []
    intent_lifecycle_summary = {
        "intent_count": int(intent_plane.get("intent_count") or len(intent_rows)),
        "selected_intent_count": int(intent_plane.get("selected_intent_count") or len([row for row in intent_rows if isinstance(row, dict) and row.get("selected") is True])),
        "manual_ib_capture_recommended_count": int(intent_plane.get("manual_ib_capture_recommended_count") or len([row for row in intent_rows if isinstance(row, dict) and row.get("capture_guidance") == "MANUAL_IB_CAPTURE_RECOMMENDED"])),
        "certification_diverged_count": int(intent_plane.get("certification_diverged_count") or len([row for row in intent_rows if isinstance(row, dict) and row.get("intent_state") == "CERTIFICATION_DIVERGED"])),
    }
    market_status = str(market.get("status") or inputs.get("status") or "").upper()
    market_failure = str(market.get("failure_reason") or inputs.get("provider_failure_reason") or "").strip()
    operator_market_state = str(market.get("operator_market_data_state") or "").upper()
    symbols_payload = market.get("symbols") if isinstance(market.get("symbols"), dict) else {}
    fetched_symbols = [str(symbol).upper() for symbol in (market.get("fetched_symbols") if isinstance(market.get("fetched_symbols"), list) else list(symbols_payload.keys())) if str(symbol or "").strip()]
    requested_symbols = [str(symbol).upper() for symbol in (market.get("requested_symbols") if isinstance(market.get("requested_symbols"), list) else fetched_symbols + missing_symbols) if str(symbol or "").strip()]
    stale_market_symbols = [str(symbol).upper() for symbol in (market.get("stale_symbols") if isinstance(market.get("stale_symbols"), list) else inputs.get("stale_symbols") if isinstance(inputs.get("stale_symbols"), list) else []) if str(symbol or "").strip()]
    current_session_symbols = sorted(
        str(symbol).upper()
        for symbol, row in symbols_payload.items()
        if isinstance(row, dict)
        and str(row.get("freshness_status") or "").upper() == "CURRENT"
        and str(row.get("market_session_date") or row.get("returned_data_date") or market.get("market_session_date") or day_utc) == day_utc
    )
    freshness_state = str(market.get("freshness_state") or market.get("validation_status") or "").upper()
    freshness_state_explicit = freshness_state in FRESHNESS_STATES
    freshness_decision = None
    if freshness_state not in FRESHNESS_STATES:
        freshness_decision = classify_market_data_freshness_v1(
            truth_root=root,
            day_utc=day_utc,
            required_symbols=requested_symbols,
            fetched_symbols=fetched_symbols,
            missing_symbols=missing_symbols,
            stale_symbols=stale_market_symbols,
            current_session_symbols=current_session_symbols,
            as_of_utc=str(market.get("generated_at_utc") or inputs.get("generated_at_utc") or ""),
            provider_status=market_status,
            provider_error=market_failure,
        )
        freshness_state = freshness_decision.freshness_state
    market_messages = {
        PENDING_VENDOR_DATA: ("Market data is still pending from vendors. Prior-day fallback is read-only.", "market_data_vendor_pending"),
        PARTIAL_DATA_AVAILABLE: ("Partial current-day market data is available; candidate generation is held until validation.", "market_data_partial_current_day"),
        READ_ONLY_PRIOR_DAY_FALLBACK: ("Current-day data is not validated. Prior-day fallback is read-only.", "market_data_read_only_prior_day_fallback"),
        VALIDATED_CURRENT_DAY: ("Current-day market data is available for read-only candidates; final EOD certification is pending.", "market_data_validated_current_day"),
        PROVISIONAL_INTRADAY: ("Provisional intraday market data is available for read-only candidates.", "market_data_provisional_intraday"),
        CERTIFICATION_PENDING: ("Current-day candidate visibility is available; final EOD certification is pending.", "market_data_certification_pending"),
        CERTIFIED: ("Certified current-day market data is ready.", "market_data_certified"),
        STALE: ("Current-day data is stale. Prior-day fallback is read-only.", "market_data_stale"),
        INVALID: ("Current-day market data failed integrity validation.", "market_data_invalid"),
        "MARKET_NOT_FINALIZED_YET": ("Market data is not finalized yet. Aegis will retry after the market-close vendor-lag window.", "market_data_finalization"),
        "PROVIDER_TIMEOUT": ("Provider timeout. Some symbols were not fetched.", "market_data_provider_timeout"),
        "PROVIDER_SOURCE_UNAVAILABLE": ("Provider source unavailable. Current-day final data is not available.", "market_data_provider_source_unavailable"),
        "CURRENT_DAY_DATA_STALE": ("Current-day market data is stale. Prior-day final rows cannot satisfy today’s final data requirement.", "market_data_stale"),
        "PARTIAL_PROVIDER_SUCCESS": ("Partial market data available. Missing or non-final symbols remain blocked.", "market_data_partial_provider_success"),
        "FINAL_EOD_READY": ("Final EOD market data is ready.", "market_data_final_eod_ready"),
        "INTRADAY_OPERATIONAL_READY": ("Today’s intraday sleeve run can proceed with current provisional market data. Final EOD certification is pending.", "market_data_intraday_operational"),
    }
    freshness_should_drive = freshness_state_explicit or market_status in {"FAILED", "STALE", "PARTIAL"} or str(inputs.get("validation_status") or "").upper() == "BLOCKED"
    effective_market_state = freshness_state if freshness_should_drive and freshness_state in FRESHNESS_STATES else operator_market_state
    if market_status in {"FAILED", "STALE", "PARTIAL"} or operator_market_state in market_messages or (freshness_state_explicit and effective_market_state in market_messages) or str(inputs.get("validation_status") or "").upper() == "BLOCKED":
        message, failed_step = market_messages.get(effective_market_state) or market_messages.get(operator_market_state, ("Today’s run failed: market data unavailable.", "market_data_refresh" if market_status == "FAILED" else "market_data"))
        if effective_market_state in FRESHNESS_STATES:
            status_value = effective_market_state
        else:
            status_value = operator_market_state if operator_market_state in market_messages and operator_market_state != "FINAL_EOD_READY" else "CURRENT_DAY_FAILED"
        retry_next = str(market.get("next_retry_utc") or ((freshness_decision.next_retry_utc if freshness_decision else "")))
        last_attempt = market.get("last_attempt") if isinstance(market.get("last_attempt"), dict) else ((freshness_decision.last_attempt if freshness_decision else {}) or {})
        retry_job = _latest_market_data_retry_job(root, day_utc)
        return _with_operator_state_semantics({
            "status": status_value,
            "runtime_mode": _runtime_mode_from_status(status_value),
            "failed_step": failed_step,
            "blocker": "" if status_value in {"INTRADAY_OPERATIONAL_READY", "FINAL_EOD_READY", VALIDATED_CURRENT_DAY} else (effective_market_state or operator_market_state or market_failure or "MARKET_DATA_STALE_OR_UNUSABLE"),
            "message": message,
            "last_attempt_time": str(market.get("generated_at_utc") or inputs.get("generated_at_utc") or ""),
            "next_action": str(market.get("next_action") or ("Review today's Candidates workspace; final EOD certification remains pending." if status_value in {"INTRADAY_OPERATIONAL_READY", VALIDATED_CURRENT_DAY} else "Wait for vendor data or retry market data refresh; prior-day fallback remains read-only." if status_value in {PENDING_VENDOR_DATA, PARTIAL_DATA_AVAILABLE, READ_ONLY_PRIOR_DAY_FALLBACK} else "Retry after market-data finalization window; do not use stale prior-day prices." if operator_market_state == "MARKET_NOT_FINALIZED_YET" else "Retry market data refresh; do not use stale prior-day prices.")),
            "missing_symbols": missing_symbols,
            "critical_missing_symbols": critical_symbols or missing_symbols,
            "missing_input_ids": missing_input_ids,
            "provider_results": provider_results,
            "provider_attempts_artifact": str(market.get("provider_attempts_artifact") or ""),
            "market_data_last_updated_at": market_data_last_updated_at,
            "candidate_snapshot_generated_at": candidate_snapshot_generated_at,
            "last_certification_attempt_at": last_certification_attempt_at,
            "final_eod_certification_completed_at": final_eod_certification_completed_at,
            "eod_certification_timing": eod_certification_timing,
            "operator_market_data_state": operator_market_state,
            "freshness_state": freshness_state,
            "validation_status": freshness_state,
            "market_calendar": market.get("market_calendar") if isinstance(market.get("market_calendar"), dict) else ((freshness_decision.market_calendar if freshness_decision else {}) or {}),
            "expected_current_data_after_utc": str(market.get("expected_current_data_after_utc") or ((freshness_decision.expected_current_data_after_utc if freshness_decision else ""))),
            "next_retry_utc": retry_next,
            "last_market_data_attempt": last_attempt,
            "finalization_window": market.get("finalization_window") if isinstance(market.get("finalization_window"), dict) else {},
            "final_eod_ready": bool(market.get("final_eod_ready") is True),
            "intraday_operational_ready": bool(market.get("intraday_operational_ready") is True),
            "market_data_mode": str(market.get("market_data_mode") or inputs.get("market_data_mode") or ""),
            "candidate_lane": str(manifest.get("candidate_lane") or market.get("candidate_lane") or ""),
            "certification_state": str(manifest.get("certification_state") or market.get("certification_state") or ""),
            "input_market_data_snapshot_ids": manifest.get("input_market_data_snapshot_ids") if isinstance(manifest.get("input_market_data_snapshot_ids"), list) else (market.get("input_market_data_snapshot_ids") if isinstance(market.get("input_market_data_snapshot_ids"), list) else []),
            "market_data_snapshot_id": str(market.get("market_data_snapshot_id") or ""),
            "final_eod_certification_status": str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or ""),
            "final_eod_certification_pending": bool(market.get("final_eod_certification_pending") is True or inputs.get("final_eod_certification_pending") is True),
            "provisional_intraday_symbols": market.get("provisional_intraday_symbols") if isinstance(market.get("provisional_intraday_symbols"), list) else [],
            "scheduler_failure": scheduler_failure,
            "retry_action_available": True,
            "retry_action_endpoint": "/api/aegis/data-remediation/run",
            "retry_action_mode": "IDEMPOTENT_BACKGROUND_JOB",
            "retry_job": retry_job,
            "retry_action_playbook_id": "refresh_required_symbol_data",
            "candidate_count": candidate_count,
            "current_intraday_candidate_count": current_intraday_candidate_count,
            "final_eod_certified_candidate_count": 0 if str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or "").upper() != "VALID" else current_intraday_candidate_count,
            "blocked_candidate_count": blocked_count,
            "selected_candidate_count": selected_candidate_count,
            "capture_ready_ticket_count": capture_ready_count,
            "blocked_selected_candidate_count": blocked_selected_count,
            "status_counts": status_counts,
            **candidate_semantic_counts,
            "candidate_pipeline_observability": candidate_pipeline_observability,
            "candidate_pipeline_alerts": candidate_pipeline_observability.get("alerts", []),
            "intent_lifecycle_summary": intent_lifecycle_summary,
            "candidate_intent_plane": intent_plane,
            "candidate_rows": operator_candidate_rows,
            "source_artifacts": refs,
        })
    if candidate_count and blocked_count == candidate_count:
        return _with_operator_state_semantics({
            "status": "CURRENT_DAY_BLOCKED",
            "runtime_mode": "CURRENT_DAY_BLOCKED",
            "failed_step": "sleeve_evaluation",
            "blocker": str(arbitration.get("canonical_blocker") or pointer.get("canonical_blocker") or "CANDIDATES_BLOCKED"),
            "message": "Today’s candidates are blocked.",
            "last_attempt_time": str(manifest.get("produced_at_utc") or arbitration.get("created_at_utc") or ""),
            "next_action": str(arbitration.get("operator_next_action") or "Resolve candidate-generation blockers or accept no-trade day."),
            "missing_symbols": missing_symbols,
            "critical_missing_symbols": critical_symbols or missing_symbols,
            "missing_input_ids": missing_input_ids,
            "provider_results": provider_results,
            "market_data_last_updated_at": market_data_last_updated_at,
            "candidate_snapshot_generated_at": candidate_snapshot_generated_at,
            "last_certification_attempt_at": last_certification_attempt_at,
            "final_eod_certification_completed_at": final_eod_certification_completed_at,
            "eod_certification_timing": eod_certification_timing,
            "scheduler_failure": scheduler_failure,
            "intraday_operational_ready": bool(market.get("intraday_operational_ready") is True),
            "market_data_mode": str(market.get("market_data_mode") or inputs.get("market_data_mode") or ""),
            "final_eod_certification_status": str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or ""),
            "final_eod_certification_pending": bool(market.get("final_eod_certification_pending") is True or inputs.get("final_eod_certification_pending") is True),
            "candidate_count": candidate_count,
            "current_intraday_candidate_count": current_intraday_candidate_count,
            "final_eod_certified_candidate_count": 0 if str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or "").upper() != "VALID" else current_intraday_candidate_count,
            "blocked_candidate_count": blocked_count,
            "selected_candidate_count": selected_candidate_count,
            "capture_ready_ticket_count": capture_ready_count,
            "blocked_selected_candidate_count": blocked_selected_count,
            "status_counts": status_counts,
            **candidate_semantic_counts,
            "candidate_pipeline_observability": candidate_pipeline_observability,
            "candidate_pipeline_alerts": candidate_pipeline_observability.get("alerts", []),
            "intent_lifecycle_summary": intent_lifecycle_summary,
            "candidate_intent_plane": intent_plane,
            "candidate_rows": operator_candidate_rows,
            "source_artifacts": refs,
        })
    if any(ref.get("exists") for ref in refs):
        return _with_operator_state_semantics({
            "status": "HISTORICAL_ONLY",
            "runtime_mode": "HISTORICAL_FALLBACK",
            "failed_step": "portfolio_gate_candidate_report",
            "blocker": "NO_CURRENT_DAY_SELECTED_CANDIDATE",
            "message": "Today’s intraday market data is ready; no current-day selected candidate is available." if operator_market_state == "INTRADAY_OPERATIONAL_READY" else "No current-day candidate is available.",
            "last_attempt_time": str(manifest.get("produced_at_utc") or arbitration.get("created_at_utc") or market.get("generated_at_utc") or ""),
            "next_action": "Review current-day artifacts; historical results are stale fallback only.",
            "missing_symbols": missing_symbols,
            "critical_missing_symbols": critical_symbols or missing_symbols,
            "missing_input_ids": missing_input_ids,
            "provider_results": provider_results,
            "market_data_last_updated_at": market_data_last_updated_at,
            "candidate_snapshot_generated_at": candidate_snapshot_generated_at,
            "last_certification_attempt_at": last_certification_attempt_at,
            "final_eod_certification_completed_at": final_eod_certification_completed_at,
            "eod_certification_timing": eod_certification_timing,
            "scheduler_failure": scheduler_failure,
            "intraday_operational_ready": bool(market.get("intraday_operational_ready") is True),
            "market_data_mode": str(market.get("market_data_mode") or inputs.get("market_data_mode") or ""),
            "final_eod_certification_status": str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or ""),
            "final_eod_certification_pending": bool(market.get("final_eod_certification_pending") is True or inputs.get("final_eod_certification_pending") is True),
            "candidate_count": candidate_count,
            "current_intraday_candidate_count": current_intraday_candidate_count,
            "final_eod_certified_candidate_count": 0 if str(market.get("final_eod_certification_status") or inputs.get("final_eod_certification_status") or "").upper() != "VALID" else current_intraday_candidate_count,
            "blocked_candidate_count": blocked_count,
            "selected_candidate_count": selected_candidate_count,
            "capture_ready_ticket_count": capture_ready_count,
            "blocked_selected_candidate_count": blocked_selected_count,
            "status_counts": status_counts,
            **candidate_semantic_counts,
            "candidate_pipeline_observability": candidate_pipeline_observability,
            "candidate_pipeline_alerts": candidate_pipeline_observability.get("alerts", []),
            "intent_lifecycle_summary": intent_lifecycle_summary,
            "candidate_intent_plane": intent_plane,
            "candidate_rows": operator_candidate_rows,
            "source_artifacts": refs,
        })
    return _with_operator_state_semantics({
        "status": "HISTORICAL_ONLY",
        "runtime_mode": "HISTORICAL_FALLBACK",
        "failed_step": "current_day_artifact_discovery",
        "blocker": "CURRENT_DAY_ARTIFACTS_MISSING",
        "message": "No current-day candidate is available.",
        "last_attempt_time": "",
        "next_action": "Run current-day market data and sleeve evaluation before using historical results.",
        "missing_symbols": [],
        "critical_missing_symbols": [],
        "missing_input_ids": [],
        "provider_results": [],
        "market_data_last_updated_at": "",
        "candidate_snapshot_generated_at": "",
        "last_certification_attempt_at": "",
        "final_eod_certification_completed_at": "",
        "eod_certification_timing": {"market_close_at": "", "vendor_lag_window": "", "certification_pending": True, "estimated_next_certification_attempt_at": "", "final_certification_status": ""},
        "scheduler_failure": _scheduler_failure_summary(root, day_utc),
        "candidate_count": 0,
        "current_intraday_candidate_count": 0,
        "final_eod_certified_candidate_count": 0,
        "blocked_candidate_count": 0,
        "status_counts": {},
        "analytical_state_counts": {},
        "operator_task_state_counts": {},
        "operator_affordance_counts": {},
        "execution_state_counts": {},
        "user_task_available_count": 0,
        "operator_action_available_count": 0,
        "manual_ib_capture_ready_count": 0,
        "manual_capture_available_count": 0,
        "system_repair_required_count": 0,
        "execution_eligible_row_count": 0,
        "execution_locked_non_certified_count": 0,
        "candidate_pipeline_observability": {
            "schema_id": "candidate_pipeline_observability",
            "schema_version": "v1",
            "day_utc": day_utc,
            "metrics": _candidate_counts([]),
            "by_sleeve": {},
            "daily": [],
            "rolling_windows": {"5d": {"trading_days": [], "metrics": _candidate_counts([]), "by_sleeve": {}}, "20d": {"trading_days": [], "metrics": _candidate_counts([]), "by_sleeve": {}}},
            "suppression_diagnostics": _candidate_diagnostics([]),
            "alerts": [],
            "regime_activity_level": "NO_CURRENT_CANDIDATE_ROWS",
            "capture_ready_trend": [],
        },
        "candidate_pipeline_alerts": [],
        "candidate_rows": [],
        "selected_candidate_count": 0,
        "capture_ready_ticket_count": 0,
        "blocked_selected_candidate_count": 0,
        "source_artifacts": refs,
    })


def _selected_row_from_gate(report: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    selected_id = str(report.get("selected_candidate_id") or "").strip()
    rows = report.get("candidate_rows") if isinstance(report.get("candidate_rows"), list) else []
    selected_row: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("candidate_id") or row.get("intent_candidate_id") or "").strip()
        selected_by_gate = str(row.get("selected_by_gate") or "").upper() == "YES" or str(row.get("portfolio_gate_decision") or "").upper() == "ALLOW"
        if (selected_id and row_id == selected_id) or (not selected_id and selected_by_gate):
            return row_id, row
    return selected_id, selected_row


def _first_evidence_path(row: dict[str, Any]) -> str:
    paths = row.get("evidence_paths") if isinstance(row.get("evidence_paths"), list) else []
    for path in paths:
        text = str(path or "").strip()
        if text:
            return text
    return str(row.get("intent_path") or row.get("raw_intent_path") or "").strip()


def _conversion_paths(root: Path, day_utc: str) -> list[Path]:
    base = root / "reports" / "exposure_intent_paper_submission_package_v1" / day_utc
    if not base.exists() or not base.is_dir():
        return []
    return sorted(base.glob("*/exposure_intent_paper_submission_package.v1.json"), key=lambda p: (p.stat().st_mtime_ns, str(p)), reverse=True)


def _matching_conversion(root: Path, day_utc: str, selected_id: str, errors: list[dict[str, Any]]) -> tuple[Path | None, dict[str, Any], str]:
    paths = _conversion_paths(root, day_utc)
    latest_mismatch_id = ""
    latest_mismatch_path: Path | None = None
    for path in paths:
        local_errors: list[dict[str, Any]] = []
        payload = _read_json(path, local_errors, "exposure_intent_conversion")
        if not payload:
            errors.extend(local_errors)
            continue
        exposure_id = str(payload.get("exposure_intent_id") or payload.get("selected_exposure_intent_id") or "").strip()
        if exposure_id == selected_id:
            return path.resolve(), payload, ""
        if latest_mismatch_path is None:
            latest_mismatch_path = path.resolve()
            latest_mismatch_id = exposure_id
    if latest_mismatch_path is not None:
        warning = f"Selected exposure {selected_id} does not match latest converter artifact {latest_mismatch_id or 'unknown'}; conversion/blocker fields are unavailable for the current selected exposure."
        errors.append({"code": "CURRENT_TRUTH_CONVERSION_MISMATCH", "message": warning, "source_path": str(latest_mismatch_path), "recoverable": True})
        return latest_mismatch_path, {}, warning
    errors.append({"code": "CURRENT_TRUTH_CONVERSION_MISSING", "message": f"No conversion artifact found for selected exposure {selected_id} on {day_utc}.", "source_path": str(root / "reports" / "exposure_intent_paper_submission_package_v1" / day_utc), "recoverable": True})
    return None, {}, ""


def _has_current_selected_pointer(root: Path, day_utc: str | None = None) -> bool:
    path = selected_pointer_path_v1(root)
    if not path.exists() or not path.is_file():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    if day_utc and str(payload.get("day_utc") or "").strip() != str(day_utc):
        return False
    selected = payload.get("selected_intent") if isinstance(payload.get("selected_intent"), dict) else {}
    arbitration_text = str(payload.get("source_arbitration_path") or "").strip()
    arbitration_path = Path(arbitration_text) if arbitration_text else None
    return bool(str(selected.get("intent_id") or "").strip() and arbitration_path is not None and arbitration_path.exists())


def _direction_from_intent(intent: dict[str, Any], selected: dict[str, Any]) -> str:
    explicit = str(selected.get("direction") or selected.get("proposed_direction") or intent.get("direction") or intent.get("proposed_direction") or "").strip().upper()
    if explicit:
        return explicit
    exposure_type = str(intent.get("exposure_type") or "").strip().upper()
    if "SHORT" in exposure_type:
        return "SHORT"
    if exposure_type:
        return "LONG"
    return ""


def _resolve_root(root: Path, day_utc: str | None = None) -> Path:
    root = root.expanduser().resolve()
    if day_utc and _has_current_selected_pointer(root, day_utc):
        return root
    if _gate_report_paths(root, day_utc):
        return root
    if str(root).startswith("/tmp/"):
        return root
    if ALT_RUNTIME_TRUTH_ROOT.exists() and _gate_report_paths(ALT_RUNTIME_TRUTH_ROOT.resolve(), day_utc):
        return ALT_RUNTIME_TRUTH_ROOT.resolve()
    return root


def resolve_current_operator_truth_v1(*, truth_root: Path | str, day_utc: str | None = None, generated_at_utc: str | None = None) -> dict[str, Any]:
    day_utc = str(day_utc or datetime.now(UTC).date().isoformat())
    root = _resolve_root(Path(truth_root), day_utc)
    errors: list[dict[str, Any]] = []
    generated = generated_at_utc or _now_iso()
    gate_path: Path | None = None
    gate_report: dict[str, Any] = {}
    selected_id = ""
    selected_row: dict[str, Any] = {}

    pointer_path = selected_pointer_path_v1(root)
    pointer = _read_json(pointer_path, errors, "selected_intent_pointer") if pointer_path.exists() else {}
    pointer_selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    pointer_day = str(pointer.get("day_utc") or "").strip()
    pointer_selected_id = str(pointer_selected.get("intent_id") or "").strip()
    pointer_arbitration_text = str(pointer.get("source_arbitration_path") or "").strip()
    pointer_arbitration_path = Path(pointer_arbitration_text) if pointer_arbitration_text else None
    if pointer_selected_id and pointer_day == day_utc and pointer_arbitration_path is not None and pointer_arbitration_path.exists():
        source_day = pointer_day or day_utc
        intent_path = Path(str(pointer_selected.get("intent_path") or "")).expanduser()
        intent = _read_json(intent_path, errors, "selected_exposure_intent") if str(intent_path) else {}
        source_run_id = str(pointer.get("source_arbitration_path") or "").strip()
        source_run_id = _sha256_file(Path(source_run_id)) if source_run_id and Path(source_run_id).exists() else (_sha256_file(pointer_path) if pointer_path.exists() else "")
        conversion_path, conversion, mismatch_warning = _matching_conversion(root, source_day, pointer_selected_id, errors)
        market = conversion.get("market_data_status") if isinstance(conversion.get("market_data_status"), dict) else {}
        blocker = str(conversion.get("blocker_code") or "").strip()
        selected = {
            "candidate_id": pointer_selected_id,
            "selected_exposure_intent_id": pointer_selected_id,
            "symbol": str(pointer_selected.get("symbol") or ((intent.get("underlying") or {}) if isinstance(intent.get("underlying"), dict) else {}).get("symbol") or "").upper(),
            "sleeve_id": str(pointer_selected.get("sleeve_id") or pointer_selected.get("engine_id") or ((intent.get("engine") or {}) if isinstance(intent.get("engine"), dict) else {}).get("engine_id") or ""),
            "engine_id": str(pointer_selected.get("engine_id") or pointer_selected.get("sleeve_id") or ((intent.get("engine") or {}) if isinstance(intent.get("engine"), dict) else {}).get("engine_id") or ""),
            "direction": _direction_from_intent(intent, pointer_selected),
            "score": pointer_selected.get("portfolio_score_total"),
            "confidence": pointer_selected.get("confidence") or "UNKNOWN",
            "selected_by_arbitration": True,
            "selected_reason": str(pointer_selected.get("selection_reason") or pointer_selected.get("arbitration_reason") or "SELECTED_INTENT_POINTER"),
            "source_artifact_path": str(intent_path) if str(intent_path) else str(pointer_selected.get("intent_path") or ""),
        }
        suppressed_count = 0
        arbitration_path = Path(str(pointer.get("source_arbitration_path") or ""))
        arbitration = _read_json(arbitration_path, errors, "intent_arbitration") if str(arbitration_path) and arbitration_path.exists() else {}
        rejected = arbitration.get("rejected_or_filtered_intents") if isinstance(arbitration.get("rejected_or_filtered_intents"), list) else []
        suppressed_count = len(rejected)
        current_day_status = _current_day_status(root, source_day, errors)
        return {
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "ok": True,
            "degraded": bool(errors or mismatch_warning),
            "current_truth_status": CURRENT_OK,
            "runtime_mode": "INTRADAY_OPERATIONAL",
            "status_reason": "Current selected exposure resolved from selected intent pointer.",
            "generated_at_utc": generated,
            "truth_root": str(root),
            "source_day": source_day,
            "source_run_id": source_run_id,
            "selected_exposure": selected,
            "selected_exposure_intent_id": pointer_selected_id,
            "selected_candidate_row": pointer_selected,
            "current_day_status": current_day_status,
            "market_data_state": str(current_day_status.get("market_data_state") or ""),
            "candidate_certification_state": str(current_day_status.get("candidate_certification_state") or ""),
            "execution_eligibility_state": str(current_day_status.get("execution_eligibility_state") or ""),
            "operator_state_semantics": current_day_status.get("operator_state_semantics") if isinstance(current_day_status.get("operator_state_semantics"), dict) else {},
            "displayed_artifact_day": source_day,
            "portfolio_gate_report": {},
            "portfolio_gate_report_path": str(pointer.get("portfolio_activation_gate_path") or ""),
            "suppressed_count": suppressed_count,
            "suppression_code_counts": {},
            "conversion": conversion,
            "conversion_path": str(conversion_path or ""),
            "conversion_status": str(conversion.get("status") or ("UNAVAILABLE" if mismatch_warning else "")),
            "conversion_blocker": blocker,
            "blocker_code": blocker,
            "blocker_message": str(conversion.get("blocker_message") or ""),
            "paper_intent_created": bool(conversion.get("paper_trade_intent_created") is True),
            "latest_market_session": str(market.get("observed_session") or ""),
            "required_market_session": str(market.get("expected_session") or source_day),
            "stale_market_data": str(market.get("status") or "").upper() in {"STALE_OR_MISSING", "STALE_OR_INVALID"} or blocker == "STALE_MARKET_DATA_BLOCKS_CONVERSION",
            "source_mismatch_warning": mismatch_warning,
            "source_consistency": {
                "valid": True,
                "reasons": [],
                "converter_matches_selected_candidate": bool(conversion) or not conversion_path,
                "selected_candidate_id": pointer_selected_id,
            },
            "source_artifacts": [
                _source_ref(pointer_path, "selected_intent_pointer", pointer),
                _source_ref(arbitration_path if str(arbitration_path) else None, "intent_arbitration", arbitration),
                _source_ref(Path(str(selected.get("source_artifact_path") or "")), "selected_exposure_intent", intent),
                _source_ref(conversion_path, "exposure_intent_conversion", conversion),
                _source_ref(market_manifest_path_v1(root), "market_data_manifest"),
                _source_ref(research_status_path_v1(root, source_day), "research_lab_summary"),
            ],
            "errors": errors,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "order_routing_allowed": False,
            "capital_allocation_allowed": False,
        }

    for path in _gate_report_paths(root, day_utc):
        candidate = _read_json(path, errors, "portfolio_gate_candidate_report")
        candidate_id, row = _selected_row_from_gate(candidate)
        if candidate and candidate_id and row:
            gate_path = path.resolve()
            gate_report = candidate
            selected_id = candidate_id
            selected_row = row
            break

    if gate_path is None or not selected_id:
        status_day = day_utc
        current_day_status = _current_day_status(root, status_day, errors) if status_day else {}
        explicit_status = str(current_day_status.get("status") or "HISTORICAL_ONLY")
        historical_fallback = _latest_historical_gate_summary(root, status_day) if status_day else {}
        source_artifacts = current_day_status.get("source_artifacts") if isinstance(current_day_status.get("source_artifacts"), list) else []
        reason = str(current_day_status.get("message") or "No current portfolio gate report with a valid selected candidate was found.")
        consistency_reasons = ["missing_current_portfolio_gate_report"]
        if explicit_status:
            consistency_reasons.append(explicit_status.lower())
        current_day_display_states = {"INTRADAY_OPERATIONAL_READY", VALIDATED_CURRENT_DAY, PROVISIONAL_INTRADAY, CERTIFICATION_PENDING, CERTIFIED, "CURRENT_DAY_BLOCKED", "FINAL_EOD_READY"}
        displayed_artifact_day = status_day if explicit_status in current_day_display_states else str(historical_fallback.get("source_day") or "")
        return {
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "ok": True,
            "degraded": True,
            "current_truth_status": explicit_status,
            "runtime_mode": _runtime_mode_from_status(explicit_status),
            "status_reason": reason,
            "generated_at_utc": generated,
            "truth_root": str(root),
            "requested_day": status_day,
            "source_day": status_day,
            "displayed_artifact_day": displayed_artifact_day,
            "historical_fallback": historical_fallback,
            "current_day_status": current_day_status,
            "market_data_state": str(current_day_status.get("market_data_state") or ""),
            "candidate_certification_state": str(current_day_status.get("candidate_certification_state") or ""),
            "execution_eligibility_state": str(current_day_status.get("execution_eligibility_state") or ""),
            "operator_state_semantics": current_day_status.get("operator_state_semantics") if isinstance(current_day_status.get("operator_state_semantics"), dict) else {},
            "source_run_id": "",
            "selected_exposure": {},
            "selected_exposure_intent_id": "",
            "suppressed_count": 0,
            "conversion_blocker": "",
            "conversion_status": "",
            "source_consistency": {"valid": False, "reasons": consistency_reasons},
            "source_artifacts": source_artifacts,
            "errors": errors,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "order_routing_allowed": False,
            "capital_allocation_allowed": False,
        }

    source_day = str(gate_report.get("day_utc") or _day_from_gate_path(gate_path)).strip()
    source_run_id = str(gate_report.get("determinism_fingerprint") or gate_report.get("portfolio_gate_candidate_report_id") or gate_report.get("report_id") or _sha256_file(gate_path)).strip()
    consistency_reasons: list[str] = []
    if not source_day:
        consistency_reasons.append("source_day_unknown")
    if not source_run_id:
        consistency_reasons.append("source_run_id_unknown")
    if not gate_path.exists():
        consistency_reasons.append("portfolio_gate_report_missing")
    if source_day and day_utc != source_day:
        consistency_reasons.append("requested_day_does_not_match_gate_report_day")

    conversion_path, conversion, mismatch_warning = _matching_conversion(root, source_day, selected_id, errors)
    market = conversion.get("market_data_status") if isinstance(conversion.get("market_data_status"), dict) else {}
    blocker = str(conversion.get("blocker_code") or "").strip()
    selected = {
        "candidate_id": selected_id,
        "selected_exposure_intent_id": selected_id,
        "symbol": str(selected_row.get("symbol") or "").upper(),
        "sleeve_id": str(selected_row.get("sleeve_id") or selected_row.get("engine_id") or ""),
        "engine_id": str(selected_row.get("engine_id") or selected_row.get("sleeve_id") or ""),
        "direction": str(selected_row.get("proposed_direction") or selected_row.get("direction") or ""),
        "score": selected_row.get("score"),
        "confidence": selected_row.get("confidence"),
        "selected_by_arbitration": True,
        "source_artifact_path": _first_evidence_path(selected_row),
    }
    suppressed_count = int(gate_report.get("suppressed_count") or len([row for row in gate_report.get("candidate_rows", []) if isinstance(row, dict) and str(row.get("selected_by_gate") or "").upper() != "YES"]))
    status = CURRENT_OK if not consistency_reasons else "UNAVAILABLE"
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "ok": True,
        "degraded": bool(errors or consistency_reasons or mismatch_warning),
        "current_truth_status": status,
        "runtime_mode": "INTRADAY_OPERATIONAL" if status == CURRENT_OK else "CURRENT_DAY_FAILED",
        "status_reason": "Current selected exposure resolved from portfolio gate report." if status == CURRENT_OK else "Current selected exposure failed source consistency checks.",
        "generated_at_utc": generated,
        "truth_root": str(root),
        "source_day": source_day,
        "source_run_id": source_run_id,
        "selected_exposure": selected if status == CURRENT_OK else {},
        "selected_exposure_intent_id": selected_id if status == CURRENT_OK else "",
        "selected_candidate_row": selected_row if status == CURRENT_OK else {},
        "portfolio_gate_report": gate_report if status == CURRENT_OK else {},
        "portfolio_gate_report_path": str(gate_path),
        "suppressed_count": suppressed_count if status == CURRENT_OK else 0,
        "suppression_code_counts": gate_report.get("suppression_code_counts") if isinstance(gate_report.get("suppression_code_counts"), dict) else {},
        "conversion": conversion,
        "conversion_path": str(conversion_path or ""),
        "conversion_status": str(conversion.get("status") or ("UNAVAILABLE" if mismatch_warning else "")),
        "conversion_blocker": blocker,
        "blocker_code": blocker,
        "blocker_message": str(conversion.get("blocker_message") or ""),
        "paper_intent_created": bool(conversion.get("paper_trade_intent_created") is True),
        "latest_market_session": str(market.get("observed_session") or ""),
        "required_market_session": str(market.get("expected_session") or source_day),
        "stale_market_data": str(market.get("status") or "").upper() in {"STALE_OR_MISSING", "STALE_OR_INVALID"} or blocker == "STALE_MARKET_DATA_BLOCKS_CONVERSION",
        "source_mismatch_warning": mismatch_warning,
        "source_consistency": {
            "valid": status == CURRENT_OK,
            "reasons": consistency_reasons,
            "converter_matches_selected_candidate": bool(conversion) or not conversion_path,
            "selected_candidate_id": selected_id,
        },
        "source_artifacts": [
            _source_ref(gate_path, "portfolio_gate_candidate_report", gate_report),
            _source_ref(conversion_path, "exposure_intent_conversion", conversion),
            _source_ref(regime_bucket_ranking_path_v1(root, source_day), "regime_bucket_candidate_ranking"),
            _source_ref(candidate_diagnostics_path_v1(root, source_day), "paper_intent_candidate_diagnostics"),
            _source_ref(selected_pointer_path_v1(root), "selected_intent_pointer"),
            _source_ref(market_manifest_path_v1(root), "market_data_manifest"),
            _source_ref(research_status_path_v1(root, source_day), "research_lab_summary"),
        ],
        "errors": errors,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
    }


def api_envelope_v1(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "degraded": bool(payload.get("degraded")),
        "current_truth_status": payload.get("current_truth_status"),
        "data": payload,
        "errors": payload.get("errors") if isinstance(payload.get("errors"), list) else [],
        "next_action": "Use current operator truth." if payload.get("current_truth_status") in {CURRENT_OK, LEGACY_CURRENT} else "Current operator truth unavailable; inspect resolver errors.",
    }
