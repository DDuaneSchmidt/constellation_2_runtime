from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.paper_session_ledger_v1 import resolve_scheduled_paper_session_v1

REPORT_FAMILY = "aegis_duplicate_candidate_v1"
REPORT_FILENAME = "duplicate_candidate.v1.json"
SCHEMA_ID = "aegis_duplicate_candidate"
SCHEMA_VERSION = "v1"

NEW_DISTINCT_SETUP = "NEW_DISTINCT_SETUP"
DUPLICATE_SAME_SESSION = "DUPLICATE_SAME_SESSION"
DUPLICATE_SAME_DAY = "DUPLICATE_SAME_DAY"
DUPLICATE_OPEN_POSITION = "DUPLICATE_OPEN_POSITION"
DUPLICATE_RECENT_CAPTURE = "DUPLICATE_RECENT_CAPTURE"
DUPLICATE_RECENT_REJECTION = "DUPLICATE_RECENT_REJECTION"
DUPLICATE_RECENT_DEFERRED = "DUPLICATE_RECENT_DEFERRED"
DUPLICATE_RECENTLY_CLOSED = "DUPLICATE_RECENTLY_CLOSED"
IMPROVED_SIGNAL = "IMPROVED_SIGNAL"
IMPROVED_SIGNAL_ADD_ON = "IMPROVED_SIGNAL_ADD_ON"
SIGNAL_REFRESH_NO_ACTION = "SIGNAL_REFRESH_NO_ACTION"
COOLDOWN_EXPIRED_REVIEW_ALLOWED = "COOLDOWN_EXPIRED_REVIEW_ALLOWED"

REVIEWABLE_CLASSIFICATIONS = {NEW_DISTINCT_SETUP, IMPROVED_SIGNAL, IMPROVED_SIGNAL_ADD_ON, COOLDOWN_EXPIRED_REVIEW_ALLOWED}
SUPPRESSED_CLASSIFICATIONS = {
    DUPLICATE_SAME_SESSION,
    DUPLICATE_SAME_DAY,
    DUPLICATE_OPEN_POSITION,
    DUPLICATE_RECENT_CAPTURE,
    DUPLICATE_RECENT_REJECTION,
    DUPLICATE_RECENT_DEFERRED,
    DUPLICATE_RECENTLY_CLOSED,
    SIGNAL_REFRESH_NO_ACTION,
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def duplicate_candidate_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows(payload: Mapping[str, Any], *keys: str) -> list[Mapping[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
    return []


def _text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _symbol(row: Mapping[str, Any]) -> str:
    return _text(row.get("symbol"), row.get("ticker"), row.get("symbol_or_pair")).upper()


def _direction(row: Mapping[str, Any]) -> str:
    return _text(row.get("direction"), row.get("side"), "LONG").upper()


def _candidate_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_id"), row.get("candidate_contract_id"), row.get("trade_candidate_id"), row.get("id"), row.get("intent_id"))


def _contract_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_contract_id"), row.get("candidate_id"), row.get("contract_id"))


def _setup_type(row: Mapping[str, Any]) -> str:
    return _text(row.get("setup_type"), row.get("strategy"), row.get("sleeve_id"), row.get("sleeve"), row.get("sleeve_name")).upper()


def _duplicate_key(row: Mapping[str, Any], *, preferred: bool = True) -> str:
    base = [_symbol(row), _direction(row)]
    if preferred:
        setup = _setup_type(row)
        if setup:
            base.append(setup)
    return "|".join(item for item in base if item)


def _float(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        try:
            if value is not None and str(value).strip() != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _entry(row: Mapping[str, Any]) -> float | None:
    return _float(row, "entry_price", "planned_entry", "entry_reference_price")


def _reward_risk(row: Mapping[str, Any]) -> float | None:
    return _float(row, "reward_risk_ratio", "reward_to_risk", "rr")


def _score(row: Mapping[str, Any]) -> float | None:
    return _float(row, "score", "signal_score", "rank_score", "confidence_score")


def _percent_improvement(new: float | None, old: float | None, *, lower_is_better: bool = False) -> float:
    if new is None or old is None or old == 0:
        return 0.0
    delta = (old - new) if lower_is_better else (new - old)
    return round((delta / abs(old)) * 100.0, 4)


def _material_improvement(candidate: Mapping[str, Any], prior: Mapping[str, Any], thresholds: Mapping[str, Any]) -> tuple[bool, float, list[str]]:
    reasons: list[str] = []
    score_improvement = _percent_improvement(_score(candidate), _score(prior))
    rr_improvement = _percent_improvement(_reward_risk(candidate), _reward_risk(prior))
    entry_improvement = _percent_improvement(_entry(candidate), _entry(prior), lower_is_better=True)
    if score_improvement >= float(thresholds.get("score_improvement_threshold_percent", 10)):
        reasons.append(f"score improved {score_improvement}%")
    if rr_improvement >= float(thresholds.get("reward_risk_improvement_threshold_percent", 10)):
        reasons.append(f"reward/risk improved {rr_improvement}%")
    if entry_improvement >= float(thresholds.get("entry_improvement_threshold_percent", 2)):
        reasons.append(f"entry improved {entry_improvement}%")
    improvement_score = max(score_improvement, rr_improvement, entry_improvement, 0.0)
    return bool(reasons), improvement_score, reasons


def _age_days(day_utc: str, prior_day: str) -> int:
    try:
        day = datetime.fromisoformat(day_utc).date()
        prior = datetime.fromisoformat(prior_day).date()
        return max(0, (day - prior).days)
    except Exception:
        return 0


def _index_by_keys(rows: list[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    index: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        for preferred in (True, False):
            key = _duplicate_key(row, preferred=preferred)
            if key:
                index.setdefault(key, []).append(row)
    return index


def _matches(index: Mapping[str, list[Mapping[str, Any]]], row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    seen: set[str] = set()
    out: list[Mapping[str, Any]] = []
    for preferred in (True, False):
        for match in index.get(_duplicate_key(row, preferred=preferred), []):
            marker = _text(_candidate_id(match), str(id(match)))
            if marker in seen:
                continue
            seen.add(marker)
            out.append(match)
    return out


def _output_candidates(lifecycle: Mapping[str, Any], signal_boundary: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    current = _rows(lifecycle, "current_session_candidates")
    present = [row for row in _rows(signal_boundary, "boundary_rows") if str(row.get("boundary_status") or "").upper() == "SIGNAL_EVIDENCE_PRESENT"]
    if not present:
        return []
    by_candidate = {_candidate_id(row): row for row in current if _candidate_id(row)}
    by_contract = {_contract_id(row): row for row in current if _contract_id(row)}
    by_symbol = {_symbol(row): row for row in current if _symbol(row)}
    out: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for boundary in present:
        row = by_candidate.get(_candidate_id(boundary)) or by_contract.get(_contract_id(boundary)) or by_symbol.get(_symbol(boundary))
        if not row:
            continue
        cid = _candidate_id(row)
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        out.append(row)
    return out


def _operator_message(classification: str) -> str:
    return {
        NEW_DISTINCT_SETUP: "New distinct setup available for operator review.",
        DUPLICATE_SAME_SESSION: "Repeated signal suppressed — duplicate within the same paper session.",
        DUPLICATE_SAME_DAY: "Repeated signal suppressed — no material improvement detected today.",
        DUPLICATE_OPEN_POSITION: "Duplicate of open position — view existing position.",
        DUPLICATE_RECENT_CAPTURE: "Recently captured — cooldown active.",
        DUPLICATE_RECENT_REJECTION: "Recently rejected — cooldown active.",
        DUPLICATE_RECENT_DEFERRED: "Recently deferred — review the deferred candidate in diagnostics.",
        DUPLICATE_RECENTLY_CLOSED: "Recently closed — cooldown active.",
        IMPROVED_SIGNAL: "Improved signal detected — review candidate.",
        IMPROVED_SIGNAL_ADD_ON: "Improved signal detected — review add-on candidate.",
        SIGNAL_REFRESH_NO_ACTION: "Signal refresh only — no material change requiring action.",
        COOLDOWN_EXPIRED_REVIEW_ALLOWED: "Cooldown expired — candidate may be reviewed again.",
    }.get(classification, "Duplicate classification requires diagnostics.")


def _allowed_blocked(classification: str) -> tuple[list[str], list[str]]:
    if classification == IMPROVED_SIGNAL_ADD_ON:
        return ["REVIEW_ADD_ON", "VIEW_EXISTING_POSITION", "REJECT_ADD_ON", "DEFER_ADD_ON", "DETAILS"], ["CONFIRM_CAPTURED"]
    if classification in {NEW_DISTINCT_SETUP, IMPROVED_SIGNAL, COOLDOWN_EXPIRED_REVIEW_ALLOWED}:
        return ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER", "DETAILS"], []
    if classification == DUPLICATE_OPEN_POSITION:
        return ["VIEW_EXISTING_POSITION", "DETAILS"], ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"]
    return ["DETAILS"], ["CONFIRM_CAPTURED", "MARK_NOT_CAPTURED", "DEFER"]


def build_duplicate_candidate_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None, thresholds: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated = generated_at_utc or _now_iso()
    thresholds = dict(thresholds or {})
    thresholds.setdefault("score_improvement_threshold_percent", 10)
    thresholds.setdefault("reward_risk_improvement_threshold_percent", 10)
    thresholds.setdefault("entry_improvement_threshold_percent", 2)
    thresholds.setdefault("cooldown_days_default", 5)

    lifecycle_path = root / "reports" / "aegis_candidate_lifecycle_projection_v1" / day / "candidate_lifecycle_projection.v1.json"
    signal_boundary_path = root / "reports" / "aegis_signal_evidence_boundary_v1" / day / "signal_evidence_boundary.v1.json"
    queue_path = root / "reports" / "aegis_paper_review_queue_v1" / day / "paper_review_queue.v1.json"
    decision_path = root / "reports" / "aegis_candidate_decision_ledger_v1" / day / "candidate_decision_ledger.v1.json"
    entry_path = root / "reports" / "aegis_paper_entry_receipts_v1" / day / "paper_entry_receipts.v1.json"
    exit_path = root / "reports" / "aegis_paper_exit_receipts_v1" / day / "paper_exit_receipts.v1.json"

    lifecycle = _read_json(lifecycle_path)
    signal_boundary = _read_json(signal_boundary_path)
    queue = _read_json(queue_path)
    decisions = _read_json(decision_path)
    entries = _read_json(entry_path)
    exits = _read_json(exit_path)
    official_session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)
    session_id = _text(lifecycle.get("paper_session_id"), signal_boundary.get("paper_session_id"), queue.get("paper_session_id"), official_session.get("paper_session_id"))

    output_rows = _output_candidates(lifecycle, signal_boundary)
    open_positions = [row for row in _rows(lifecycle, "open_paper_positions") if _symbol(row)]
    closed_positions = [row for row in _rows(lifecycle, "closed_paper_positions") if _symbol(row)]
    carry_rows = _rows(lifecycle, "carry_forward_context")
    queue_rows = _rows(queue, "rows")
    decision_rows = _rows(decisions, "events")
    entry_rows = _rows(entries, "receipts")
    exit_rows = _rows(exits, "receipts")

    open_index = _index_by_keys(open_positions)
    closed_index = _index_by_keys(closed_positions + exit_rows)
    prior_same_day = [
        row
        for row in queue_rows
        if _text(row.get("paper_session_id")) != session_id
        and _text(row.get("paper_session_id")).startswith(f"PAPER-{day}")
        and str(row.get("rollover_status") or "").upper() != "CARRIED_FORWARD"
    ]
    prior_same_day_index = _index_by_keys(prior_same_day)
    carry_index = _index_by_keys(carry_rows)
    entry_index = _index_by_keys(entry_rows)
    rejection_index = _index_by_keys([row for row in decision_rows if str(row.get("decision") or row.get("event_type") or "").upper().endswith("REJECTED") or str(row.get("decision") or "").upper() in {"REJECT", "REJECTED", "NOT_CAPTURED"}])
    deferred_index = _index_by_keys([row for row in decision_rows if "DEFER" in str(row.get("decision") or row.get("event_type") or "").upper()])

    same_session_seen: dict[str, Mapping[str, Any]] = {}
    rows_out: list[dict[str, Any]] = []
    for candidate in output_rows:
        key = _duplicate_key(candidate, preferred=True) or _duplicate_key(candidate, preferred=False)
        prior: Mapping[str, Any] = {}
        prior_state = ""
        prior_position_id = ""
        prior_candidate_id = ""
        cooldown_days_remaining = 0
        classification = NEW_DISTINCT_SETUP
        duplicate_scope = "NONE"
        improvement_score = 0.0
        improvement_reasons: list[str] = []

        open_match = next(iter(_matches(open_index, candidate)), {})
        same_session_match = same_session_seen.get(key, {}) if key else {}
        recent_capture = next(iter(_matches(entry_index, candidate)), {})
        same_day_match = next(iter(_matches(prior_same_day_index, candidate)), {})
        recent_rejection = next(iter(_matches(rejection_index, candidate)), {})
        recent_deferred = next(iter(_matches(deferred_index, candidate)), {})
        recently_closed = next(iter(_matches(closed_index, candidate)), {})
        carry_match = next(iter(_matches(carry_index, candidate)), {})

        if open_match:
            improved, improvement_score, improvement_reasons = _material_improvement(candidate, open_match, thresholds)
            prior = open_match
            prior_state = _text(open_match.get("candidate_lifecycle_state"), open_match.get("current_state"), "PAPER_POSITION_OPEN")
            prior_position_id = _text(open_match.get("position_id"), open_match.get("prior_position_id"))
            classification = IMPROVED_SIGNAL_ADD_ON if improved else DUPLICATE_OPEN_POSITION
            duplicate_scope = "OPEN_POSITION"
        elif same_session_match:
            prior = same_session_match
            prior_state = "CURRENT_SESSION"
            classification = DUPLICATE_SAME_SESSION
            duplicate_scope = "SAME_SESSION"
        elif recent_capture:
            improved, improvement_score, improvement_reasons = _material_improvement(candidate, recent_capture, thresholds)
            prior = recent_capture
            prior_state = "RECENT_CAPTURE"
            classification = IMPROVED_SIGNAL if improved else DUPLICATE_RECENT_CAPTURE
            duplicate_scope = "RECENT_CAPTURE"
            cooldown_days_remaining = int(thresholds["cooldown_days_default"])
        elif same_day_match:
            improved, improvement_score, improvement_reasons = _material_improvement(candidate, same_day_match, thresholds)
            prior = same_day_match
            prior_state = _text(same_day_match.get("status"), same_day_match.get("candidate_lifecycle_state"), "SAME_DAY")
            classification = IMPROVED_SIGNAL if improved else DUPLICATE_SAME_DAY
            duplicate_scope = "SAME_DAY"
        elif recent_rejection:
            improved, improvement_score, improvement_reasons = _material_improvement(candidate, recent_rejection, thresholds)
            prior = recent_rejection
            prior_state = "RECENT_REJECTION"
            classification = IMPROVED_SIGNAL if improved else DUPLICATE_RECENT_REJECTION
            duplicate_scope = "RECENT_REJECTION"
            cooldown_days_remaining = int(thresholds["cooldown_days_default"])
        elif recent_deferred:
            prior = recent_deferred
            prior_state = "RECENT_DEFERRED"
            classification = DUPLICATE_RECENT_DEFERRED
            duplicate_scope = "RECENT_DEFERRED"
        elif recently_closed:
            prior = recently_closed
            prior_day = _text(recently_closed.get("day_utc"), recently_closed.get("exit_timestamp_utc"))[:10]
            age = _age_days(day, prior_day) if prior_day else 0
            cooldown_days_remaining = max(0, int(thresholds["cooldown_days_default"]) - age)
            improved, improvement_score, improvement_reasons = _material_improvement(candidate, recently_closed, thresholds)
            if improved:
                classification = IMPROVED_SIGNAL
            elif cooldown_days_remaining <= 0:
                classification = COOLDOWN_EXPIRED_REVIEW_ALLOWED
            else:
                classification = DUPLICATE_RECENTLY_CLOSED
            prior_state = "RECENTLY_CLOSED"
            duplicate_scope = "RECENTLY_CLOSED"
        elif carry_match:
            prior = carry_match
            prior_state = _text(carry_match.get("candidate_lifecycle_state"), carry_match.get("status"), "CARRY_FORWARD")
            classification = SIGNAL_REFRESH_NO_ACTION
            duplicate_scope = "CARRY_FORWARD"

        if key and key not in same_session_seen:
            same_session_seen[key] = candidate
        prior_candidate_id = _candidate_id(prior)
        allowed, blocked = _allowed_blocked(classification)
        rows_out.append(
            {
                "symbol": _symbol(candidate),
                "direction": _direction(candidate),
                "candidate_id": _candidate_id(candidate),
                "candidate_contract_id": _contract_id(candidate) or _candidate_id(candidate),
                "paper_session_id": _text(candidate.get("paper_session_id"), session_id),
                "duplicate_key": key,
                "duplicate_scope": duplicate_scope,
                "duplicate_classification": classification,
                "reviewable": classification in REVIEWABLE_CLASSIFICATIONS,
                "suppressed": classification in SUPPRESSED_CLASSIFICATIONS,
                "prior_candidate_id": prior_candidate_id or None,
                "prior_position_id": prior_position_id or _text(prior.get("position_id")) or None,
                "prior_state": prior_state or None,
                "improvement_score": improvement_score,
                "improvement_reasons": improvement_reasons,
                "cooldown_days_remaining": cooldown_days_remaining,
                "allowed_actions": allowed,
                "blocked_actions": blocked,
                "operator_message": _operator_message(classification),
            }
        )

    counts: dict[str, int] = {name: 0 for name in [
        NEW_DISTINCT_SETUP,
        DUPLICATE_SAME_SESSION,
        DUPLICATE_SAME_DAY,
        DUPLICATE_OPEN_POSITION,
        DUPLICATE_RECENT_CAPTURE,
        DUPLICATE_RECENT_REJECTION,
        DUPLICATE_RECENT_DEFERRED,
        DUPLICATE_RECENTLY_CLOSED,
        IMPROVED_SIGNAL,
        IMPROVED_SIGNAL_ADD_ON,
        SIGNAL_REFRESH_NO_ACTION,
        COOLDOWN_EXPIRED_REVIEW_ALLOWED,
    ]}
    for row in rows_out:
        counts[str(row.get("duplicate_classification"))] = counts.get(str(row.get("duplicate_classification")), 0) + 1
    suppressed_count = sum(1 for row in rows_out if row.get("suppressed"))
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": generated,
        "paper_session_id": session_id,
        "status": "PASS",
        "thresholds": thresholds,
        "current_output_candidate_count": len(rows_out),
        "new_distinct_setup_count": counts.get(NEW_DISTINCT_SETUP, 0),
        "duplicate_open_position_count": counts.get(DUPLICATE_OPEN_POSITION, 0),
        "duplicate_recent_capture_count": counts.get(DUPLICATE_RECENT_CAPTURE, 0),
        "duplicate_recent_rejection_count": counts.get(DUPLICATE_RECENT_REJECTION, 0),
        "duplicate_recently_closed_count": counts.get(DUPLICATE_RECENTLY_CLOSED, 0),
        "improved_add_on_count": counts.get(IMPROVED_SIGNAL_ADD_ON, 0),
        "suppressed_duplicate_count": suppressed_count,
        "classification_counts": counts,
        "source_artifacts": {
            "candidate_lifecycle_projection_v1": str(lifecycle_path),
            "signal_evidence_boundary_v1": str(signal_boundary_path),
            "aegis_paper_review_queue_v1": str(queue_path),
            "aegis_candidate_decision_ledger_v1": str(decision_path),
            "aegis_paper_entry_receipts_v1": str(entry_path),
            "aegis_paper_exit_receipts_v1": str(exit_path),
        },
        "duplicate_rows": rows_out,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_duplicate_candidate_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    return write_json_v1(duplicate_candidate_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payload))


def build_and_write_duplicate_candidate_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_duplicate_candidate_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_duplicate_candidate_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path


def render_duplicate_candidate_v1(payload: Mapping[str, Any]) -> str:
    lines = [
        "Duplicate candidate status:",
        f"- current output candidates: {payload.get('current_output_candidate_count', 0)}",
        f"- new distinct setups: {payload.get('new_distinct_setup_count', 0)}",
        f"- duplicate open positions: {payload.get('duplicate_open_position_count', 0)}",
        f"- duplicate recent captures: {payload.get('duplicate_recent_capture_count', 0)}",
        f"- duplicate recent rejections: {payload.get('duplicate_recent_rejection_count', 0)}",
        f"- duplicate recently closed: {payload.get('duplicate_recently_closed_count', 0)}",
        f"- improved add-ons: {payload.get('improved_add_on_count', 0)}",
        f"- suppressed duplicates: {payload.get('suppressed_duplicate_count', 0)}",
    ]
    return "\n".join(lines) + "\n"
