from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1

REPORT_FAMILY = "aegis_trading_lifecycle_state_v1"
REPORT_FILENAME = "trading_lifecycle_state.v1.json"

CANONICAL_STATES = {
    "AWAITING_REVIEW",
    "PAPER_TRADE_FAILED",
    "PAPER_POSITION_OPEN",
    "PAPER_POSITION_CLOSED",
    "REJECTED",
    "EXPIRED",
    "LEGACY_PARTIAL_OPEN",
    "LEGACY_PARTIAL_CLOSED",
    "UNCLASSIFIED_LIFECYCLE_ITEM",
}


def trading_lifecycle_state_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / REPORT_FILENAME


def build_trading_lifecycle_state_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    days = _inspection_days(day_utc)
    source_paths = _source_paths(root, days, day_utc)

    queue_rows = _latest_by_candidate(_queue_rows(root, days))
    state_rows = _latest_by_candidate(_candidate_state_rows(root, day_utc))
    receipts = _receipt_rows(root, day_utc)
    receipt_by_candidate = _group_by_candidate([row for row in receipts if _is_governed_entry_receipt(row)])
    legacy_receipts = [row for row in receipts if _is_legacy_receipt(row)]
    events = _event_rows(root, day_utc)
    open_events = _group_by_candidate([row for row in events if str(row.get("event_type") or "").upper() == "PAPER_POSITION_OPENED"])
    close_events = _group_by_candidate([row for row in events if str(row.get("event_type") or "").upper() == "PAPER_POSITION_CLOSED"])
    audits = _paper_trade_audit_rows(root, days)
    failed_audits = [row for row in audits if not _audit_succeeded(row)]
    failed_by_candidate = _group_by_target(failed_audits)
    ledger = _read_json(root / "reports" / "aegis_paper_position_ledger_v1" / day_utc / "paper_position_ledger.v1.json")
    open_positions = _safe_list(ledger.get("open_positions"))
    closed_positions = _safe_list(ledger.get("closed_positions"), _safe_list(ledger.get("historical_positions")))
    ledger_legacy = _safe_list(ledger.get("legacy_captures"))
    open_by_candidate = _by_candidate(open_positions)
    closed_by_candidate = _by_candidate(closed_positions)
    legacy_rows = _legacy_rows(root, day_utc, legacy_receipts=legacy_receipts, ledger_legacy=ledger_legacy)

    candidate_ids = sorted(set(queue_rows) | set(state_rows) | set(receipt_by_candidate) | set(open_events) | set(close_events) | set(failed_by_candidate) | set(open_by_candidate) | set(closed_by_candidate))
    rows: list[dict[str, Any]] = []
    assigned_keys: set[str] = set()
    for candidate_id in candidate_ids:
        queue = queue_rows.get(candidate_id, {})
        state = state_rows.get(candidate_id, {})
        open_pos = open_by_candidate.get(candidate_id, {})
        closed_pos = closed_by_candidate.get(candidate_id, {})
        failures = failed_by_candidate.get(candidate_id, [])
        candidate_receipts = receipt_by_candidate.get(candidate_id, [])
        state_name, reason, next_action, lineage_quality = _candidate_state(
            candidate_id=candidate_id,
            queue=queue,
            state=state,
            open_pos=open_pos,
            closed_pos=closed_pos,
            failures=failures,
            receipts=candidate_receipts,
        )
        item = _canonical_row(
            symbol=_first_non_empty(open_pos.get("symbol"), closed_pos.get("symbol"), queue.get("symbol"), state.get("symbol"), _last(candidate_receipts).get("symbol")),
            candidate_id=candidate_id,
            originating_day=_first_non_empty(queue.get("originating_day"), state.get("originating_day"), queue.get("source_day"), state.get("source_queue_day"), day_utc),
            current_state=state_name,
            last_event=_last_event(queue, state, open_pos, closed_pos, failures, candidate_receipts, open_events.get(candidate_id, []), close_events.get(candidate_id, [])),
            reason=reason,
            next_action=next_action,
            source_artifacts=_artifacts_for_candidate(candidate_id, source_paths, failures, candidate_receipts, open_events.get(candidate_id, []), close_events.get(candidate_id, [])),
            lineage_quality=lineage_quality,
            payload=_first_dict(open_pos, closed_pos, queue, state),
        )
        rows.append(item)
        assigned_keys.add(_item_key(item))

    for legacy in legacy_rows:
        item = _legacy_state_row(legacy, day_utc)
        key = _item_key(item)
        if key not in assigned_keys:
            rows.append(item)
            assigned_keys.add(key)

    unclassified = _unclassified_items(receipts=receipts, events=events, rows=rows, source_paths=source_paths, day_utc=day_utc)
    rows.extend(unclassified)
    rows.sort(key=lambda row: (str(row.get("originating_day") or ""), str(row.get("symbol") or ""), str(row.get("candidate_id") or "")))
    counts = Counter(str(row.get("current_state") or "UNCLASSIFIED_LIFECYCLE_ITEM") for row in rows)
    payload = {
        "schema_id": "aegis_trading_lifecycle_state",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "canonical_states": sorted(CANONICAL_STATES),
        "counts": {state: int(counts.get(state, 0)) for state in sorted(CANONICAL_STATES)},
        "total_item_count": len(rows),
        "lifecycle_items": rows,
        "open_governed_positions": [row for row in rows if row.get("current_state") == "PAPER_POSITION_OPEN"],
        "legacy_partial_open_positions": [row for row in rows if row.get("current_state") == "LEGACY_PARTIAL_OPEN"],
        "failed_paper_trade_attempts": [row for row in rows if row.get("current_state") == "PAPER_TRADE_FAILED"],
        "awaiting_paper_trade": [row for row in rows if row.get("current_state") == "AWAITING_REVIEW"],
        "closed_positions": [row for row in rows if row.get("current_state") in {"PAPER_POSITION_CLOSED", "LEGACY_PARTIAL_CLOSED"}],
        "unclassified_lifecycle_items": [row for row in rows if row.get("current_state") == "UNCLASSIFIED_LIFECYCLE_ITEM"],
        "source_artifact_paths": source_paths,
        "safety": {
            "paper_only": True,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "live_trading_allowed": False,
        },
    }
    return payload


def write_trading_lifecycle_state_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(trading_lifecycle_state_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_trading_lifecycle_state_v1(truth_root=truth_root, day_utc=day_utc))


def _candidate_state(*, candidate_id: str, queue: dict[str, Any], state: dict[str, Any], open_pos: dict[str, Any], closed_pos: dict[str, Any], failures: list[dict[str, Any]], receipts: list[dict[str, Any]]) -> tuple[str, str, str, str]:
    queue_state = str(queue.get("status") or queue.get("current_state") or state.get("current_state") or "").upper()
    if closed_pos:
        return "PAPER_POSITION_CLOSED", "Governed paper position has a close event/ledger row.", "Review in History.", "GOVERNED"
    if open_pos:
        return "PAPER_POSITION_OPEN", "Governed paper position is open in paper_position_ledger.", "Monitor exit recommendation or Record Exit.", "GOVERNED"
    if failures:
        return "PAPER_TRADE_FAILED", str(failures[-1].get("error") or "Paper Trade action failed before receipt creation."), "Refresh queue state, confirm approval, then retry Paper Trade.", "FAILED_ACTION"
    if queue_state in {"REJECTED", "REJECTED_BY_OPERATOR"}:
        return "REJECTED", "Candidate was rejected by operator review.", "No action unless reconsidered.", "GOVERNED"
    if queue_state == "EXPIRED":
        return "EXPIRED", "Candidate aged out before a governed paper position was opened.", "No action unless regenerated.", "GOVERNED"
    if receipts:
        return "UNCLASSIFIED_LIFECYCLE_ITEM", "SIMULATED_PAPER receipt exists but no open or closed ledger row was materialized.", "Run paper-position-ledger and inspect receipt/event linkage.", "FAILED_ACTION"
    return "AWAITING_REVIEW", "Candidate is not opened yet; no successful Paper Trade receipt exists.", "Use Paper Trade if the operator wants to open a simulated position.", "GOVERNED"


def _legacy_state_row(row: dict[str, Any], day_utc: str) -> dict[str, Any]:
    state = str(row.get("current_state") or row.get("lifecycle_state") or row.get("historical_state") or row.get("current_status") or "").upper()
    closed = any(token in state for token in ("CLOSED", "EXIT", "SOLD"))
    return _canonical_row(
        symbol=row.get("symbol"),
        candidate_id=_first_non_empty(row.get("candidate_id"), row.get("selected_exposure_intent_id"), row.get("capture_record_id"), row.get("receipt_id"), row.get("trade_lifecycle_case_id")),
        originating_day=_first_non_empty(row.get("source_day"), row.get("day_utc"), str(row.get("captured_at_utc") or "")[:10], day_utc),
        current_state="LEGACY_PARTIAL_CLOSED" if closed else "LEGACY_PARTIAL_OPEN",
        last_event={
            "event_type": _first_non_empty(row.get("event_type"), row.get("current_state"), row.get("lifecycle_state"), row.get("capture_status"), "LEGACY_CAPTURE"),
            "timestamp_utc": _first_non_empty(row.get("captured_at_utc"), row.get("fill_timestamp_utc"), row.get("generated_at_utc"), row.get("created_at"), ""),
            "source_path": str(row.get("source_path") or row.get("receipt_path") or row.get("artifact_path") or ""),
        },
        reason="Legacy/manual capture has partial lineage and no governed paper ledger authority.",
        next_action="Review legacy evidence; record an exit if this partial position is no longer open.",
        source_artifacts=[str(row.get("source_path") or row.get("receipt_path") or row.get("artifact_path") or "")],
        lineage_quality="LEGACY_PARTIAL",
        payload=row,
    )


def _canonical_row(*, symbol: Any, candidate_id: Any, originating_day: Any, current_state: str, last_event: dict[str, Any], reason: str, next_action: str, source_artifacts: list[str], lineage_quality: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(symbol or "").upper(),
        "candidate_id": str(candidate_id or ""),
        "originating_day": str(originating_day or ""),
        "current_state": current_state if current_state in CANONICAL_STATES else "UNCLASSIFIED_LIFECYCLE_ITEM",
        "last_event": str(last_event.get("event_type") or ""),
        "last_event_time": str(last_event.get("timestamp_utc") or ""),
        "reason": reason,
        "next_action": next_action,
        "source_artifacts": [str(path) for path in source_artifacts if path],
        "lineage_quality": lineage_quality if lineage_quality in {"GOVERNED", "LEGACY_PARTIAL", "FAILED_ACTION"} else "FAILED_ACTION",
        "position_id": str(payload.get("position_id") or ""),
        "side": str(payload.get("side") or payload.get("action") or payload.get("direction") or ""),
        "quantity": str(payload.get("quantity") or payload.get("actual_quantity") or ""),
        "entry_price": str(payload.get("entry_price") or payload.get("paper_entry_price") or payload.get("fill_price") or ""),
        "entry_time": str(payload.get("entry_time") or payload.get("timestamp_utc") or payload.get("captured_at_utc") or ""),
        "current_status": str(payload.get("current_status") or current_state),
    }


def _legacy_rows(root: Path, day_utc: str, *, legacy_receipts: list[dict[str, Any]], ledger_legacy: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(row for row in ledger_legacy if isinstance(row, dict))
    rows.extend(legacy_receipts)
    for path in _paths_through_day(root, "manual_capture_record_v1", day_utc, "manual_capture_record.v1.jsonl"):
        rows.extend({**row, "source_path": str(path)} for row in _read_jsonl(path) if _looks_legacy_capture(row))
    for family, filename in [
        ("captured_ticket_projection_v1", "captured_ticket_projection.v1.json"),
        ("captured_ticket_history_v1", "captured_ticket_history.v1.json"),
        ("operator_state_snapshot_v1", "operator_state_snapshot.v1.json"),
    ]:
        for path in _deep_paths_through_day(root, family, day_utc, filename):
            payload = _read_json(path)
            rows.extend({**row, "source_path": str(path)} for row in _find_legacy_capture_objects(payload))
    return _dedupe_legacy(rows)


def _find_legacy_capture_objects(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(value, dict):
        if _looks_legacy_capture(value):
            found.append(value)
        for child in value.values():
            found.extend(_find_legacy_capture_objects(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_find_legacy_capture_objects(child))
    return found


def _looks_legacy_capture(row: dict[str, Any]) -> bool:
    symbol = str(row.get("symbol") or row.get("actual_symbol") or "").upper()
    status = " ".join(str(row.get(key) or "") for key in ("capture_status", "current_state", "lifecycle_state", "historical_state", "receipt_type")).upper()
    has_capture_id = bool(row.get("capture_record_id") or row.get("selected_exposure_intent_id") or row.get("manual_capture_record_id") or row.get("receipt_id"))
    return bool(symbol and has_capture_id and ("CAPTURE" in status or "LEGACY" in status or "MANUAL" in status))


def _dedupe_legacy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or row.get("actual_symbol") or "").upper()
        if not symbol:
            continue
        normalized = {**row, "symbol": symbol}
        key = _legacy_key(row, symbol)
        current = out.get(key)
        if not current or _legacy_score(normalized) > _legacy_score(current):
            out[key] = normalized
    return list(out.values())


def _legacy_key(row: dict[str, Any], symbol: str) -> str:
    selected = _first_non_empty(row.get("selected_exposure_intent_id"), row.get("recommended_trade_id"))
    if selected:
        return selected
    capture_id = _first_non_empty(row.get("capture_record_id"), row.get("manual_capture_record_id"))
    parts = capture_id.split(":")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return _first_non_empty(capture_id, row.get("receipt_id"), symbol)


def _legacy_score(row: dict[str, Any]) -> int:
    fields = ("fill_price", "entry_price", "actual_quantity", "quantity", "captured_at_utc", "fill_timestamp_utc", "capture_record_id", "source_path", "receipt_path")
    return sum(1 for field in fields if str(row.get(field) or "").strip())


def _unclassified_items(*, receipts: list[dict[str, Any]], events: list[dict[str, Any]], rows: list[dict[str, Any]], source_paths: dict[str, Any], day_utc: str) -> list[dict[str, Any]]:
    classified_candidates = {str(row.get("candidate_id") or "") for row in rows if row.get("candidate_id")}
    out: list[dict[str, Any]] = []
    for receipt in receipts:
        cid = str(receipt.get("candidate_id") or "")
        if cid or _is_legacy_receipt(receipt):
            continue
        out.append(_canonical_row(symbol=receipt.get("symbol"), candidate_id=receipt.get("receipt_id"), originating_day=receipt.get("receipt_day") or day_utc, current_state="UNCLASSIFIED_LIFECYCLE_ITEM", last_event={"event_type": "UNCLASSIFIED_RECEIPT", "timestamp_utc": receipt.get("timestamp_utc"), "source_path": receipt.get("receipt_path")}, reason="Receipt could not be linked to governed or legacy lifecycle state.", next_action="Inspect receipt and classify as governed paper or legacy partial.", source_artifacts=[str(receipt.get("receipt_path") or "")], lineage_quality="FAILED_ACTION", payload=receipt))
    for event in events:
        cid = str(event.get("candidate_id") or "")
        if cid and cid not in classified_candidates:
            out.append(_canonical_row(symbol=event.get("symbol"), candidate_id=cid, originating_day=event.get("event_day") or day_utc, current_state="UNCLASSIFIED_LIFECYCLE_ITEM", last_event={"event_type": event.get("event_type"), "timestamp_utc": event.get("event_time_utc"), "source_path": event.get("event_path")}, reason="Position event could not be linked to a queue, receipt, ledger, or candidate state row.", next_action="Inspect paper_position_events linkage.", source_artifacts=[str(event.get("event_path") or "")], lineage_quality="FAILED_ACTION", payload=event))
    return out


def _queue_rows(root: Path, days: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in days:
        path = root / "reports" / "aegis_paper_review_queue_v1" / day / "paper_review_queue.v1.json"
        payload = _read_json(path)
        rows.extend({**row, "source_day": day, "source_path": str(path)} for row in _safe_list(payload.get("rows")) if isinstance(row, dict))
    return rows


def _candidate_state_rows(root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = root / "reports" / "aegis_candidate_state_v1" / day_utc / "candidate_state.v1.json"
    payload = _read_json(path)
    return [{**row, "source_path": str(path)} for row in _safe_list(payload.get("candidates"), _safe_list(payload.get("active_candidates"))) if isinstance(row, dict)]


def _receipt_rows(root: Path, day_utc: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _paths_through_day(root, "aegis_paper_trade_receipts_v1", day_utc, "paper_trade_receipts.v1.json"):
        payload = _read_json(path)
        rows.extend({**row, "receipt_day": path.parent.name, "receipt_path": str(path)} for row in _safe_list(payload.get("receipts")) if isinstance(row, dict))
    for path in _deep_paths_through_day(root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json"):
        payload = _read_json(path)
        if payload:
            rows.append({**payload, "receipt_day": _day_from_path(path), "receipt_path": str(path), "receipt_type": str(payload.get("receipt_type") or "LEGACY_CAPTURE")})
    return rows


def _event_rows(root: Path, day_utc: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _paths_through_day(root, "aegis_paper_position_events_v1", day_utc, "paper_position_events.v1.jsonl"):
        rows.extend({**row, "event_day": path.parent.name, "event_path": str(path)} for row in _read_jsonl(path))
    return rows


def _paper_trade_audit_rows(root: Path, days: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for day in days:
        path = root / "reports" / "aegis_operator_command_audit_v1" / day / "operator_command_audit.v1.jsonl"
        rows.extend({**row, "audit_day": day, "audit_path": str(path)} for row in _read_jsonl(path) if str(row.get("command_id") or "") == "PAPER_TRADE_CANDIDATE")
    return rows


def _inspection_days(day_utc: str) -> list[str]:
    try:
        day = datetime.strptime(day_utc, "%Y-%m-%d").replace(tzinfo=UTC)
        prev = (day - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        prev = day_utc
    return sorted({prev, day_utc})


def _paths_through_day(root: Path, family: str, day_utc: str, filename: str) -> list[Path]:
    base = root / "reports" / family
    if not base.exists():
        return []
    return [path / filename for path in sorted(base.iterdir()) if path.is_dir() and path.name <= day_utc and (path / filename).exists()]


def _deep_paths_through_day(root: Path, family: str, day_utc: str, filename: str) -> list[Path]:
    base = root / "reports" / family
    if not base.exists():
        return []
    paths: list[Path] = []
    for day_dir in sorted(path for path in base.iterdir() if path.is_dir() and path.name <= day_utc):
        direct = day_dir / filename
        if direct.exists():
            paths.append(direct)
        paths.extend(sorted(day_dir.glob(f"*/{filename}")))
    return paths


def _source_paths(root: Path, days: list[str], day_utc: str) -> dict[str, Any]:
    families = {
        "paper_review_queue": ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json"),
        "candidate_state": ("aegis_candidate_state_v1", "candidate_state.v1.json"),
        "paper_position_events": ("aegis_paper_position_events_v1", "paper_position_events.v1.jsonl"),
        "paper_position_ledger": ("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"),
        "paper_trade_receipts": ("aegis_paper_trade_receipts_v1", "paper_trade_receipts.v1.json"),
        "operator_command_audit": ("aegis_operator_command_audit_v1", "operator_command_audit.v1.jsonl"),
        "manual_execution_receipts": ("manual_execution_receipt_v1", "manual_execution_receipt.v1.json"),
        "manual_capture_records": ("manual_capture_record_v1", "manual_capture_record.v1.jsonl"),
        "captured_ticket_projection": ("captured_ticket_projection_v1", "captured_ticket_projection.v1.json"),
        "captured_ticket_history": ("captured_ticket_history_v1", "captured_ticket_history.v1.json"),
        "operator_state_snapshot": ("operator_state_snapshot_v1", "operator_state_snapshot.v1.json"),
    }
    out: dict[str, Any] = {}
    for key, (family, filename) in families.items():
        if key in {"manual_execution_receipts", "captured_ticket_projection", "captured_ticket_history", "manual_capture_records", "operator_state_snapshot"}:
            paths = _deep_paths_through_day(root, family, day_utc, filename) if key != "manual_capture_records" else _paths_through_day(root, family, day_utc, filename)
            out[key] = [str(path) for path in paths]
            continue
        selected_days = [day_utc] if key in {"candidate_state", "paper_position_ledger"} else days
        out[key] = {day: str(root / "reports" / family / day / filename) for day in selected_days if (root / "reports" / family / day / filename).exists()}
    return out


def _latest_by_candidate(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        cid = str(row.get("candidate_id") or row.get("linked_candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _by_candidate(rows: list[Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("candidate_id") or row.get("linked_candidate_id") or "")
        if cid:
            out[cid] = row
    return out


def _group_by_candidate(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cid = str(row.get("candidate_id") or row.get("linked_candidate_id") or "")
        if cid:
            out[cid].append(row)
    return dict(out)


def _group_by_target(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cid = str(row.get("target_id") or (row.get("request_payload") or {}).get("target_id") or "")
        if cid:
            out[cid].append(row)
    return dict(out)


def _is_governed_entry_receipt(row: dict[str, Any]) -> bool:
    return str(row.get("receipt_type") or "").upper() == "SIMULATED_PAPER" and str(row.get("action") or "").upper() != "EXIT" and bool(str(row.get("candidate_id") or ""))


def _is_legacy_receipt(row: dict[str, Any]) -> bool:
    receipt_type = str(row.get("receipt_type") or "").upper()
    return receipt_type in {"LEGACY_CAPTURE", "LEGACY"} or (receipt_type and receipt_type != "SIMULATED_PAPER") or (not str(row.get("candidate_id") or "") and bool(str(row.get("actual_symbol") or row.get("symbol") or "")))


def _audit_succeeded(row: dict[str, Any]) -> bool:
    summary = row.get("response_summary") if isinstance(row.get("response_summary"), dict) else {}
    return bool(summary.get("ok") is True or str(row.get("result") or "").upper() == "SIMULATED_PAPER_RECEIPT_RECORDED")


def _last_event(*items: Any) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, list):
            candidates.extend(row for row in item if isinstance(row, dict))
        elif isinstance(item, dict) and item:
            candidates.append(item)
    if not candidates:
        return {}
    def ts(row: dict[str, Any]) -> str:
        return str(row.get("event_time_utc") or row.get("timestamp_utc") or row.get("requested_at") or row.get("entry_time") or row.get("exit_time") or row.get("timestamp") or "")
    row = sorted(candidates, key=ts)[-1]
    return {
        "event_type": str(row.get("event_type") or row.get("result") or row.get("status") or row.get("current_state") or row.get("receipt_type") or "LIFECYCLE_UPDATE"),
        "timestamp_utc": ts(row),
        "source_path": str(row.get("source_path") or row.get("audit_path") or row.get("receipt_path") or row.get("event_path") or ""),
    }


def _artifacts_for_candidate(candidate_id: str, source_paths: dict[str, Any], *row_groups: Any) -> list[str]:
    paths: list[str] = []
    for group in row_groups:
        rows = group if isinstance(group, list) else [group]
        for row in rows:
            if isinstance(row, dict):
                paths.extend(str(row.get(key) or "") for key in ("source_path", "audit_path", "receipt_path", "event_path") if row.get(key))
    return sorted(set(paths))


def _item_key(row: dict[str, Any]) -> str:
    return _first_non_empty(row.get("candidate_id"), row.get("position_id"), row.get("symbol"))


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _first_dict(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict) and value:
            return value
    return {}


def _last(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return rows[-1] if rows else {}


def _safe_list(*values: Any) -> list[Any]:
    for value in values:
        if isinstance(value, list):
            return value
    return []


def _day_from_path(path: Path) -> str:
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except Exception:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows
