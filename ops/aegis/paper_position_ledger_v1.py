from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import declared_or_legacy_mapping_v1

REPORT_LEDGER = "aegis_paper_position_ledger_v1"
REPORT_EVENTS = "aegis_paper_position_events_v1"


def paper_position_ledger_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_LEDGER / day_utc / "paper_position_ledger.v1.json"


def paper_position_events_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_EVENTS / day_utc / "paper_position_events.v1.jsonl"


def position_id_for_candidate_v1(candidate_id: str) -> str:
    return f"paper-position:{str(candidate_id or '').strip()}"


def append_paper_position_event_v1(*, truth_root: Path, day_utc: str, event: dict[str, Any]) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    payload = {
        "schema_id": "paper_position_event",
        "schema_version": "v1",
        "day_utc": day_utc,
        "event_time_utc": str(event.get("event_time_utc") or event.get("timestamp_utc") or now_utc_v1()),
        "paper_only": True,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        **event,
    }
    payload["event_id"] = str(payload.get("event_id") or _event_id(payload))
    path = paper_position_events_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
    return payload


def open_position_event_from_receipt_v1(*, receipt: dict[str, Any], candidate: dict[str, Any] | None = None, day_utc: str) -> dict[str, Any]:
    candidate = candidate if isinstance(candidate, dict) else {}
    candidate_id = str(receipt.get("candidate_id") or candidate.get("candidate_id") or "").strip()
    symbol = str(receipt.get("symbol") or candidate.get("symbol") or "").upper()
    entry_time = str(receipt.get("timestamp_utc") or receipt.get("timestamp") or now_utc_v1())
    return {
        "event_type": "PAPER_POSITION_OPENED",
        "position_id": str(receipt.get("position_id") or position_id_for_candidate_v1(candidate_id)),
        "paper_session_id": str(receipt.get("paper_session_id") or candidate.get("paper_session_id") or ""),
        "candidate_id": candidate_id,
        "symbol": symbol,
        "side": _side(receipt.get("action") or candidate.get("direction") or "BUY"),
        "quantity": str(receipt.get("quantity") or ""),
        "notional": str(receipt.get("notional") or ""),
        "entry_price": str(receipt.get("paper_entry_price") or receipt.get("entry_price") or ""),
        "entry_time": entry_time,
        "operator": str(receipt.get("operator") or ""),
        "source_receipt": receipt,
        "candidate_lineage": _candidate_lineage(candidate, receipt),
        "event_time_utc": entry_time,
        "originating_day": day_utc,
    }


def close_position_event_from_receipt_v1(*, receipt: dict[str, Any], open_position: dict[str, Any], day_utc: str) -> dict[str, Any]:
    candidate_id = str(receipt.get("candidate_id") or open_position.get("candidate_id") or "").strip()
    exit_time = str(receipt.get("timestamp_utc") or receipt.get("timestamp") or now_utc_v1())
    return {
        "event_type": "PAPER_POSITION_CLOSED",
        "position_id": str(open_position.get("position_id") or receipt.get("position_id") or position_id_for_candidate_v1(candidate_id)),
        "paper_session_id": str(receipt.get("paper_session_id") or open_position.get("paper_session_id") or ""),
        "candidate_id": candidate_id,
        "symbol": str(receipt.get("symbol") or open_position.get("symbol") or "").upper(),
        "exit_price": str(receipt.get("paper_exit_price") or receipt.get("exit_price") or ""),
        "exit_time": exit_time,
        "realized_pnl": str(receipt.get("realized_pnl") or ""),
        "operator": str(receipt.get("operator") or ""),
        "source_receipt": receipt,
        "event_time_utc": exit_time,
        "originating_day": day_utc,
    }


def build_paper_position_ledger_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    events = _load_events_through_day(root, day_utc)
    bootstrap_events = _bootstrap_events_from_receipts(root, day_utc, explicit_position_ids={str(row.get("position_id") or "") for row in events})
    closure_ingestion = _closure_events_from_auto_closure(
        root,
        day_utc,
        source_events=events + bootstrap_events,
    )
    all_events = sorted(
        events + bootstrap_events + closure_ingestion["events"],
        key=lambda row: (str(row.get("event_time_utc") or ""), _event_type_order(row), str(row.get("event_id") or "")),
    )
    positions: dict[str, dict[str, Any]] = {}
    legacy_captures: list[dict[str, Any]] = []
    invalidated: list[dict[str, Any]] = []
    for event in all_events:
        event_type = str(event.get("event_type") or "").upper()
        if event_type == "PAPER_POSITION_OPENED":
            pos = _position_from_open_event(event)
            if _is_legacy_capture(pos):
                pos["current_status"] = "LEGACY"
                pos["legacy_classification"] = "LEGACY_CAPTURE"
                legacy_captures.append(pos)
            else:
                positions[str(pos["position_id"])] = pos
        elif event_type == "PAPER_POSITION_CLOSED":
            position_id = str(event.get("position_id") or position_id_for_candidate_v1(str(event.get("candidate_id") or "")))
            pos = positions.get(position_id)
            if not pos:
                pos = _position_from_close_only_event(event)
                positions[position_id] = pos
            _apply_close_event(pos, event)
        elif event_type == "PAPER_POSITION_INVALIDATED":
            position_id = str(event.get("position_id") or position_id_for_candidate_v1(str(event.get("candidate_id") or "")))
            pos = positions.get(position_id) or _position_from_close_only_event(event)
            pos["current_status"] = "INVALIDATED"
            pos["invalidation_reason"] = str(event.get("reason") or "INVALIDATED_BY_OPERATOR")
            positions[position_id] = pos
            invalidated.append(pos)

    lineage_index = _candidate_lineage_index(root, day_utc)
    repaired_positions = [_repair_position_lineage(row, lineage_index) for row in positions.values()]
    open_symbols = sorted({str(row.get("symbol") or "").upper() for row in repaired_positions if str(row.get("current_status") or "").upper() == "OPEN" and str(row.get("symbol") or "").strip()})
    market_path, market_payload = _latest_market_data_payload(root, day_utc, required_symbols=open_symbols)
    marks = _market_marks_by_symbol(market_payload)
    rows = [_mark_position(row, marks, market_path=market_path, market_payload=market_payload, day_utc=day_utc) for row in repaired_positions]
    rows.sort(key=lambda row: (str(row.get("entry_time") or ""), str(row.get("position_id") or "")))
    open_positions = [row for row in rows if str(row.get("current_status") or "").upper() == "OPEN"]
    closed_positions = [row for row in rows if str(row.get("current_status") or "").upper() == "CLOSED"]
    ledger_mismatches = _receipt_ledger_mismatches(root, day_utc, rows)
    payload = {
        "schema_id": "aegis_paper_position_ledger",
        "schema_version": "v1",
        "artifact_id": "aegis_paper_position_ledger_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "truth_model": "EVENT_SOURCED_PAPER_POSITION_LEDGER",
        "position_count": len(rows),
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "legacy_capture_count": len(legacy_captures),
        "invalidated_position_count": len([row for row in rows if str(row.get("current_status") or "").upper() == "INVALIDATED"]),
        "paper_position_ledger_mismatch_count": len(ledger_mismatches),
        "paper_position_ledger_mismatches": ledger_mismatches,
        "closure_ingestion": closure_ingestion["summary"],
        "closure_ingestion_unmatched": closure_ingestion["unmatched"],
        "closure_ingestion_blocked": closure_ingestion["blocked"],
        "closure_ingestion_source_paths": closure_ingestion["source_paths"],
        "positions": rows,
        "open_positions": open_positions,
        "closed_positions": closed_positions,
        "historical_positions": closed_positions,
        "legacy_captures": legacy_captures,
        "invalidated_positions": invalidated,
        "events": all_events,
        "event_count": len(all_events),
        "source_event_paths": [str(path) for path in _event_paths_through_day(root, day_utc)],
        "market_data_path": str(market_path or ""),
        "repair_action": f"TARGET_DAY={day_utc} npm run aegis:paper-position-ledger",
        "safety": _safety(),
    }
    return payload


def materialize_paper_position_events_v1(*, truth_root: Path, day_utc: str, events: list[dict[str, Any]]) -> Path:
    root = Path(truth_root).expanduser().resolve()
    path = paper_position_events_path_v1(truth_root=root, day_utc=day_utc)
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [row for row in events if isinstance(row, dict) and str(row.get("originating_day") or row.get("day_utc") or day_utc) == day_utc]
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    return path


def write_paper_position_ledger_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(paper_position_ledger_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_paper_position_ledger_v1(truth_root=truth_root, day_utc=day_utc))


def _load_events_through_day(root: Path, day_utc: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _event_paths_through_day(root, day_utc):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _event_paths_through_day(root: Path, day_utc: str) -> list[Path]:
    base = root / "reports" / REPORT_EVENTS
    if not base.exists():
        return []
    paths = []
    for day_dir in sorted(path for path in base.iterdir() if path.is_dir() and path.name <= day_utc):
        path = day_dir / "paper_position_events.v1.jsonl"
        if path.exists():
            paths.append(path)
    return paths


def _bootstrap_events_from_receipts(root: Path, day_utc: str, explicit_position_ids: set[str]) -> list[dict[str, Any]]:
    base = root / "reports" / "aegis_paper_trade_receipts_v1"
    if not base.exists():
        return []
    events: list[dict[str, Any]] = []
    opens: dict[str, dict[str, Any]] = {}
    for day_dir in sorted(path for path in base.iterdir() if path.is_dir() and path.name <= day_utc):
        path = day_dir / "paper_trade_receipts.v1.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        receipts = payload.get("receipts") if isinstance(payload.get("receipts"), list) else []
        candidates = _candidate_index(root, day_dir.name)
        for receipt in receipts:
            if not isinstance(receipt, dict):
                continue
            candidate_id = str(receipt.get("candidate_id") or "")
            position_id = str(receipt.get("position_id") or position_id_for_candidate_v1(candidate_id))
            if position_id in explicit_position_ids:
                continue
            action = str(receipt.get("action") or "").upper()
            if action == "EXIT":
                open_position = opens.get(position_id) or {"position_id": position_id, "candidate_id": candidate_id, "symbol": receipt.get("symbol")}
                events.append(close_position_event_from_receipt_v1(receipt=receipt, open_position=open_position, day_utc=day_dir.name))
            else:
                event = open_position_event_from_receipt_v1(receipt=receipt, candidate=candidates.get(candidate_id, {}), day_utc=day_dir.name)
                if _legacy_receipt(receipt):
                    event["event_type"] = "PAPER_POSITION_LEGACY_CAPTURED"
                else:
                    opens[position_id] = _position_from_open_event(event)
                events.append(event)
    for event in events:
        event["event_id"] = str(event.get("event_id") or _event_id(event))
        if str(event.get("event_type") or "").upper() == "PAPER_POSITION_LEGACY_CAPTURED":
            event["event_type"] = "PAPER_POSITION_OPENED"
            event["legacy_classification"] = "LEGACY_CAPTURE"
    return events


def _closure_events_from_auto_closure(root: Path, day_utc: str, *, source_events: list[dict[str, Any]]) -> dict[str, Any]:
    existing_open_position_ids = {
        str(row.get("position_id") or position_id_for_candidate_v1(str(row.get("candidate_id") or "")))
        for row in source_events
        if str(row.get("event_type") or "").upper() == "PAPER_POSITION_OPENED"
    }
    existing_closed_position_ids = {
        str(row.get("position_id") or position_id_for_candidate_v1(str(row.get("candidate_id") or "")))
        for row in source_events
        if str(row.get("event_type") or "").upper() == "PAPER_POSITION_CLOSED"
    }
    source_paths = _auto_closure_paths_through_day(root, day_utc)
    events: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    observed = 0
    duplicate = 0
    for path in source_paths:
        payload = _read_json(path)
        for row in _auto_closed_rows(payload):
            observed += 1
            position_id = str(row.get("position_id") or position_id_for_candidate_v1(str(row.get("candidate_id") or ""))).strip()
            candidate_id = str(row.get("candidate_id") or "").strip()
            exit_price = row.get("exit_mark") if row.get("exit_mark") not in (None, "") else row.get("paper_exit_price")
            exit_time = str(row.get("outcome_timestamp") or row.get("exit_timestamp") or row.get("trigger_timestamp") or "").strip()
            reasons = []
            if not position_id:
                reasons.append("POSITION_ID_MISSING")
            if _decimal(exit_price) is None:
                reasons.append("EXIT_PRICE_MISSING")
            if not exit_time:
                reasons.append("EXIT_TIMESTAMP_MISSING")
            if reasons:
                blocked.append(_closure_ingestion_issue(row, path=path, status="BLOCKED", reason_codes=reasons))
                continue
            if position_id not in existing_open_position_ids:
                unmatched.append(
                    _closure_ingestion_issue(
                        row,
                        path=path,
                        status="UNMATCHED",
                        reason_codes=["POSITION_ID_NOT_FOUND_IN_PAPER_POSITION_OPEN_EVENTS"],
                    )
                )
                continue
            if position_id in existing_closed_position_ids:
                duplicate += 1
                blocked.append(
                    _closure_ingestion_issue(
                        row,
                        path=path,
                        status="DUPLICATE_SKIPPED",
                        reason_codes=["PAPER_POSITION_CLOSE_EVENT_ALREADY_EXISTS"],
                    )
                )
                continue
            close_event = {
                "event_type": "PAPER_POSITION_CLOSED",
                "position_id": position_id,
                "candidate_id": candidate_id,
                "symbol": str(row.get("symbol") or "").upper(),
                "exit_price": str(exit_price),
                "exit_time": exit_time,
                "operator": "AEGIS_AUTO_CLOSURE_INGESTION",
                "source_receipt": {
                    "receipt_type": "DERIVED_FROM_AEGIS_PAPER_OUTCOME_AUTO_CLOSURE_V1",
                    "closure_id": str(row.get("closure_id") or ""),
                    "auto_closure_state": str(row.get("auto_closure_state") or ""),
                    "exit_trigger": str(row.get("exit_trigger") or ""),
                    "exit_reason_selected_by_operator": str(row.get("exit_trigger") or "AUTO_CLOSED_PAPER_OUTCOME"),
                    "source_artifact_path": str(path),
                    "source_artifact_hash": _file_hash(path),
                    "source_content_hash": str(row.get("content_hash") or ""),
                    "source_row": row,
                },
                "source_artifacts": [str(path), *[str(item) for item in row.get("source_artifacts", []) if str(item or "")]],
                "source_hashes": row.get("source_hashes") if isinstance(row.get("source_hashes"), dict) else {str(path): _file_hash(path)},
                "closure_lineage": {
                    "source_family": "aegis_paper_outcome_auto_closure_v1",
                    "source_path": str(path),
                    "source_hash": _file_hash(path),
                    "source_content_hash": str(row.get("content_hash") or ""),
                    "closure_id": str(row.get("closure_id") or ""),
                    "auto_closure_state": str(row.get("auto_closure_state") or ""),
                    "exit_trigger": str(row.get("exit_trigger") or ""),
                    "realized_return": str(row.get("realized_return") or ""),
                    "deterministic_return_formula_version": str(row.get("deterministic_return_formula_version") or ""),
                },
                "event_time_utc": exit_time,
                "originating_day": str(path.parent.name or day_utc),
            }
            close_event["event_id"] = _event_id(close_event)
            events.append(close_event)
            existing_closed_position_ids.add(position_id)
    return {
        "events": events,
        "unmatched": unmatched,
        "blocked": blocked,
        "source_paths": [str(path) for path in source_paths],
        "summary": {
            "source_family": "aegis_paper_outcome_auto_closure_v1",
            "source_closure_count": observed,
            "ingested_closure_count": len(events),
            "unmatched_closure_count": len(unmatched),
            "blocked_closure_count": len(blocked),
            "duplicate_closure_count": duplicate,
            "read_model_only": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }


def _auto_closure_paths_through_day(root: Path, day_utc: str) -> list[Path]:
    path = root / "reports" / "aegis_paper_outcome_auto_closure_v1" / day_utc / "paper_outcome_auto_closure.v1.json"
    return [path] if path.exists() else []


def _auto_closed_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("rows", "closures"):
        values = payload.get(key)
        if not isinstance(values, list):
            continue
        for row in values:
            if not isinstance(row, dict):
                continue
            if str(row.get("auto_closure_state") or "").upper() == "AUTO_CLOSED_PAPER_OUTCOME":
                rows.append(row)
    return rows


def _closure_ingestion_issue(row: dict[str, Any], *, path: Path, status: str, reason_codes: list[str]) -> dict[str, Any]:
    return {
        "status": status,
        "reason_codes": reason_codes,
        "candidate_id": str(row.get("candidate_id") or ""),
        "position_id": str(row.get("position_id") or position_id_for_candidate_v1(str(row.get("candidate_id") or ""))),
        "symbol": str(row.get("symbol") or "").upper(),
        "sleeve_id": str(row.get("sleeve_id") or row.get("sleeve") or ""),
        "exit_price_present": _decimal(row.get("exit_mark") if row.get("exit_mark") not in (None, "") else row.get("paper_exit_price")) is not None,
        "exit_timestamp_present": bool(str(row.get("outcome_timestamp") or row.get("exit_timestamp") or row.get("trigger_timestamp") or "").strip()),
        "source_artifact_path": str(path),
        "source_artifact_hash": _file_hash(path),
        "source_content_hash": str(row.get("content_hash") or ""),
    }


def _candidate_index(root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    _, packet = latest_json_v1(root, "aegis_candidate_review_packet_v1", day_utc, "candidate_review_packet.v1.json")
    rows = packet.get("review_candidates") if isinstance(packet.get("review_candidates"), list) else []
    return {str(row.get("candidate_id") or ""): row for row in rows if isinstance(row, dict)}




def _candidate_lineage_index(root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    families = (
        ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        ("aegis_candidate_review_packet_v1", "candidate_review_packet.v1.json"),
        ("aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"),
        ("aegis_candidate_lifecycle_projection_v1", "candidate_lifecycle_projection.v1.json"),
    )
    for family, filename in families:
        base = root / "reports" / family
        if not base.exists():
            continue
        for path in sorted(base.glob(f"*/{filename}")):
            if path.parent.name > day_utc:
                continue
            payload = _read_json(path)
            for row in _walk_dicts(payload):
                candidate_id = str(row.get("candidate_id") or row.get("candidate_contract_id") or "").strip()
                if not candidate_id:
                    continue
                existing = index.get(candidate_id, {})
                if existing.get("sleeve_id") and existing.get("raw_signal_id"):
                    continue
                evidence_paths = row.get("evidence_paths") if isinstance(row.get("evidence_paths"), list) else existing.get("evidence_paths", [])
                evidence_hashes = row.get("evidence_hashes") if isinstance(row.get("evidence_hashes"), dict) else existing.get("evidence_hashes", {})
                index[candidate_id] = {
                    "candidate_id": candidate_id,
                    "sleeve_id": str(row.get("sleeve_id") or existing.get("sleeve_id") or "").strip(),
                    "raw_signal_id": str(row.get("raw_signal_id") or existing.get("raw_signal_id") or "").strip(),
                    "intent_id": str(row.get("intent_id") or existing.get("intent_id") or "").strip(),
                    "symbol": str(row.get("symbol") or existing.get("symbol") or "").upper().strip(),
                    "candidate_created_at": str(row.get("candidate_snapshot_timestamp_utc") or row.get("candidate_snapshot_timestamp") or row.get("generated_at_utc") or row.get("generated_at") or existing.get("candidate_created_at") or ""),
                    "evidence_paths": evidence_paths,
                    "evidence_hashes": evidence_hashes,
                    "lineage_source_path": str(path),
                }
    return index


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _repair_position_lineage(pos: dict[str, Any], lineage_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    candidate_id = str(pos.get("candidate_id") or "").strip()
    recovered = lineage_index.get(candidate_id, {})
    lineage = pos.get("candidate_lineage") if isinstance(pos.get("candidate_lineage"), dict) else {}
    merged = {
        "paper_session_id": str(lineage.get("paper_session_id") or pos.get("paper_session_id") or ""),
        "candidate_id": candidate_id,
        "raw_signal_id": str(lineage.get("raw_signal_id") or recovered.get("raw_signal_id") or ""),
        "intent_id": str(lineage.get("intent_id") or recovered.get("intent_id") or ""),
        "sleeve_id": str(lineage.get("sleeve_id") or recovered.get("sleeve_id") or ""),
        "candidate_created_at": str(lineage.get("candidate_created_at") or recovered.get("candidate_created_at") or ""),
        "evidence_paths": lineage.get("evidence_paths") if isinstance(lineage.get("evidence_paths"), list) and lineage.get("evidence_paths") else recovered.get("evidence_paths", []),
        "evidence_hashes": lineage.get("evidence_hashes") if isinstance(lineage.get("evidence_hashes"), dict) and lineage.get("evidence_hashes") else recovered.get("evidence_hashes", {}),
        "lineage_source_path": str(lineage.get("lineage_source_path") or recovered.get("lineage_source_path") or ""),
    }
    pos["candidate_lineage"] = merged
    pos["sleeve_id"] = merged["sleeve_id"] or "UNKNOWN"
    pos["sleeve_assignment_source"] = "candidate_lineage_recovery" if merged["sleeve_id"] and recovered else ("candidate_lineage" if merged["sleeve_id"] else "unresolved")
    if recovered.get("symbol") and not pos.get("symbol"):
        pos["symbol"] = recovered["symbol"]
    hypothesis_mapping = declared_or_legacy_mapping_v1(pos, sleeve_id=str(pos.get("sleeve_id") or merged.get("sleeve_id") or ""))
    if hypothesis_mapping.get("hypothesis_id"):
        pos["hypothesis_id"] = hypothesis_mapping.get("hypothesis_id")
        pos["thesis_id"] = hypothesis_mapping.get("thesis_id")
        pos["hypothesis_mapping_confidence"] = hypothesis_mapping.get("mapping_confidence")
        pos["hypothesis_mapping_source"] = hypothesis_mapping.get("mapping_source")
        merged["hypothesis_id"] = hypothesis_mapping.get("hypothesis_id")
        merged["thesis_id"] = hypothesis_mapping.get("thesis_id")
        merged["hypothesis_mapping_confidence"] = hypothesis_mapping.get("mapping_confidence")
        pos["candidate_lineage"] = merged
    return pos

def _receipt_rows_through_day(root: Path, day_utc: str) -> list[dict[str, Any]]:
    base = root / "reports" / "aegis_paper_trade_receipts_v1"
    if not base.exists():
        return []
    rows: list[dict[str, Any]] = []
    for day_dir in sorted(path for path in base.iterdir() if path.is_dir() and path.name <= day_utc):
        path = day_dir / "paper_trade_receipts.v1.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        receipts = payload.get("receipts") if isinstance(payload.get("receipts"), list) else []
        for receipt in receipts:
            if not isinstance(receipt, dict):
                continue
            rows.append({"receipt": receipt, "receipt_path": str(path), "receipt_day": day_dir.name})
    return rows


def _receipt_ledger_mismatches(root: Path, day_utc: str, positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    position_candidate_ids = {str(row.get("candidate_id") or row.get("linked_candidate_id") or "") for row in positions}
    position_ids = {str(row.get("position_id") or "") for row in positions}
    mismatches: list[dict[str, Any]] = []
    for item in _receipt_rows_through_day(root, day_utc):
        receipt = item["receipt"]
        if _legacy_receipt(receipt):
            continue
        action = str(receipt.get("action") or "").upper()
        if action == "EXIT":
            continue
        candidate_id = str(receipt.get("candidate_id") or "").strip()
        position_id = str(receipt.get("position_id") or position_id_for_candidate_v1(candidate_id))
        if candidate_id in position_candidate_ids or position_id in position_ids:
            continue
        mismatches.append({
            "status": "PAPER_POSITION_LEDGER_MISMATCH",
            "candidate_id": candidate_id,
            "symbol": str(receipt.get("symbol") or "").upper(),
            "receipt_path": item["receipt_path"],
            "receipt_day": item["receipt_day"],
            "missing_event": "PAPER_POSITION_OPENED",
            "reason": "SIMULATED_PAPER receipt exists but no open or closed paper_position_ledger row was materialized.",
            "repair_action": f"TARGET_DAY={day_utc} npm run aegis:paper-position-ledger",
        })
    return mismatches


def _position_from_open_event(event: dict[str, Any]) -> dict[str, Any]:
    receipt = event.get("source_receipt") if isinstance(event.get("source_receipt"), dict) else {}
    lineage = event.get("candidate_lineage") if isinstance(event.get("candidate_lineage"), dict) else {}
    return {
        "position_id": str(event.get("position_id") or position_id_for_candidate_v1(str(event.get("candidate_id") or ""))),
        "paper_session_id": str(event.get("paper_session_id") or receipt.get("paper_session_id") or lineage.get("paper_session_id") or ""),
        "paper_session_id": str(event.get("paper_session_id") or ""),
        "candidate_id": str(event.get("candidate_id") or ""),
        "symbol": str(event.get("symbol") or "").upper(),
        "side": _side(event.get("side") or receipt.get("action") or ""),
        "quantity": str(event.get("quantity") or receipt.get("quantity") or ""),
        "notional": str(event.get("notional") or receipt.get("notional") or ""),
        "entry_price": str(event.get("entry_price") or receipt.get("paper_entry_price") or ""),
        "entry_time": str(event.get("entry_time") or event.get("event_time_utc") or ""),
        "current_status": "OPEN",
        "exit_price": "",
        "exit_time": "",
        "unrealized_pnl": "",
        "realized_pnl": "",
        "source_receipt": receipt,
        "candidate_lineage": lineage,
        "operator": str(event.get("operator") or receipt.get("operator") or ""),
        "source_event_id": str(event.get("event_id") or ""),
        "originating_day": str(event.get("originating_day") or str(event.get("entry_time") or "")[:10]),
        "current_state": str(event.get("current_state") or ""),
        "paper_tracking_mode": str(event.get("paper_tracking_mode") or ""),
        "human_approval_status": str(event.get("human_approval_status") or ""),
        "auto_promotion_reason_codes": event.get("auto_promotion_reason_codes") if isinstance(event.get("auto_promotion_reason_codes"), list) else [],
        "promotion_timestamp": str(event.get("promotion_timestamp") or ""),
        "source_artifacts": event.get("source_artifacts") if isinstance(event.get("source_artifacts"), dict) else {},
        "source_hashes": event.get("source_hashes") if isinstance(event.get("source_hashes"), dict) else {},
        "trade_advice_allowed": False,
        "manual_capture_allowed": False,
        "broker_submit_transmit_allowed": False,
        "live_trade_eligible": False,
        "automatic_approval_allowed": False,
        "paper_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _position_from_close_only_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "position_id": str(event.get("position_id") or position_id_for_candidate_v1(str(event.get("candidate_id") or ""))),
        "paper_session_id": str(event.get("paper_session_id") or ""),
        "candidate_id": str(event.get("candidate_id") or ""),
        "symbol": str(event.get("symbol") or "").upper(),
        "side": "",
        "quantity": "",
        "notional": "",
        "entry_price": "",
        "entry_time": "",
        "current_status": "OPEN",
        "exit_price": "",
        "exit_time": "",
        "unrealized_pnl": "",
        "realized_pnl": "",
        "source_receipt": {},
        "candidate_lineage": {},
        "operator": str(event.get("operator") or ""),
        "paper_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _apply_close_event(pos: dict[str, Any], event: dict[str, Any]) -> None:
    pos["current_status"] = "CLOSED"
    pos["exit_price"] = str(event.get("exit_price") or "")
    pos["exit_time"] = str(event.get("exit_time") or event.get("event_time_utc") or "")
    realized = _decimal(event.get("realized_pnl"))
    if realized is None:
        exit_price = _decimal(pos.get("exit_price")) or Decimal("0")
        entry = _decimal(pos.get("entry_price")) or Decimal("0")
        qty = _decimal(pos.get("quantity")) or Decimal("0")
        sign = Decimal("1") if str(pos.get("side") or "").upper() in {"BUY", "LONG", "COVER", ""} else Decimal("-1")
        realized = (exit_price - entry) * qty * sign
    pos["realized_pnl"] = _fmt_decimal(realized)
    pos["unrealized_pnl"] = ""
    pos["exit_source_receipt"] = event.get("source_receipt") if isinstance(event.get("source_receipt"), dict) else {}
    if isinstance(event.get("closure_lineage"), dict):
        pos["closure_lineage"] = event["closure_lineage"]
    if isinstance(event.get("source_artifacts"), list):
        pos["closure_source_artifacts"] = event["source_artifacts"]
    if isinstance(event.get("source_hashes"), dict):
        pos["closure_source_hashes"] = event["source_hashes"]
    pos["operator"] = str(event.get("operator") or pos.get("operator") or "")


def _mark_position(pos: dict[str, Any], marks: dict[str, Any], *, market_path: Path | None, market_payload: dict[str, Any], day_utc: str) -> dict[str, Any]:
    if str(pos.get("current_status") or "").upper() != "OPEN":
        return pos
    market_row = marks.get(str(pos.get("symbol") or "")) if isinstance(marks.get(str(pos.get("symbol") or "")), dict) else {}
    mark = _decimal(market_row.get("last_price") or market_row.get("close"))
    entry = _decimal(pos.get("entry_price")) or Decimal("0")
    qty = _decimal(pos.get("quantity")) or Decimal("0")
    side = str(pos.get("side") or "BUY").upper()
    sign = Decimal("1") if side in {"BUY", "LONG", "COVER", ""} else Decimal("-1")
    certified = _certified_mark_status(market_row=market_row, day_utc=day_utc)
    unrealized = ((mark - entry) * qty * sign) if mark is not None and certified == "CERTIFIED" else None
    pos["mark_price"] = _fmt_decimal(mark) if mark is not None else ""
    pos["current_certified_mark"] = _fmt_decimal(mark) if mark is not None and certified == "CERTIFIED" else ""
    pos["mark_timestamp_utc"] = str(market_row.get("data_timestamp_utc") or market_row.get("source_timestamp_utc") or market_row.get("retrieved_at_utc") or "")
    pos["mark_source_path"] = str(market_row.get("source_url_or_path") or market_path or "")
    pos["mark_source_hash"] = str(market_row.get("source_hash") or (_file_hash(market_path) if market_path else ""))
    pos["mark_freshness_status"] = str(market_row.get("freshness_status") or ("MISSING" if not market_row else "UNKNOWN")).upper()
    pos["mark_market_session_date"] = str(market_row.get("market_session_date") or market_payload.get("market_session_date") or "")
    pos["mark_certification_status"] = certified
    pos["unrealized_pnl_status"] = "AVAILABLE" if unrealized is not None else "NOT_CANONICAL"
    pos["unrealized_pnl"] = _fmt_decimal(unrealized) if unrealized is not None else ""
    return pos


def _certified_mark_status(*, market_row: dict[str, Any], day_utc: str) -> str:
    if not market_row:
        return "MISSING_MARK"
    freshness = str(market_row.get("freshness_status") or "").upper()
    session = str(market_row.get("market_session_date") or "")
    mark = _decimal(market_row.get("last_price") or market_row.get("close"))
    if mark is None:
        return "MISSING_MARK"
    if freshness != "CURRENT":
        return "STALE_MARK"
    if session and session > day_utc:
        return "STALE_MARK"
    return "CERTIFIED"


def _latest_market_data_payload(root: Path, day_utc: str, required_symbols: list[str] | None = None) -> tuple[Path | None, dict[str, Any]]:
    base = root / "reports" / "aegis_market_data_v1"
    required = {str(symbol or "").upper().strip() for symbol in (required_symbols or []) if str(symbol or "").strip()}
    candidates: list[tuple[Path | None, dict[str, Any]]] = []
    target_path = base / day_utc / "market_data.v1.json"
    if target_path.exists():
        candidates.append((target_path, _read_json(target_path)))
    inputs_path, inputs_payload = latest_json_v1(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")
    if inputs_path:
        candidates.append((inputs_path, inputs_payload))
    if base.exists():
        for path in sorted(base.glob("*/market_data.v1.json"), reverse=True):
            if path.parent.name > day_utc or path == target_path:
                continue
            candidates.append((path, _read_json(path)))
    scored: list[tuple[int, int, str, Path | None, dict[str, Any]]] = []
    for path, payload in candidates:
        marks = _market_marks_by_symbol(payload)
        if not marks:
            continue
        coverage = len(required.intersection(set(marks))) if required else len(marks)
        same_day = 1 if str(payload.get("day_utc") or payload.get("market_session_date") or (path.parent.name if path else "")) == day_utc else 0
        scored.append((coverage, same_day, str(path or ""), path, payload))
    if scored:
        scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        return scored[0][3], scored[0][4]
    return latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")


def _market_marks_by_symbol(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    symbols = payload.get("symbols")
    if isinstance(symbols, dict) and symbols:
        return {str(key).upper(): dict(value) for key, value in symbols.items() if isinstance(value, dict)}
    if isinstance(symbols, list) and symbols:
        out: dict[str, dict[str, Any]] = {}
        for row in symbols:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("canonical_symbol") or "").upper().strip()
            if symbol:
                out[symbol] = dict(row)
        return out
    records = payload.get("input_records")
    if isinstance(records, list):
        out: dict[str, dict[str, Any]] = {}
        for row in records:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or row.get("canonical_symbol") or "").upper().strip()
            value = row.get("value")
            if not symbol or value in {None, ""}:
                continue
            valid = str(row.get("validation_status") or "").upper() == "VALID"
            day = str(row.get("day_utc") or payload.get("day_utc") or payload.get("market_session_date") or "")
            out[symbol] = {
                **row,
                "last_price": value,
                "close": value,
                "data_timestamp_utc": row.get("source_timestamp_utc") or row.get("retrieved_at_utc") or row.get("retrieval_timestamp_utc") or "",
                "freshness_status": "CURRENT" if valid else str(row.get("validation_status") or "UNKNOWN").upper(),
                "market_session_date": day,
                "source_url_or_path": row.get("source_path") or row.get("cache_path") or "",
                "source_hash": row.get("raw_source_hash") or row.get("source_hash") or row.get("transformed_value_hash") or "",
            }
        return out
    return {}


def _file_hash(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _candidate_lineage(candidate: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_session_id": str(receipt.get("paper_session_id") or candidate.get("paper_session_id") or ""),
        "candidate_id": str(receipt.get("candidate_id") or candidate.get("candidate_id") or ""),
        "raw_signal_id": str(candidate.get("raw_signal_id") or ""),
        "intent_id": str(candidate.get("intent_id") or ""),
        "sleeve_id": str(candidate.get("sleeve_id") or ""),
        "evidence_paths": candidate.get("evidence_paths") if isinstance(candidate.get("evidence_paths"), list) else [],
        "evidence_hashes": candidate.get("evidence_hashes") if isinstance(candidate.get("evidence_hashes"), dict) else {},
    }


def _event_type_order(event: dict[str, Any]) -> int:
    event_type = str(event.get("event_type") or "").upper()
    if event_type == "PAPER_POSITION_OPENED":
        return 0
    if event_type == "PAPER_POSITION_CLOSED":
        return 1
    if event_type == "PAPER_POSITION_INVALIDATED":
        return 2
    return 9


def _event_id(event: dict[str, Any]) -> str:
    return ":".join(
        [
            str(event.get("event_type") or "PAPER_POSITION_EVENT").lower(),
            str(event.get("position_id") or position_id_for_candidate_v1(str(event.get("candidate_id") or ""))).replace(":", "_"),
            str(event.get("event_time_utc") or now_utc_v1()).replace(":", "").replace("-", ""),
        ]
    )


def _side(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"LONG", "BUY", ""}:
        return "BUY"
    if raw in {"SHORT", "SELL"}:
        return "SELL"
    return raw


def _legacy_receipt(receipt: dict[str, Any]) -> bool:
    receipt_type = str(receipt.get("receipt_type") or "").upper()
    candidate_id = str(receipt.get("candidate_id") or "").strip()
    return receipt_type in {"LEGACY_CAPTURE", "LEGACY"} or not candidate_id or (receipt_type and receipt_type not in {"SIMULATED_PAPER", "AUTO_PROMOTED_RESEARCH_OBSERVATION"})


def _is_legacy_capture(pos: dict[str, Any]) -> bool:
    receipt = pos.get("source_receipt") if isinstance(pos.get("source_receipt"), dict) else {}
    return _legacy_receipt(receipt)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _decimal(value: Any) -> Decimal | None:
    try:
        text = str(value).strip().replace(",", "")
        return Decimal(text) if text else None
    except (InvalidOperation, ValueError):
        return None


def _fmt_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    text = format(value.normalize(), "f")
    return "0" if text in {"-0", "-0.0"} else text.rstrip("0").rstrip(".") if "." in text else text


def _safety() -> dict[str, Any]:
    return {
        "paper_only": True,
        "human_review_required": True,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "live_trade_eligible": False,
    }
