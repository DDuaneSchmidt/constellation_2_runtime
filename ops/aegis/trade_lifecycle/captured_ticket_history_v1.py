from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


HISTORY_SCHEMA_ID = "captured_ticket_history"
PROJECTION_SCHEMA_ID = "captured_ticket_projection"
SCHEMA_VERSION = "v1"
HISTORY_FAMILY = "captured_ticket_history_v1"
PROJECTION_FAMILY = "captured_ticket_projection_v1"

CAPTURED_STATUSES = {"captured_manually", "partial"}


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def manual_capture_record_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "manual_capture_record_v1" / str(day_utc) / "manual_capture_record.v1.jsonl"


def captured_ticket_history_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    safe_ticket = str(ticket_id or "NO_TICKET").replace(":", "_").replace("/", "_")
    return Path(truth_root).expanduser().resolve() / "reports" / HISTORY_FAMILY / str(day_utc) / safe_ticket / "captured_ticket_history.v1.json"


def captured_ticket_projection_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    safe_ticket = str(ticket_id or "NO_TICKET").replace(":", "_").replace("/", "_")
    return Path(truth_root).expanduser().resolve() / "reports" / PROJECTION_FAMILY / str(day_utc) / safe_ticket / "captured_ticket_projection.v1.json"


def _event_log_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "events" / "aegis_evidence_events_v1" / str(day_utc) / "events.jsonl"


def _manual_capture_event_ids_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str, record: Mapping[str, Any]) -> list[str]:
    rows = _read_jsonl(_event_log_path_v1(truth_root=truth_root, day_utc=day_utc))
    matched: list[str] = []
    for row in rows:
        if str(row.get("event_type") or "") != "ManualActionAccepted":
            continue
        if ticket_id and str(row.get("parent_run_id") or "") != str(ticket_id):
            continue
        event_id = str(row.get("event_id") or "")
        if event_id:
            matched.append(event_id)
    return matched[-1:] if matched else []


def list_capture_records_for_ticket_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str = "", selected_exposure_intent_id: str = "") -> list[dict[str, Any]]:
    rows = _read_jsonl(manual_capture_record_path_v1(truth_root=truth_root, day_utc=day_utc))
    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("capture_status") or "").lower() not in CAPTURED_STATUSES:
            continue
        if ticket_id and str(row.get("ticket_id") or "") != str(ticket_id):
            continue
        if selected_exposure_intent_id and str(row.get("selected_exposure_intent_id") or "") != str(selected_exposure_intent_id):
            continue
        out.append(row)
    return out


def latest_capture_record_for_ticket_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str = "", selected_exposure_intent_id: str = "") -> dict[str, Any]:
    records = list_capture_records_for_ticket_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        ticket_id=ticket_id,
        selected_exposure_intent_id=selected_exposure_intent_id,
    )
    return records[-1] if records else {}


def build_captured_ticket_history_v1(*, truth_root: Path | str, day_utc: str, capture_record: Mapping[str, Any]) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    record = dict(capture_record)
    original_lineage = record.get("trade_ticket_lineage") if isinstance(record.get("trade_ticket_lineage"), Mapping) else {}
    ticket_id = str(record.get("ticket_id") or original_lineage.get("ticket_id") or "")
    record_id = str(record.get("record_id") or record.get("manual_capture_record_id") or "")
    event_ids = [str(item) for item in record.get("event_ids") or [] if str(item)]
    if not event_ids:
        event_ids = _manual_capture_event_ids_v1(truth_root=root, day_utc=day_utc, ticket_id=ticket_id, record=record)
    captured_at = str(record.get("fill_time") or record.get("created_at_utc") or "")
    created_at = str(record.get("created_at_utc") or captured_at)
    source_paths = [str(path) for path in original_lineage.get("source_artifact_paths") or [] if str(path)]
    record_path = manual_capture_record_path_v1(truth_root=root, day_utc=day_utc)
    source_paths.append(str(record_path))
    payload: dict[str, Any] = {
        "schema_id": HISTORY_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "ticket_id": ticket_id,
        "day_utc": str(day_utc),
        "historical_state": "CAPTURED_HISTORICAL",
        "lifecycle_state": "CAPTURED_HISTORICAL",
        "capture_record_id": record_id,
        "event_ids": event_ids,
        "captured_at_utc": captured_at,
        "record_created_at_utc": created_at,
        "operator_id": str(record.get("operator_id") or ""),
        "selected_exposure_intent_id": str(record.get("selected_exposure_intent_id") or original_lineage.get("exposure_id") or ""),
        "symbol": str(record.get("symbol") or original_lineage.get("symbol") or "").upper(),
        "side": str(original_lineage.get("side") or record.get("direction") or ""),
        "sleeve_id": str(record.get("sleeve") or original_lineage.get("sleeve_id") or ""),
        "quantity": record.get("quantity"),
        "fill_price": str(record.get("fill_price") or ""),
        "fill_time": str(record.get("fill_time") or ""),
        "stop_price": str(record.get("stop_price") or ""),
        "notes": str(record.get("notes") or ""),
        "original_runtime_evaluation_hash": str(original_lineage.get("runtime_evaluation_hash") or record.get("runtime_evaluation_hash") or ""),
        "original_lineage_hash": str(original_lineage.get("lineage_hash") or record.get("ticket_lineage_hash") or ""),
        "original_submit_boundary_hash": str(original_lineage.get("submit_boundary_hash") or record.get("submit_boundary_hash") or ""),
        "original_construction_contract_hash": str(original_lineage.get("construction_contract_hash") or record.get("construction_contract_hash") or ""),
        "original_paper_trade_construction_id": str(original_lineage.get("paper_trade_construction_id") or record.get("paper_trade_construction_id") or ""),
        "capture_time_lineage_status": str(record.get("lineage_status") or original_lineage.get("lineage_status") or ""),
        "capture_time_submit_boundary_status": str(record.get("submit_boundary_status") or (original_lineage.get("evidence_status") or {}).get("submit_boundary") if isinstance(original_lineage.get("evidence_status"), Mapping) else ""),
        "source_artifact_paths": sorted(set(source_paths)),
        "source_event_ids": sorted(set([*event_ids, *[str(item) for item in original_lineage.get("source_event_ids") or [] if str(item)]])),
        "append_integrity_status": "VALID" if record_id and ticket_id else "INVALID",
        "replay_status": "REPLAYABLE",
        "post_capture_authority": "IMMUTABLE_CAPTURE_EVIDENCE",
        "pre_capture_authority_frozen": True,
        "current_runtime_revalidation_required": False,
        "submit_boundary_revalidation_required": False,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "paper_submit_created": False,
    }
    payload["artifact_id"] = f"{HISTORY_FAMILY}:{day_utc}:{stable_hash_v1(payload)[:20]}"
    payload["history_hash"] = stable_hash_v1({**payload, "history_hash": ""})
    return payload


def captured_ticket_projection_v1(history: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_id": PROJECTION_SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "ticket_id": str(history.get("ticket_id") or ""),
        "day_utc": str(history.get("day_utc") or ""),
        "historical_state": "CAPTURED_HISTORICAL",
        "lifecycle_state": "CAPTURED_HISTORICAL",
        "current_state": "CAPTURED_HISTORICAL",
        "capture_status": "Manual capture recorded",
        "capture_record_id": str(history.get("capture_record_id") or ""),
        "event_ids": [str(item) for item in history.get("event_ids") or [] if str(item)],
        "captured_at_utc": str(history.get("captured_at_utc") or ""),
        "operator_id": str(history.get("operator_id") or ""),
        "symbol": str(history.get("symbol") or "").upper(),
        "side": str(history.get("side") or ""),
        "sleeve_id": str(history.get("sleeve_id") or ""),
        "quantity": history.get("quantity"),
        "fill_price": str(history.get("fill_price") or ""),
        "fill_time": str(history.get("fill_time") or ""),
        "stop_price": str(history.get("stop_price") or ""),
        "original_runtime_evaluation_hash": str(history.get("original_runtime_evaluation_hash") or ""),
        "original_lineage_hash": str(history.get("original_lineage_hash") or ""),
        "original_submit_boundary_hash": str(history.get("original_submit_boundary_hash") or ""),
        "original_construction_contract_hash": str(history.get("original_construction_contract_hash") or ""),
        "append_integrity_status": str(history.get("append_integrity_status") or ""),
        "replay_status": str(history.get("replay_status") or ""),
        "editable": False,
        "read_only": True,
        "historical_read_only": True,
        "capture_editing_disabled": True,
        "manual_capture_ready": False,
        "capture_save_ready": False,
        "submit_boundary_status": "CAPTURE_TIME_VALIDATED",
        "lineage_status": "CAPTURED_HISTORICAL",
        "status_message": "Manual capture recorded. No broker action was taken.",
        "operator_message": "Read-only historical record.",
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "paper_submit_created": False,
    }
    payload["artifact_id"] = f"{PROJECTION_FAMILY}:{payload['day_utc']}:{stable_hash_v1(payload)[:20]}"
    payload["projection_hash"] = stable_hash_v1({**payload, "projection_hash": ""})
    return payload


def write_captured_ticket_history_v1(*, truth_root: Path | str, payload: Mapping[str, Any]) -> Path:
    path = captured_ticket_history_path_v1(truth_root=truth_root, day_utc=str(payload.get("day_utc") or ""), ticket_id=str(payload.get("ticket_id") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    return path


def write_captured_ticket_projection_v1(*, truth_root: Path | str, payload: Mapping[str, Any]) -> Path:
    path = captured_ticket_projection_path_v1(truth_root=truth_root, day_utc=str(payload.get("day_utc") or ""), ticket_id=str(payload.get("ticket_id") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    return path

