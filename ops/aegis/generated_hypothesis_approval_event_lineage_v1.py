from __future__ import annotations

import json
from datetime import UTC, datetime
import re
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.hypothesis_proposal_promotion_v1 import approval_events_path_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_generated_hypothesis_approval_event_lineage_v1"
FILENAME = "generated_hypothesis_approval_event_lineage.v1.json"
OIL_SHOCK_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_SHOCK_NAME = "Oil shock reversals across energy ETFs"
SAFETY = {
    "research_only": True,
    "lineage_only": True,
    "approval_created_by_this_artifact": False,
    "raw_signal_created_by_this_artifact": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "outcome_created_by_this_artifact": False,
    "trade_created_by_this_artifact": False,
    "allocation_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "safety_gates_changed": False,
}


def generated_hypothesis_approval_event_lineage_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, str(day_utc), FILENAME)


def build_generated_hypothesis_approval_event_lineage_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _paths(root, day)
    discovery_paths = _discovery_paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in paths.items() if path.suffix != ".jsonl"}
    event, source_type, source_path, source_day = _find_event(payloads, paths, discovery_paths, day)
    status, reason_codes = _classify(event=event, payloads=payloads, paths=paths, source_type=source_type, source_day=source_day, day=day)
    event_hash = _event_hash(event) if event else ""
    source_hash = file_hash_v1(source_path) if source_path else ""
    row = {
        "hypothesis_id": OIL_SHOCK_HYPOTHESIS_ID,
        "hypothesis_name": OIL_SHOCK_NAME,
        "approval_lineage_status": status,
        "approval_event_found": status == "APPROVAL_EVENT_FOUND",
        "approval_event": event if status == "APPROVAL_EVENT_FOUND" else {},
        "approval_event_hash": event_hash if status == "APPROVAL_EVENT_FOUND" else "",
        "approval_source_type": source_type,
        "approval_source_path": str(source_path) if source_path else "",
        "approval_source_hash": source_hash,
        "approval_actor": text_v1(event.get("actor") or event.get("approval_actor")) if event else "",
        "approval_timestamp_utc": text_v1(event.get("generated_at_utc") or event.get("event_timestamp_utc") or event.get("approval_timestamp_utc")) if event else "",
        "approval_prior_state": text_v1(event.get("prior_state") or event.get("approval_prior_state")) if event else "",
        "approval_new_state": text_v1(event.get("new_state") or event.get("approval_state") or event.get("approval_new_state")) if event else "",
        "target_day": day,
        "source_target_day": source_day or (text_v1(event.get("day_utc") or event.get("target_day")) if event else ""),
        "reason_codes": reason_codes,
        "david_action_required": False,
        "source_artifact_paths": [str(path) for path in discovery_paths],
        "source_artifact_hashes": {str(path): file_hash_v1(path) for path in discovery_paths},
        "computed_at_utc": _now(),
        **SAFETY,
        "safety": dict(SAFETY),
    }
    artifact = {
        "schema_id": "aegis_generated_hypothesis_approval_event_lineage",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": day,
        "approval_lineage_status": row["approval_lineage_status"],
        "hypotheses": [row],
        "oil_shock": row,
        **row,
        "summary": {
            "approval_lineage_status": row["approval_lineage_status"],
            "approval_event_found": row["approval_event_found"],
            "approval_event_hash_present": bool(row["approval_event_hash"]),
            "reason_codes": row["reason_codes"],
        },
        "computed_at_utc": _now(),
    }
    artifact["content_hash"] = stable_hash_v1(_without_generated(artifact))
    return artifact


def write_generated_hypothesis_approval_event_lineage_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_approval_event_lineage_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_approval_event_lineage_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "approval_queue": report_path_v1(root, "aegis_paper_promotion_approval_queue_v1", day, "approval_queue.v1.json"),
        "approval_events": approval_events_path_v1(truth_root=root, day_utc=day),
        "operator_action_events": report_path_v1(root, "aegis_operator_action_event_log_v1", day, "operator_action_events.v1.jsonl"),
        "operator_action_queue": report_path_v1(root, "aegis_operator_action_queue_v1", day, "operator_action_queue.v1.json"),
        "promotion_packets": report_path_v1(root, "aegis_hypothesis_promotion_packet_v1", day, "promotion_packets.v1.json"),
        "workflow_state": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
        "workflow_replay": report_path_v1(root, "aegis_hypothesis_workflow_replay_verification_v1", day, "hypothesis_workflow_replay_verification.v1.json"),
        "paper_setup": report_path_v1(root, "aegis_approved_hypothesis_paper_tracking_setup_v1", day, "approved_hypothesis_paper_tracking_setup.v1.json"),
        "paper_blueprint": report_path_v1(root, "aegis_paper_sleeve_blueprint_v1", day, "paper_sleeve_blueprint.v1.json"),
    }


def _discovery_paths(root: Path, day: str) -> list[Path]:
    paths = list(_paths(root, day).values())
    reports = root / "reports"
    for family, filename in (
        ("aegis_paper_promotion_approval_queue_v1", "approval_events.v1.jsonl"),
        ("aegis_operator_action_event_log_v1", "operator_action_events.v1.jsonl"),
        ("aegis_paper_promotion_approval_queue_v1", "approval_queue.v1.json"),
        ("aegis_hypothesis_promotion_packet_v1", "promotion_packets.v1.json"),
        ("aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"),
        ("aegis_hypothesis_workflow_replay_verification_v1", "hypothesis_workflow_replay_verification.v1.json"),
    ):
        base = reports / family
        if not base.exists():
            continue
        for child in sorted(base.iterdir()):
            if child.is_dir() and child.name <= day:
                paths.append(child / filename)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return unique


def _find_event(payloads: Mapping[str, Any], paths: Mapping[str, Path], discovery_paths: list[Path], day: str) -> tuple[dict[str, Any], str, Path | None, str]:
    current_jsonl_rows = _read_jsonl(paths["approval_events"])
    current_event = _first_matching_approved_event(current_jsonl_rows)
    if current_event:
        return current_event, "approval_event_jsonl", paths["approval_events"], text_v1(current_event.get("day_utc") or current_event.get("target_day") or day)

    current_operator_rows = _read_jsonl(paths["operator_action_events"])
    current_operator = _first_matching_approved_event(current_operator_rows)
    if current_operator:
        event = _canonicalize_operator_event(current_operator, day)
        return event, "operator_action_event_log", paths["operator_action_events"], text_v1(current_operator.get("day_utc") or current_operator.get("target_day") or day)

    historical: list[tuple[dict[str, Any], str, Path, str]] = []
    for path in discovery_paths:
        if path == paths["approval_events"] or path == paths["operator_action_events"]:
            continue
        source_day = _day_from_path(path)
        if source_day and source_day > day:
            continue
        if path.name == "approval_events.v1.jsonl":
            event = _first_matching_approved_event(_read_jsonl(path))
            if event:
                historical.append((event, "approval_event_jsonl_historical", path, text_v1(event.get("day_utc") or event.get("target_day") or source_day)))
        elif path.name == "operator_action_events.v1.jsonl":
            event = _first_matching_approved_event(_read_jsonl(path))
            if event:
                historical.append((_canonicalize_operator_event(event, source_day or day), "operator_action_event_log_historical", path, text_v1(event.get("day_utc") or event.get("target_day") or source_day)))
    if historical:
        historical.sort(key=lambda item: (item[3], str(item[2])), reverse=True)
        return historical[0]

    queue_item = _find_oil(payloads.get("approval_queue", {}).get("approval_queue"))
    latest = queue_item.get("latest_approval_event") if isinstance(queue_item.get("latest_approval_event"), Mapping) else {}
    if latest and _is_approved(latest) and text_v1(latest.get("event_hash") or latest.get("approval_event_hash")):
        return dict(latest), "approval_queue_latest_event", paths["approval_queue"], text_v1(latest.get("day_utc") or latest.get("target_day") or day)
    for key in ("paper_setup", "paper_blueprint"):
        row = _find_oil(payloads.get(key, {}).get("paper_tracking_setups") or payloads.get(key, {}).get("paper_sleeve_blueprints"))
        if row.get("approval_event_hash") and text_v1(row.get("day_utc") or day) == day:
            event = {"hypothesis_id": OIL_SHOCK_HYPOTHESIS_ID, "event_hash": row.get("approval_event_hash"), "day_utc": day, "new_state": "PAPER_PROMOTION_APPROVED"}
            return event, key, paths[key], day
    return {}, "", None, ""


def _classify(*, event: Mapping[str, Any], payloads: Mapping[str, Any], paths: Mapping[str, Path], source_type: str, source_day: str, day: str) -> tuple[str, list[str]]:
    queue_item = _find_oil(payloads.get("approval_queue", {}).get("approval_queue"))
    if event:
        if source_type in {"approval_event_jsonl", "operator_action_event_log"} and text_v1(event.get("day_utc") or event.get("target_day") or day) != day:
            return "TARGET_DAY_MISMATCH", ["TARGET_DAY_MISMATCH"]
        if not _schema_ok(event):
            return "APPROVAL_EVENT_SCHEMA_MISMATCH", ["APPROVAL_EVENT_SCHEMA_MISMATCH"]
        if not text_v1(event.get("event_hash") or event.get("approval_event_hash")):
            return "APPROVAL_EVENT_NOT_HASHED", ["APPROVAL_EVENT_NOT_HASHED"]
        reasons = ["APPROVAL_EVENT_FOUND"]
        if source_type.endswith("_historical") or (source_day and source_day != day):
            reasons.append("HISTORICAL_APPROVAL_EVENT_REUSED")
        if source_type.startswith("operator_action_event_log"):
            reasons.append("UI_OPERATOR_ACTION_EVENT_NORMALIZED")
        return "APPROVAL_EVENT_FOUND", reasons
    if queue_item.get("approval_state") == "PAPER_PROMOTION_APPROVED":
        return "APPROVAL_QUEUE_STATE_ONLY_NO_EVENT", ["APPROVAL_QUEUE_STATE_ONLY_NO_EVENT"]
    if not paths["approval_events"].exists():
        return "APPROVAL_EVENT_MISSING", ["APPROVAL_EVENT_MISSING", "APPROVAL_EVENT_JSONL_MISSING"]
    return "APPROVAL_EVENT_MISSING", ["APPROVAL_EVENT_MISSING"]


def _first_matching_approved_event(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if _is_oil(row) and _is_approved(row):
            return dict(row)
    return {}


def _canonicalize_operator_event(row: Mapping[str, Any], day: str) -> dict[str, Any]:
    event = dict(row)
    event["schema_id"] = "aegis_paper_promotion_approval_event_v1"
    event["schema_version"] = "v1"
    event.setdefault("day_utc", text_v1(row.get("day_utc") or row.get("target_day") or day))
    event.setdefault("generated_at_utc", text_v1(row.get("event_timestamp_utc") or row.get("created_at_utc") or row.get("approval_timestamp_utc")))
    event.setdefault("hypothesis_id", text_v1(row.get("hypothesis_id") or row.get("proposal_id") or row.get("hypothesis_proposal_id")))
    event.setdefault("proposal_id", text_v1(row.get("proposal_id") or row.get("hypothesis_proposal_id") or row.get("hypothesis_id")))
    event.setdefault("approval_action", text_v1(row.get("approval_action") or row.get("action_type") or row.get("button_clicked")))
    event.setdefault("approval_decision", "APPROVED")
    event.setdefault("actor", text_v1(row.get("actor") or row.get("created_by") or "David / operator"))
    event.setdefault("prior_state", text_v1(row.get("prior_state")))
    event.setdefault("new_state", text_v1(row.get("new_state") or row.get("approval_state") or "PAPER_PROMOTION_APPROVED"))
    event.setdefault("event_hash", text_v1(row.get("event_hash") or row.get("approval_event_hash")) or stable_hash_v1({**dict(event), "event_hash": ""}))
    return event


def _day_from_path(path: Path) -> str:
    for part in reversed(path.parts):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part):
            return part
    return ""

def _event_hash(event: Mapping[str, Any]) -> str:
    existing = text_v1(event.get("event_hash") or event.get("approval_event_hash"))
    if existing:
        return existing
    return stable_hash_v1({**dict(event), "event_hash": ""}) if event else ""


def _schema_ok(event: Mapping[str, Any]) -> bool:
    if text_v1(event.get("schema_id")) and text_v1(event.get("schema_id")) != "aegis_paper_promotion_approval_event_v1":
        return False
    return bool(text_v1(event.get("hypothesis_id") or event.get("proposal_id")) and (text_v1(event.get("new_state") or event.get("approval_state")) == "PAPER_PROMOTION_APPROVED" or text_v1(event.get("approval_decision")) == "APPROVED"))


def _is_approved(row: Mapping[str, Any]) -> bool:
    return text_v1(row.get("new_state") or row.get("approval_state")) == "PAPER_PROMOTION_APPROVED" or text_v1(row.get("approval_decision")) == "APPROVED"


def _is_oil(row: Mapping[str, Any]) -> bool:
    text = " ".join(text_v1(row.get(key)) for key in ("hypothesis_id", "proposal_id", "hypothesis_proposal_id", "hypothesis_name"))
    return OIL_SHOCK_HYPOTHESIS_ID in text or "Oil shock reversals" in text


def _find_oil(rows: Any) -> dict[str, Any]:
    for row in rows or []:
        if isinstance(row, Mapping) and _is_oil(row):
            return dict(row)
    return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            rows.append({"schema_id": "INVALID_JSONL_ROW", "raw_line": line})
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _without_generated(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _without_generated(item) for key, item in value.items() if key not in {"computed_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
