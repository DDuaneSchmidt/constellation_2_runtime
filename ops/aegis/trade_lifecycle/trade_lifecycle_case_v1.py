from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import build_and_write_paper_trade_construction_v1
from ops.aegis.trade_lifecycle.readiness_domain_evaluation_v1 import (
    build_readiness_domain_evaluations_v1,
    blockers_by_domain_v1,
    domain_status_map_v1,
    write_readiness_domain_evaluations_v1,
)
from ops.aegis.trade_lifecycle.trade_lifecycle_event_v1 import append_trade_lifecycle_event_v1, list_trade_lifecycle_events_v1


SCHEMA_ID = "trade_lifecycle_case"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "trade_lifecycle_case_v1"
REPORT_FILENAME = "trade_lifecycle_case.v1.json"

ALLOWED_STATES = {
    "OBSERVED",
    "SELECTED_EXPOSURE",
    "CONSTRUCTION_BLOCKED",
    "CAPTURE_READY",
    "CAPTURED_MANUALLY",
    "CAPTURED_HISTORICAL",
    "SKIPPED",
    "EXPIRED",
    "CLOSED",
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


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


def _source_ref(path: Path, artifact_id: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": exists,
        "sha256": _sha256_file(path) if exists else "",
        "schema_id": str((payload or {}).get("schema_id") or ""),
    }


def trade_lifecycle_case_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _manual_record_path(root: Path, day: str) -> Path:
    return root / "reports" / "manual_capture_record_v1" / day / "manual_capture_record.v1.jsonl"


def _manual_records(root: Path, day: str, selected_id: str) -> list[dict[str, Any]]:
    return [
        row
        for row in _read_jsonl(_manual_record_path(root, day))
        if not selected_id or str(row.get("selected_exposure_intent_id") or "") == selected_id
    ]


def _state_from_readiness(
    construction: Mapping[str, Any],
    records: list[dict[str, Any]],
    domain_statuses: Mapping[str, str],
) -> tuple[str, str, str]:
    if not construction.get("selected_exposure_intent_id"):
        return "OBSERVED", "No current selected exposure is available.", "CASE_OPENED"
    latest = records[-1] if records else {}
    latest_status = str(latest.get("capture_status") or "").lower()
    manual_status = str(domain_statuses.get("manual_capture") or "")
    construction_status = str(construction.get("trade_construction_status") or "")
    if latest_status == "skipped":
        return "SKIPPED", "Operator explicitly marked the case skipped.", "SKIPPED"
    if latest_status in {"captured_manually", "partial"}:
        return "CAPTURED_HISTORICAL", "Immutable manual capture evidence is attached; pre-capture gates are frozen at capture time.", "MANUAL_CAPTURE_RECORDED"
    if manual_status == "READY":
        return "CAPTURE_READY", "ManualCaptureDomain is READY for operator manual paper capture.", "CAPTURE_READY"
    return "CONSTRUCTION_BLOCKED", f"ManualCaptureDomain is {manual_status or 'BLOCKED'}; construction_status={construction_status or 'unknown'}.", "CONSTRUCTION_BLOCKED"

def build_trade_lifecycle_case_v1(
    *,
    truth_root: Path | str,
    day_utc: str | None = None,
    current_operator_truth: Mapping[str, Any] | None = None,
    paper_trade_construction: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
    append_event: bool = False,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    now = generated_at_utc or _now()
    current_truth = dict(current_operator_truth or resolve_current_operator_truth_v1(truth_root=root, day_utc=day_utc, generated_at_utc=now))
    day = str(current_truth.get("source_day") or day_utc or now[:10])
    if paper_trade_construction is None:
        construction, construction_path = build_and_write_paper_trade_construction_v1(
            truth_root=root,
            day_utc=day,
            current_operator_truth=current_truth,
            generated_at_utc=now,
        )
    else:
        construction = dict(paper_trade_construction)
        construction_path = root / "reports" / "paper_trade_construction_v1" / str(construction.get("source_day") or day) / "paper_trade_construction.v1.json"
    selected_id = str(construction.get("selected_exposure_intent_id") or current_truth.get("selected_exposure_intent_id") or "")
    symbol = str(construction.get("symbol") or "").upper()
    direction = str(construction.get("direction") or "")
    sleeve_id = str(construction.get("sleeve_id") or "")
    engine_id = str(construction.get("engine_id") or sleeve_id)
    source_run_id = str(construction.get("source_run_id") or current_truth.get("source_run_id") or "")
    source_day = str(construction.get("source_day") or day)
    case_id = "trade-lifecycle-case:" + _stable_hash(
        {
            "selected_exposure_intent_id": selected_id,
            "symbol": symbol,
            "source_day": source_day,
            "source_run_id": source_run_id,
        }
    )[:24]
    records = _manual_records(root, source_day, selected_id)
    readiness_evaluations = build_readiness_domain_evaluations_v1(
        truth_root=root,
        trade_lifecycle_case_id=case_id,
        selected_exposure_intent_id=selected_id,
        symbol=symbol,
        source_day=source_day,
        paper_trade_construction=construction,
        evaluated_at=now,
    )
    domain_statuses = domain_status_map_v1(readiness_evaluations)
    blockers_by_domain = blockers_by_domain_v1(readiness_evaluations)
    state, reason, event_type = _state_from_readiness(construction, records, domain_statuses)
    existing = read_trade_lifecycle_case_v1(truth_root=root, day_utc=source_day)
    created_at = str(existing.get("created_at") or now)
    existing_events = list_trade_lifecycle_events_v1(truth_root=root, day_utc=source_day, trade_lifecycle_case_id=case_id)
    blocker_codes = [str(code) for code in construction.get("blocker_codes") or []]
    blocker_messages = [str(message) for message in construction.get("blocker_messages") or []]
    source_artifacts = list(construction.get("source_artifacts") if isinstance(construction.get("source_artifacts"), list) else [])
    source_artifacts.append(_source_ref(construction_path, "paper_trade_construction_v1", construction))
    event_ids = [str(row.get("event_id") or "") for row in existing_events if row.get("event_id")]
    if append_event:
        event = append_trade_lifecycle_event_v1(
            truth_root=root,
            day_utc=source_day,
            trade_lifecycle_case_id=case_id,
            event_type=event_type,
            state=state,
            state_reason=reason,
            paper_trade_construction_id=str(construction.get("construction_id") or ""),
            manual_capture_record_id=str(records[-1].get("record_id") or "") if records else "",
            blocker_codes=blocker_codes,
            source_artifacts=source_artifacts,
            generated_at_utc=now,
        )
        event_ids.append(str(event.get("event_id") or ""))
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "trade_lifecycle_case_id": case_id,
        "selected_exposure_intent_id": selected_id,
        "symbol": symbol,
        "direction": direction,
        "sleeve_id": sleeve_id,
        "engine_id": engine_id,
        "source_day": source_day,
        "source_run_id": source_run_id,
        "current_state": state,
        "state_reason": reason,
        "paper_trade_construction_id": str(construction.get("construction_id") or ""),
        "manual_capture_candidate_id": f"manual-capture-candidate:{case_id.rsplit(':', 1)[-1]}" if state == "CAPTURE_READY" else "",
        "manual_capture_record_ids": [str(row.get("record_id") or "") for row in records if row.get("record_id")],
        "manual_capture_records": records,
        "lifecycle_event_ids": event_ids,
        "readiness_domain_evaluation_ids": [str(row.get("readiness_domain_evaluation_id") or "") for row in readiness_evaluations if row.get("readiness_domain_evaluation_id")],
        "readiness_domain_evaluations": readiness_evaluations,
        "domain_statuses": domain_statuses,
        "blockers_by_domain": blockers_by_domain,
        "aggregate_case_status": state,
        "blocker_codes": blocker_codes,
        "blocker_messages": blocker_messages,
        "source_artifacts": source_artifacts,
        "paper_trade_construction": dict(construction),
        "created_at": created_at,
        "updated_at": now,
        "manual_capture_ready": domain_statuses.get("manual_capture") == "READY" and state not in {"CAPTURED_MANUALLY", "CAPTURED_HISTORICAL"},
        "paper_submit_ready": domain_statuses.get("paper_submit") == "READY" and state not in {"CAPTURED_MANUALLY", "CAPTURED_HISTORICAL"},
        "execution_ready": domain_statuses.get("execution") == "READY" and state not in {"CAPTURED_MANUALLY", "CAPTURED_HISTORICAL"},
        "captured_historical": state == "CAPTURED_HISTORICAL",
        "submit_boundary_status": str(construction.get("submit_boundary_status") or ""),
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "paper_submit_created": False,
        "selected_candidate_mutation_allowed": False,
    }
    payload["immutable_hash"] = _stable_hash({**payload, "immutable_hash": ""})
    return payload


def write_trade_lifecycle_case_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    path = trade_lifecycle_case_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    return path


def build_and_write_trade_lifecycle_case_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    current_operator_truth: Mapping[str, Any] | None = None,
    paper_trade_construction: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
    append_event: bool = False,
) -> tuple[dict[str, Any], Path]:
    payload = build_trade_lifecycle_case_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        current_operator_truth=current_operator_truth,
        paper_trade_construction=paper_trade_construction,
        generated_at_utc=generated_at_utc,
        append_event=append_event,
    )
    root = Path(truth_root).expanduser().resolve()
    source_day = str(payload.get("source_day") or day_utc)
    path = write_trade_lifecycle_case_v1(truth_root=root, day_utc=source_day, payload=payload)
    evaluations = payload.get("readiness_domain_evaluations") if isinstance(payload.get("readiness_domain_evaluations"), list) else []
    write_readiness_domain_evaluations_v1(truth_root=root, day_utc=source_day, evaluations=evaluations)
    return {**payload, "artifact_path": str(path)}, path


def read_trade_lifecycle_case_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return _read_json(trade_lifecycle_case_path_v1(truth_root=truth_root, day_utc=day_utc))


def latest_trade_lifecycle_case_v1(*, truth_root: Path | str, day_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc or "")
    if not day:
        base = root / "reports" / REPORT_FAMILY
        days = sorted(path.name for path in base.iterdir() if path.is_dir()) if base.exists() else []
        day = days[-1] if days else _now()[:10]
    payload = read_trade_lifecycle_case_v1(truth_root=root, day_utc=day)
    if payload:
        payload["artifact_path"] = str(trade_lifecycle_case_path_v1(truth_root=root, day_utc=day))
        return payload
    payload, _path = build_and_write_trade_lifecycle_case_v1(truth_root=root, day_utc=day)
    return payload
