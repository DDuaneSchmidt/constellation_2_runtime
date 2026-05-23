from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1

SCHEMA_ID = "readiness_domain_evaluation"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "readiness_domain_evaluation_v1"
REPORT_FILENAME = "readiness_domain_evaluation.v1.json"

ALLOWED_DOMAINS = {
    "market_data",
    "trade_construction",
    "capital_authority",
    "stop_risk",
    "manual_capture",
    "paper_submit",
    "execution",
}

READY = "READY"
BLOCKED = "BLOCKED"
PARTIAL = "PARTIAL"
DEGRADED = "DEGRADED"
NOT_REQUIRED = "NOT_REQUIRED"
NOT_ENABLED = "NOT_ENABLED"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _sha256_file(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
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


def _source_ref(path: Path | None, artifact_id: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    exists = bool(path and path.exists() and path.is_file())
    return {
        "artifact_id": artifact_id,
        "path": str(path or ""),
        "exists": exists,
        "sha256": _sha256_file(path) if exists else "",
        "schema_id": str((payload or {}).get("schema_id") or ""),
    }


def readiness_domain_evaluation_dir_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc)


def readiness_domain_evaluation_path_v1(*, truth_root: Path | str, day_utc: str, domain: str) -> Path:
    return readiness_domain_evaluation_dir_v1(truth_root=truth_root, day_utc=day_utc) / f"{domain}.{REPORT_FILENAME}"


def _truth_submit_path(root: Path, day: str) -> Path:
    return root / "reports" / "submit_boundary_status_v1" / day / "submit_boundary_status.v1.json"


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _has_positive_quantity(value: Any) -> bool:
    try:
        return int(value) > 0
    except Exception:
        return False


def _blocker_messages(construction: Mapping[str, Any], codes: list[str]) -> list[str]:
    source_codes = [str(code) for code in construction.get("blocker_codes") or []]
    source_messages = [str(message) for message in construction.get("blocker_messages") or []]
    by_code = {code: source_messages[index] for index, code in enumerate(source_codes) if index < len(source_messages)}
    return [by_code.get(code, code) for code in codes]


def _dedupe_with_messages(codes: list[str], messages: list[str]) -> tuple[list[str], list[str]]:
    out_codes: list[str] = []
    out_messages: list[str] = []
    seen: set[str] = set()
    for index, code in enumerate(codes):
        text = str(code or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out_codes.append(text)
        out_messages.append(str(messages[index]) if index < len(messages) else text)
    return out_codes, out_messages


def _eval(
    *,
    case_id: str,
    selected_id: str,
    symbol: str,
    source_day: str,
    domain: str,
    status: str,
    required_inputs: list[str],
    resolved_inputs: list[str],
    missing_inputs: list[str],
    blocker_codes: list[str],
    blocker_messages: list[str],
    allowed_actions: list[str],
    disallowed_actions: list[str],
    source_artifacts: list[dict[str, Any]],
    evaluated_at: str,
) -> dict[str, Any]:
    normalized_codes, normalized_messages = _dedupe_with_messages(blocker_codes, blocker_messages)
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "readiness_domain_evaluation_id": "",
        "trade_lifecycle_case_id": case_id,
        "selected_exposure_intent_id": selected_id,
        "symbol": symbol,
        "source_day": source_day,
        "domain": domain,
        "status": status,
        "required_inputs": required_inputs,
        "resolved_inputs": resolved_inputs,
        "missing_inputs": missing_inputs,
        "blocker_codes": normalized_codes,
        "blocker_messages": normalized_messages,
        "allowed_actions": allowed_actions,
        "disallowed_actions": disallowed_actions,
        "source_artifacts": source_artifacts,
        "evaluated_at": evaluated_at,
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "paper_submit_created": False,
    }
    payload["readiness_domain_evaluation_id"] = f"readiness-domain-evaluation:{domain}:{_stable_hash(payload)[:20]}"
    payload["immutable_hash"] = _stable_hash({**payload, "immutable_hash": ""})
    return payload


def build_readiness_domain_evaluations_v1(
    *,
    truth_root: Path | str,
    trade_lifecycle_case_id: str,
    selected_exposure_intent_id: str,
    symbol: str,
    source_day: str,
    paper_trade_construction: Mapping[str, Any],
    evaluated_at: str | None = None,
) -> list[dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    now = evaluated_at or _now()
    construction = dict(paper_trade_construction)
    construction_sources = list(construction.get("source_artifacts") if isinstance(construction.get("source_artifacts"), list) else [])
    submit_path = _truth_submit_path(root, source_day)
    submit = _read_json(submit_path)
    submit_sources = [*construction_sources, _source_ref(submit_path, "submit_boundary_status_v1", submit)]

    entry_ready = _has_value(construction.get("entry_reference_price"))
    qty_ready = _has_positive_quantity(construction.get("suggested_quantity"))
    notional_ready = _has_value(construction.get("suggested_notional"))
    stop_ready = _has_value(construction.get("stop_price")) or _has_value(construction.get("invalidation_level"))
    risk_ready = _has_value(construction.get("risk_per_share")) and _has_value(construction.get("max_loss_estimate"))
    market_ready = bool(symbol and construction.get("market_data_latest_session") == source_day and entry_ready)

    evaluations: list[dict[str, Any]] = []

    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="market_data",
        status=READY if market_ready else BLOCKED,
        required_inputs=["market_data_snapshot_v1", "entry_reference_price", "market_data_latest_session"],
        resolved_inputs=[name for name, ready in [("market_data_snapshot_v1", bool(construction.get("market_data_snapshot_id"))), ("entry_reference_price", entry_ready), ("market_data_latest_session", construction.get("market_data_latest_session") == source_day)] if ready],
        missing_inputs=[] if market_ready else [name for name, ready in [("current_market_data", construction.get("market_data_latest_session") == source_day), ("entry_reference_price", entry_ready)] if not ready],
        blocker_codes=[] if market_ready else [code for code in ["MISSING_CURRENT_MARKET_DATA" if construction.get("market_data_latest_session") != source_day else "", "MISSING_ENTRY_REFERENCE_PRICE" if not entry_ready else ""] if code],
        blocker_messages=[] if market_ready else _blocker_messages(construction, [code for code in ["MISSING_CURRENT_MARKET_DATA" if construction.get("market_data_latest_session") != source_day else "", "MISSING_ENTRY_REFERENCE_PRICE" if not entry_ready else ""] if code]),
        allowed_actions=["review", "blocked_record", "skipped_record"] if market_ready else ["review", "blocked_record"],
        disallowed_actions=[] if market_ready else ["captured_manually", "paper_submit", "broker_execution"],
        source_artifacts=construction_sources,
        evaluated_at=now,
    ))

    construction_required = ["entry_reference_price", "suggested_quantity", "suggested_notional", "stop_or_invalidation", "risk_estimate"]
    construction_resolved = [name for name, ready in [("entry_reference_price", entry_ready), ("suggested_quantity", qty_ready), ("suggested_notional", notional_ready), ("stop_or_invalidation", stop_ready), ("risk_estimate", risk_ready)] if ready]
    construction_missing = [name for name in construction_required if name not in construction_resolved]
    if not construction_missing:
        construction_status = READY
    elif construction_resolved:
        construction_status = PARTIAL
    else:
        construction_status = BLOCKED
    construction_codes = []
    if not entry_ready:
        construction_codes.append("MISSING_ENTRY_REFERENCE_PRICE")
    if not qty_ready:
        construction_codes.append("MISSING_SUGGESTED_QUANTITY")
    if not notional_ready:
        construction_codes.append("INCOMPLETE_TRADE_DEFINITION")
    if not stop_ready:
        construction_codes.append("MISSING_STOP_OR_INVALIDATION_LEVEL")
    if not risk_ready:
        construction_codes.append("MISSING_RISK_ESTIMATE")
    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="trade_construction",
        status=construction_status,
        required_inputs=construction_required,
        resolved_inputs=construction_resolved,
        missing_inputs=construction_missing,
        blocker_codes=construction_codes,
        blocker_messages=_blocker_messages(construction, construction_codes),
        allowed_actions=["review", "not_captured_record", "skipped_record", "blocked_record"],
        disallowed_actions=[] if construction_status == READY else ["captured_manually", "paper_submit", "broker_execution"],
        source_artifacts=construction_sources,
        evaluated_at=now,
    ))

    cap_codes = [code for code in construction.get("blocker_codes") or [] if str(code).startswith("CAPITAL_") or str(code) == "MISSING_CAPITAL_AUTHORITY"]
    cap_ready = bool(construction.get("capital_authority_source")) and not cap_codes and qty_ready
    cap_status = READY if cap_ready else (BLOCKED if cap_codes or not qty_ready else NOT_REQUIRED)
    cap_missing = [] if cap_ready else (["paper_capital_headroom", "authorized_quantity"] if cap_status == BLOCKED else [])
    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="capital_authority",
        status=cap_status,
        required_inputs=["paper_capital_authority", "capital_headroom", "authorized_quantity"],
        resolved_inputs=[name for name, ready in [("paper_capital_authority", bool(construction.get("capital_authority_source"))), ("authorized_quantity", qty_ready)] if ready],
        missing_inputs=cap_missing,
        blocker_codes=[str(code) for code in cap_codes] or ([] if cap_status == READY else ["MISSING_SUGGESTED_QUANTITY"]),
        blocker_messages=_blocker_messages(construction, [str(code) for code in cap_codes] or ([] if cap_status == READY else ["MISSING_SUGGESTED_QUANTITY"])),
        allowed_actions=["review", "not_captured_record", "skipped_record", "blocked_record"],
        disallowed_actions=[] if cap_status in {READY, NOT_REQUIRED} else ["paper_submit", "broker_execution"],
        source_artifacts=construction_sources,
        evaluated_at=now,
    ))

    stop_codes = []
    if not stop_ready:
        stop_codes.extend(["MISSING_STOP_POLICY", "MISSING_STOP_OR_INVALIDATION_LEVEL"])
    if not risk_ready:
        stop_codes.append("MISSING_RISK_ESTIMATE")
    stop_status = READY if stop_ready and risk_ready else BLOCKED
    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="stop_risk",
        status=stop_status,
        required_inputs=["stop_price_or_invalidation_level", "risk_per_share", "max_loss_estimate"],
        resolved_inputs=[name for name, ready in [("stop_price_or_invalidation_level", stop_ready), ("risk_per_share", _has_value(construction.get("risk_per_share"))), ("max_loss_estimate", _has_value(construction.get("max_loss_estimate")))] if ready],
        missing_inputs=[name for name, ready in [("stop_price_or_invalidation_level", stop_ready), ("risk_per_share", _has_value(construction.get("risk_per_share"))), ("max_loss_estimate", _has_value(construction.get("max_loss_estimate")))] if not ready],
        blocker_codes=stop_codes,
        blocker_messages=_blocker_messages(construction, stop_codes),
        allowed_actions=["review", "not_captured_record", "skipped_record", "blocked_record"],
        disallowed_actions=[] if stop_status == READY else ["captured_manually", "paper_submit", "broker_execution"],
        source_artifacts=construction_sources,
        evaluated_at=now,
    ))

    manual_ready = market_ready and construction_status == READY and stop_status == READY and entry_ready and qty_ready and stop_ready and risk_ready
    manual_codes: list[str] = []
    for evaluation in evaluations:
        if evaluation["domain"] in {"market_data", "trade_construction", "stop_risk"} and evaluation["status"] != READY:
            manual_codes.extend([str(code) for code in evaluation.get("blocker_codes") or []])
    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="manual_capture",
        status=READY if manual_ready else BLOCKED,
        required_inputs=["market_data_ready", "complete_trade_ticket", "stop_risk_ready"],
        resolved_inputs=[name for name, ready in [("market_data_ready", market_ready), ("complete_trade_ticket", construction_status == READY), ("stop_risk_ready", stop_status == READY)] if ready],
        missing_inputs=[name for name, ready in [("market_data_ready", market_ready), ("complete_trade_ticket", construction_status == READY), ("stop_risk_ready", stop_status == READY)] if not ready],
        blocker_codes=manual_codes,
        blocker_messages=_blocker_messages(construction, manual_codes),
        allowed_actions=["captured_manually", "not_captured_record", "skipped_record", "blocked_record", "notes"] if manual_ready else ["not_captured_record", "skipped_record", "blocked_record", "notes"],
        disallowed_actions=[] if manual_ready else ["captured_manually", "paper_submit", "broker_execution"],
        source_artifacts=construction_sources,
        evaluated_at=now,
    ))

    submit_status = str(submit.get("boundary_status") or submit.get("status") or construction.get("submit_boundary_status") or "").strip().upper()
    submit_authorized = bool(submit.get("submission_authorized") is True or submit.get("submit_allowed") is True or submit_status in {"AUTHORIZED", "PASS", "READY", "CLEAR"})
    submit_codes = [str(code) for code in submit.get("blocking_codes") or []]
    if not submit and not submit_authorized:
        submit_codes.append("SUBMIT_BOUNDARY_MISSING")
    elif not submit_authorized and not submit_codes:
        submit_codes.append("SUBMIT_BOUNDARY_BLOCKED")
    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="paper_submit",
        status=READY if submit_authorized else BLOCKED,
        required_inputs=["execution_package", "submission_record", "submit_boundary_status", "paper_governance"],
        resolved_inputs=["submit_boundary_status"] if submit_authorized or submit else [],
        missing_inputs=[] if submit_authorized else (["submit_boundary_status"] if not submit else ["paper_submit_authorization"]),
        blocker_codes=submit_codes,
        blocker_messages=_blocker_messages(construction, submit_codes),
        allowed_actions=["paper_submit"] if submit_authorized else ["review", "manual_capture_if_ready", "skipped_record", "blocked_record"],
        disallowed_actions=[] if submit_authorized else ["paper_submit", "broker_execution"],
        source_artifacts=submit_sources,
        evaluated_at=now,
    ))

    evaluations.append(_eval(
        case_id=trade_lifecycle_case_id,
        selected_id=selected_exposure_intent_id,
        symbol=symbol,
        source_day=source_day,
        domain="execution",
        status=NOT_ENABLED,
        required_inputs=["explicit_execution_enablement"],
        resolved_inputs=[],
        missing_inputs=[],
        blocker_codes=[],
        blocker_messages=[],
        allowed_actions=["review"],
        disallowed_actions=["broker_execution", "live_order", "real_capital_allocation", "paper_submit_automation"],
        source_artifacts=[],
        evaluated_at=now,
    ))
    return evaluations


def domain_status_map_v1(evaluations: list[Mapping[str, Any]]) -> dict[str, str]:
    return {str(row.get("domain") or ""): str(row.get("status") or "") for row in evaluations}


def blockers_by_domain_v1(evaluations: list[Mapping[str, Any]]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    for row in evaluations:
        domain = str(row.get("domain") or "")
        codes = [str(code) for code in row.get("blocker_codes") or []]
        messages = [str(message) for message in row.get("blocker_messages") or []]
        out[domain] = [{"code": code, "message": messages[index] if index < len(messages) else code} for index, code in enumerate(codes)]
    return out


def write_readiness_domain_evaluations_v1(*, truth_root: Path | str, day_utc: str, evaluations: list[Mapping[str, Any]]) -> list[Path]:
    paths: list[Path] = []
    for evaluation in evaluations:
        domain = str(evaluation.get("domain") or "").strip()
        if domain not in ALLOWED_DOMAINS:
            continue
        path = readiness_domain_evaluation_path_v1(truth_root=truth_root, day_utc=day_utc, domain=domain)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_json_bytes_v1(dict(evaluation)) + b"\n")
        paths.append(path)
    return paths


def latest_readiness_domain_evaluations_v1(*, truth_root: Path | str, day_utc: str) -> list[dict[str, Any]]:
    directory = readiness_domain_evaluation_dir_v1(truth_root=truth_root, day_utc=day_utc)
    rows: list[dict[str, Any]] = []
    if not directory.exists():
        return rows
    for path in sorted(directory.glob(f"*.{REPORT_FILENAME}")):
        payload = _read_json(path)
        if payload:
            payload["artifact_path"] = str(path)
            rows.append(payload)
    return rows
