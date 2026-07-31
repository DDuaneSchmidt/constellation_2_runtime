from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ops.aegis.entry_reference_price_certification_v1 import ALLOWED_CERTIFICATION_STATUSES, ALLOWED_DETAIL_REASON_CODES, CERTIFIED, build_entry_reference_price_certification_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1

REPORT_FAMILY = "aegis_entry_reference_price_certification_self_check_v1"


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (_text(row.get("sleeve_id")), _text(row.get("symbol")), _text(row.get("raw_signal_id")))


def _normalized(payload: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(payload)
    out.pop("generated_at", None)
    for row in _safe_list(out.get("rows")):
        if isinstance(row, dict):
            row.pop("generated_at", None)
            row.pop("certification_artifact_path", None)
    return out


def build_entry_reference_price_certification_self_check_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    cert_path, cert = latest_json_v1(root, "aegis_entry_reference_price_certification_v1", day_utc, "entry_reference_price_certification.v1.json")
    graph_path, graph = latest_json_v1(root, "aegis_signal_evidence_graph_v1", day_utc, "signal_evidence_graph.v1.json")
    contracts_path, contracts = latest_json_v1(root, "aegis_candidate_contracts_v1", day_utc, "candidate_contracts.v1.json")
    failures: list[dict[str, Any]] = []
    if not cert_path:
        failures.append({"failure_code": "CERTIFICATION_ARTIFACT_MISSING", "detail_reason_code": "CERTIFICATION_ARTIFACT_MISSING"})
    if graph and not _text(_safe_dict(graph.get("input_artifacts")).get("entry_reference_price_certification")):
        failures.append({"failure_code": "SIGNAL_GRAPH_MISSING_CERTIFICATION_ARTIFACT_PATH", "detail_reason_code": "CERTIFICATION_ARTIFACT_MISSING"})
    cert_rows = [row for row in _safe_list(cert.get("rows")) if isinstance(row, dict)]
    by_key = {_key(row): row for row in cert_rows}
    for signal in _safe_list(graph.get("signals")):
        if not isinstance(signal, dict):
            continue
        key = _key(signal)
        if key not in by_key:
            failures.append({"failure_code": "MISSING_ENTRY_PRICE_CERTIFICATION_ROW", "raw_signal_id": signal.get("raw_signal_id"), "sleeve_id": signal.get("sleeve_id"), "symbol": signal.get("symbol")})
    for row in _safe_list(contracts.get("candidate_contracts")):
        if not isinstance(row, dict):
            continue
        status = _text(row.get("entry_reference_price_certification_status") or row.get("entry_reference_price_status"))
        if status != CERTIFIED:
            failures.append({"failure_code": "VALID_CONTRACT_WITH_UNCERTIFIED_ENTRY_PRICE", "candidate_id": row.get("candidate_id"), "raw_signal_id": row.get("raw_signal_id"), "status": status})
    for row in _safe_list(contracts.get("rejected_raw_signals")):
        if not isinstance(row, dict):
            continue
        if _text(row.get("rejection_reason")).startswith("ENTRY_REFERENCE_PRICE"):
            detail_codes = _safe_list(row.get("detail_reason_codes")) or _safe_list(row.get("entry_reference_price_certification_reason_codes"))
            if not detail_codes:
                failures.append({"failure_code": "UNCERTIFIED_REJECTION_MISSING_DETAIL_REASON_CODES", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol")})
            for code in detail_codes:
                if _text(code) not in ALLOWED_DETAIL_REASON_CODES:
                    failures.append({"failure_code": "UNCERTIFIED_REJECTION_UNKNOWN_DETAIL_REASON_CODE", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol"), "detail_reason_code": code})
    for row in cert_rows:
        status = _text(row.get("certification_status"))
        reason_codes = [_text(code) for code in _safe_list(row.get("certification_reason_codes")) if _text(code)]
        if status not in ALLOWED_CERTIFICATION_STATUSES:
            failures.append({"failure_code": "CERTIFICATION_STATUS_NOT_ALLOWED", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol"), "status": status})
        if not reason_codes:
            failures.append({"failure_code": "CERTIFICATION_MISSING_DETAIL_REASON_CODES", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol"), "status": status})
        for code in reason_codes:
            if code not in ALLOWED_DETAIL_REASON_CODES:
                failures.append({"failure_code": "CERTIFICATION_UNKNOWN_DETAIL_REASON_CODE", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol"), "detail_reason_code": code})
        if status == CERTIFIED and (not _text(row.get("source_artifact")) or not _text(row.get("source_hash"))):
            failures.append({"failure_code": "CERTIFIED_PRICE_MISSING_SOURCE_ARTIFACT_OR_HASH", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol")})
        if status == CERTIFIED and (not _text(row.get("price_timestamp")) or _text(row.get("market_session")) != day_utc):
            failures.append({"failure_code": "CERTIFIED_PRICE_NOT_CURRENT_SESSION", "raw_signal_id": row.get("raw_signal_id"), "symbol": row.get("symbol"), "market_session": row.get("market_session")})
    rebuilt = build_entry_reference_price_certification_v1(truth_root=root, day_utc=day_utc)
    if cert and _normalized(rebuilt) != _normalized(cert):
        failures.append({"failure_code": "NON_DETERMINISTIC_OUTPUT"})
    return {
        "schema_id": "aegis_entry_reference_price_certification_self_check",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "ok": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "source_artifacts": {"certification": str(cert_path or ""), "signal_evidence_graph": str(graph_path or ""), "candidate_contracts": str(contracts_path or "")},
        "safety": {"trade_advice_allowed": False, "manual_capture_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False},
    }


def write_entry_reference_price_certification_self_check_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "self_check.v1.json", payload)
    return {"json": str(path)}
