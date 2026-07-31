from __future__ import annotations

import csv
import hashlib
import json
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, read_json_v1, write_json_v1

REPORT_FAMILY = "aegis_entry_reference_price_certification_v1"
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
ALLOWED_SOURCES = {"LOCAL_CACHE", "MANUAL_CSV_DROP", "STOOQ", "YAHOO_CHART", "YFINANCE", "TIINGO", "ALPHA_VANTAGE"}
CERTIFIED = "CERTIFIED"
UNCERTIFIED_MISSING_PRICE = "UNCERTIFIED_MISSING_PRICE"
UNCERTIFIED_MISSING_SOURCE = "UNCERTIFIED_MISSING_SOURCE"
UNCERTIFIED_STALE_PRICE = "UNCERTIFIED_STALE_PRICE"
UNCERTIFIED_BAD_SYMBOL = "UNCERTIFIED_BAD_SYMBOL"
UNCERTIFIED_BAD_SCHEMA = "UNCERTIFIED_BAD_SCHEMA"
UNCERTIFIED_SOURCE_NOT_ALLOWED = "UNCERTIFIED_SOURCE_NOT_ALLOWED"
UNCERTIFIED_MARKET_CLOSED = "UNCERTIFIED_MARKET_CLOSED"
UNCERTIFIED_UNKNOWN = "UNCERTIFIED_UNKNOWN"

PRICE_MISSING = "PRICE_MISSING"
PRICE_TIMESTAMP_MISSING = "PRICE_TIMESTAMP_MISSING"
PRICE_STALE = "PRICE_STALE"
PRICE_SOURCE_MISSING = "PRICE_SOURCE_MISSING"
PRICE_SOURCE_NOT_ALLOWED = "PRICE_SOURCE_NOT_ALLOWED"
SOURCE_ARTIFACT_MISSING = "SOURCE_ARTIFACT_MISSING"
SOURCE_HASH_MISSING = "SOURCE_HASH_MISSING"
SYMBOL_NORMALIZATION_FAILED = "SYMBOL_NORMALIZATION_FAILED"
MARKET_SESSION_UNSUPPORTED = "MARKET_SESSION_UNSUPPORTED"
CERTIFICATION_ARTIFACT_MISSING = "CERTIFICATION_ARTIFACT_MISSING"
CERTIFICATION_SCHEMA_MISMATCH = "CERTIFICATION_SCHEMA_MISMATCH"

ALLOWED_CERTIFICATION_STATUSES = {
    CERTIFIED,
    UNCERTIFIED_MISSING_PRICE,
    UNCERTIFIED_MISSING_SOURCE,
    UNCERTIFIED_STALE_PRICE,
    UNCERTIFIED_BAD_SYMBOL,
    UNCERTIFIED_BAD_SCHEMA,
    UNCERTIFIED_SOURCE_NOT_ALLOWED,
    UNCERTIFIED_MARKET_CLOSED,
    UNCERTIFIED_UNKNOWN,
}

ALLOWED_DETAIL_REASON_CODES = {
    PRICE_MISSING,
    PRICE_TIMESTAMP_MISSING,
    PRICE_STALE,
    PRICE_SOURCE_MISSING,
    PRICE_SOURCE_NOT_ALLOWED,
    SOURCE_ARTIFACT_MISSING,
    SOURCE_HASH_MISSING,
    SYMBOL_NORMALIZATION_FAILED,
    MARKET_SESSION_UNSUPPORTED,
    CERTIFICATION_ARTIFACT_MISSING,
    CERTIFICATION_SCHEMA_MISMATCH,
    "CURRENT_SESSION_PRICE_INPUT_VALID",
    "DATA_REGISTRY_CURRENT",
}


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _is_number(value: Any) -> bool:
    try:
        return float(str(value)) > 0
    except Exception:
        return False


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() and path.is_file() else ""
    except Exception:
        return ""


def _executed_sleeve(row: dict[str, Any]) -> bool:
    status = _upper(row.get("status") or row.get("current_status"))
    signal_state = _upper(_safe_dict(row.get("signal_state")).get("state"))
    try:
        output_count = int(row.get("output_count") or 0)
    except (TypeError, ValueError):
        output_count = 0
    return status in {"NO_INTENT", "INTENT_CREATED", "FILTERED_OUT", "BLOCKED"} or signal_state == "ACTIVE" or output_count > 0


def _is_paper_rehearsal_ref(*values: Any) -> bool:
    return any("paper_rehearsal" in _text(value).lower() for value in values)


def _iter_sleeve_outcomes(root: Path, day_utc: str) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    rollup_path = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc / "sleeve_evaluation_rollup.v1.json"
    rollup = read_json_v1(rollup_path)
    for row in _safe_list(rollup.get("outcomes") or rollup.get("sleeve_outcomes")):
        if isinstance(row, dict):
            outcomes.append(row)
    known = {_upper(row.get("sleeve_id") or row.get("engine_id")) for row in outcomes}
    family_root = root / "reports" / "sleeve_evaluation_kernel_v1" / day_utc
    if family_root.exists():
        for child in sorted(family_root.iterdir()):
            if not child.is_dir():
                continue
            sleeve_id = _upper(child.name)
            if not sleeve_id or sleeve_id in known:
                continue
            payload = read_json_v1(child / "sleeve_evaluation.v1.json")
            if payload:
                outcomes.append(payload)
    return outcomes


def collect_entry_price_raw_signals_v1(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in _iter_sleeve_outcomes(root, day_utc):
        sleeve_id = _upper(row.get("sleeve_id") or row.get("engine_id"))
        if not sleeve_id or sleeve_id == SIMULATOR_ENGINE_ID or not _executed_sleeve(row):
            continue
        source_artifact = _text(row.get("artifact_path"))
        batch = _safe_dict(row.get("exposure_intent_batch"))
        intents = _safe_list(batch.get("output_intents")) or _safe_list(row.get("output_intents"))
        for item in intents:
            if not isinstance(item, dict):
                continue
            raw_signal_id = _text(item.get("raw_signal_id") or item.get("intent_id") or item.get("intent_hash"))
            symbol = _upper(item.get("symbol") or item.get("symbol_or_pair"))
            evidence_path = _text(item.get("intent_path") or item.get("raw_intent_path") or source_artifact)
            if not raw_signal_id or not symbol or _is_paper_rehearsal_ref(raw_signal_id, evidence_path):
                continue
            key = (sleeve_id, symbol, raw_signal_id)
            if key in seen:
                continue
            seen.add(key)
            out.append({"raw_signal_id": raw_signal_id, "intent_id": _text(item.get("intent_id")), "sleeve_id": sleeve_id, "symbol": symbol, "evidence_path": evidence_path, "source_artifact_path": source_artifact})
    return sorted(out, key=lambda row: (_text(row.get("sleeve_id")), _text(row.get("symbol")), _text(row.get("raw_signal_id"))))


def _input_by_data_item(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in _safe_list(payload.get("input_records")):
        if isinstance(row, dict) and _text(row.get("data_item_id")):
            out[_text(row.get("data_item_id"))] = row
    return out


def _registry_by_data_item(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in _safe_list(payload.get("data_items")):
        if isinstance(row, dict) and _text(row.get("data_item_id")):
            out[_text(row.get("data_item_id"))] = row
    return out


def _certify_signal(*, signal: dict[str, Any], day_utc: str, market_inputs_path: Path | None, market_inputs_hash: str, input_row: dict[str, Any], registry_row: dict[str, Any]) -> dict[str, Any]:
    symbol = _upper(signal.get("symbol"))
    data_item_id = f"market.price.{symbol}"
    reason_codes: list[str] = []
    status = CERTIFIED
    price = input_row.get("value") if input_row else None
    source = _upper(input_row.get("source_vendor")) if input_row else ""
    price_timestamp = _text(input_row.get("source_timestamp_utc")) if input_row else ""
    market_session = _text(input_row.get("day_utc")) if input_row else ""
    source_artifact = str(market_inputs_path.resolve()) if market_inputs_path and market_inputs_path.exists() else ""
    source_hash = market_inputs_hash if source_artifact else ""
    normalized_symbol = _upper(input_row.get("symbol")) if input_row else symbol
    if not input_row:
        status = UNCERTIFIED_MISSING_PRICE
        reason_codes.append(PRICE_MISSING)
    elif _text(input_row.get("data_item_id")) != data_item_id or normalized_symbol != symbol:
        status = UNCERTIFIED_BAD_SYMBOL
        reason_codes.append(SYMBOL_NORMALIZATION_FAILED)
    elif not _is_number(price):
        status = UNCERTIFIED_MISSING_PRICE
        reason_codes.append(PRICE_MISSING)
    elif _upper(input_row.get("validation_status")) != "VALID":
        status = UNCERTIFIED_BAD_SCHEMA
        reason_codes.append(CERTIFICATION_SCHEMA_MISMATCH)
    elif not market_session or market_session != day_utc:
        status = UNCERTIFIED_STALE_PRICE
        reason_codes.append(PRICE_STALE)
        if not market_session:
            reason_codes.append(MARKET_SESSION_UNSUPPORTED)
    elif not price_timestamp:
        status = UNCERTIFIED_STALE_PRICE
        reason_codes.append(PRICE_TIMESTAMP_MISSING)
    elif not source:
        status = UNCERTIFIED_MISSING_SOURCE
        reason_codes.append(PRICE_SOURCE_MISSING)
    elif source not in ALLOWED_SOURCES:
        status = UNCERTIFIED_SOURCE_NOT_ALLOWED
        reason_codes.append(PRICE_SOURCE_NOT_ALLOWED)
    elif not source_artifact:
        status = UNCERTIFIED_MISSING_SOURCE
        reason_codes.append(SOURCE_ARTIFACT_MISSING)
    elif not source_hash:
        status = UNCERTIFIED_MISSING_SOURCE
        reason_codes.append(SOURCE_HASH_MISSING)
    else:
        reason_codes.append("CURRENT_SESSION_PRICE_INPUT_VALID")
        if registry_row and _upper(registry_row.get("status")) == "CURRENT":
            reason_codes.append("DATA_REGISTRY_CURRENT")
    safe_repair = status in {UNCERTIFIED_MISSING_PRICE, UNCERTIFIED_STALE_PRICE, UNCERTIFIED_MISSING_SOURCE}
    return {
        "certification_id": "entry_price_cert_" + hashlib.sha256(json.dumps({"day_utc": day_utc, "raw_signal_id": signal.get("raw_signal_id"), "sleeve_id": signal.get("sleeve_id"), "symbol": symbol}, sort_keys=True).encode("utf-8")).hexdigest()[:24],
        "raw_signal_id": _text(signal.get("raw_signal_id")),
        "intent_id": _text(signal.get("intent_id")),
        "sleeve_id": _text(signal.get("sleeve_id")),
        "symbol": symbol,
        "normalized_symbol": normalized_symbol or symbol,
        "data_item_id": data_item_id,
        "price": _text(price),
        "price_timestamp": price_timestamp,
        "market_session": market_session,
        "source": source,
        "source_artifact": source_artifact,
        "source_hash": source_hash,
        "upstream_registry_status": _upper(registry_row.get("status")) if registry_row else "",
        "upstream_registry_market_data_validation_status": _upper(registry_row.get("market_data_validation_status")) if registry_row else "",
        "certification_status": status,
        "certification_reason_codes": reason_codes,
        "freshness_window_seconds": int(input_row.get("freshness_ttl_seconds") or registry_row.get("freshness_ttl_seconds") or 900) if input_row or registry_row else 900,
        "safe_repair_available": bool(safe_repair),
        "generated_at": "",
        "day_utc": day_utc,
    }


def build_entry_reference_price_certification_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    market_inputs_path, market_inputs = latest_json_v1(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")
    data_registry_path, data_registry = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    input_by_id = _input_by_data_item(_safe_dict(market_inputs))
    registry_by_id = _registry_by_data_item(_safe_dict(data_registry))
    market_inputs_hash = _sha256(Path(market_inputs_path)) if market_inputs_path else ""
    rows = []
    for signal in collect_entry_price_raw_signals_v1(truth_root=root, day_utc=day_utc):
        symbol = _upper(signal.get("symbol"))
        data_item_id = f"market.price.{symbol}"
        rows.append(_certify_signal(signal=signal, day_utc=day_utc, market_inputs_path=Path(market_inputs_path) if market_inputs_path else None, market_inputs_hash=market_inputs_hash, input_row=input_by_id.get(data_item_id, {}), registry_row=registry_by_id.get(data_item_id, {})))
    generated_at = now_utc_v1()
    for row in rows:
        row["generated_at"] = generated_at
    certified = [row for row in rows if row.get("certification_status") == CERTIFIED]
    status_counts = {status: sum(1 for row in rows if row.get("certification_status") == status) for status in sorted({str(row.get("certification_status")) for row in rows})}
    return {
        "schema_id": "aegis_entry_reference_price_certification",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "generated_at": generated_at,
        "certification_status": "PASS" if len(certified) == len(rows) else ("PARTIAL" if certified else "FAILED"),
        "total_rows": len(rows),
        "certified_count": len(certified),
        "uncertified_count": len(rows) - len(certified),
        "status_counts": status_counts,
        "rows": rows,
        "input_artifacts": {
            "market_data_inputs": str(market_inputs_path or ""),
            "data_registry": str(data_registry_path or ""),
        },
        "source_hashes": {
            "market_data_inputs": market_inputs_hash,
            "data_registry": _sha256(Path(data_registry_path)) if data_registry_path else "",
        },
        "safety": {"trade_advice_allowed": False, "manual_capture_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False},
    }


def write_entry_reference_price_certification_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_target = out_dir / "entry_reference_price_certification.v1.json"
    for row in _safe_list(payload.get("rows")):
        if isinstance(row, dict):
            row["certification_artifact_path"] = str(json_target)
    json_path = write_json_v1(json_target, payload)
    summary_path = out_dir / "entry_reference_price_certification.summary.txt"
    matrix_path = out_dir / "entry_reference_price_certification.matrix.csv"
    summary_path.write_text(render_entry_reference_price_certification_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_entry_reference_price_certification_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_entry_reference_price_certification_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS ENTRY REFERENCE PRICE CERTIFICATION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"certification_status: {payload.get('certification_status')}",
        f"total_rows: {payload.get('total_rows')}",
        f"certified_count: {payload.get('certified_count')}",
        f"uncertified_count: {payload.get('uncertified_count')}",
        "status_counts:",
    ]
    for status, count in sorted(_safe_dict(payload.get("status_counts")).items()):
        lines.append(f"- {status}: {count}")
    lines.extend(["", "safety:", "- trade_advice_allowed: false", "- manual_capture_allowed: false", "- broker_execution_allowed: false", "- autonomous_execution_allowed: false"])
    return "\n".join(lines) + "\n"


def render_entry_reference_price_certification_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    fields = ["raw_signal_id", "sleeve_id", "symbol", "price", "price_timestamp", "market_session", "source", "certification_status", "certification_reason_codes", "source_artifact", "safe_repair_available"]
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    for row in _safe_list(payload.get("rows")):
        if not isinstance(row, dict):
            continue
        writer.writerow({field: "|".join(row.get(field) or []) if field == "certification_reason_codes" else row.get(field, "") for field in fields})
    return out.getvalue()
