from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from ops.aegis.event_append_transaction_v1 import (
    contract_input_hashes_for_paths_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)
from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1
from ops.aegis.market_data.market_data_provider_v1 import fetch_market_data_v1, provider_config_from_env_v1
from ops.aegis.market_data.market_data_mode_v1 import FINAL_EOD_CERTIFIED, INTRADAY_OPERATIONAL, normalize_market_data_mode_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, market_data_kind_v1, normalize_market_symbol_v1
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1, write_symbol_map_v1
from ops.aegis.runtime_evaluation_v1 import stable_hash_v1

REPORT_FAMILY = "market_data_inputs_v1"
READINESS_FAMILY = "market_data_readiness_v1"
PRODUCER_ID = "ops/tools/build_aegis_market_data_inputs_v1.py"
PRODUCER_VERSION = "v1"


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")




def _final_eod_pointer_path_v1(root: Path, day_utc: str) -> Path:
    return root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"


def _read_final_eod_artifact_v1(root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    pointer = _final_eod_pointer_path_v1(root, day_utc)
    payload = read_json_local_v1(pointer)
    if payload.get("schema_id") == "final_eod_market_data_current_manifest.v1":
        artifact_path = Path(str(payload.get("current_artifact_path") or ""))
        artifact = read_json_local_v1(artifact_path) if artifact_path.exists() else {}
        return (artifact_path if artifact else pointer), artifact
    if payload:
        return pointer, payload
    manifest = root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.current.v1.json"
    manifest_payload = read_json_local_v1(manifest)
    artifact_path = Path(str(manifest_payload.get("current_artifact_path") or ""))
    artifact = read_json_local_v1(artifact_path) if artifact_path.exists() else {}
    return (artifact_path if artifact else None), artifact


def _final_eod_artifact_valid_v1(payload: dict[str, Any], day_utc: str) -> bool:
    if not payload:
        return False
    status_text = " ".join(str(payload.get(key) or "") for key in ("status", "validation_status", "final_eod_certification_status")).upper()
    if "VALID" not in status_text and "CURRENT" not in status_text and "CERTIFIED" not in status_text:
        return False
    requested = {normalize_market_symbol_v1(symbol) for symbol in payload.get("requested_symbols", []) if str(symbol)}
    final_symbols = {normalize_market_symbol_v1(symbol) for symbol in payload.get("final_eod_symbols", []) if str(symbol)}
    if requested and not requested <= final_symbols:
        return False
    rows = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
    for symbol in requested or set(rows):
        row = rows.get(symbol) if isinstance(rows.get(symbol), dict) else rows.get(str(symbol).upper())
        if not isinstance(row, dict):
            return False
        if str(row.get("market_session_date") or row.get("trading_day") or "") != day_utc:
            return False
        if str(row.get("freshness_status") or "").upper() == "STALE":
            return False
        if str(row.get("data_finality") or "").upper() != "FINAL_EOD":
            return False
    return True


def _provider_result_from_final_eod_artifact_v1(*, root: Path, day_utc: str, symbols: list[str], generated_at_utc: str) -> tuple[Any, dict[str, Any]]:
    artifact_path, payload = _read_final_eod_artifact_v1(root, day_utc)
    rows = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
    valid = _final_eod_artifact_valid_v1(payload, day_utc)
    requested = canonicalize_symbol_list_v1(symbols)
    out_symbols: dict[str, dict[str, Any]] = {}
    for symbol in requested:
        row = rows.get(symbol) if isinstance(rows.get(symbol), dict) else rows.get(str(symbol).upper())
        if isinstance(row, dict):
            out_symbols[symbol] = {**row, "canonical_symbol": symbol, "symbol": symbol}
    missing = sorted(set(requested) - set(out_symbols))
    status = "SUCCESS" if valid and not missing else "FAILED"
    artifact_hash = sha256_file_v1(artifact_path) if artifact_path and artifact_path.exists() else ""
    provider_results = list(payload.get("provider_results") or []) if isinstance(payload.get("provider_results"), list) else []
    provider_results.append({
        "provider": "FINAL_EOD_ARTIFACT",
        "status": status,
        "request_status": status,
        "artifact_path": str(artifact_path or ""),
        "artifact_hash": artifact_hash,
        "requested_symbols": requested,
        "fetched_symbols": sorted(out_symbols),
        "missing_symbols": missing,
        "failure_reason": "" if status == "SUCCESS" else "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE",
    })
    result = SimpleNamespace(
        provider="FINAL_EOD_ARTIFACT",
        request_status=status,
        timestamp_utc=str(payload.get("generated_at_utc") or generated_at_utc),
        returned_data_date=day_utc,
        symbols=out_symbols,
        breadth={},
        failure_reason="" if status == "SUCCESS" else "FINAL_EOD_ARTIFACT_MISSING_OR_INCOMPLETE",
        provider_results=tuple(provider_results),
        requested_symbols=tuple(requested),
        fetched_symbols=tuple(sorted(out_symbols)),
        missing_symbols=tuple(missing),
        stale_symbols=tuple(payload.get("stale_symbols") or ()),
        mapping_missing_symbols=(),
        provider_failed_symbols=tuple(missing),
    )
    lineage = {
        "source": "final_eod_market_data_v1",
        "artifact_path": str(artifact_path or ""),
        "artifact_hash": artifact_hash,
        "artifact_content_hash": str(payload.get("content_hash") or ""),
        "artifact_status": str(payload.get("status") or ""),
        "artifact_validation_status": str(payload.get("validation_status") or ""),
        "artifact_symbol_count": len(rows),
        "artifact_final_symbol_count": len(payload.get("final_eod_symbols") or []),
        "artifact_valid_for_market_inputs": bool(valid),
    }
    return result, lineage


def read_json_local_v1(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def build_market_data_inputs_v1(
    *,
    truth_root: Path,
    day_utc: str,
    generated_at_utc: str | None = None,
    symbol_map_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or utc_now_v1()
    contracts_path, contracts = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    required_item_ids, optional_item_ids, sleeve_dependencies = _required_market_inputs(contracts)
    all_item_ids = sorted(set(required_item_ids) | set(optional_item_ids))
    symbols = _symbols_from_item_ids(all_item_ids)
    symbol_map = symbol_map_override if isinstance(symbol_map_override, dict) else build_symbol_map_v1(repo_root=Path(__file__).resolve().parents[2], day_utc=day_utc)
    symbol_map_paths = write_symbol_map_v1(truth_root=root, day_utc=day_utc, payload=symbol_map)
    symbol_map_path = Path(symbol_map_paths["json"])
    config = provider_config_from_env_v1()
    requested_mode = normalize_market_data_mode_v1(config.market_data_mode)
    final_eod_lineage: dict[str, Any] = {}
    if requested_mode == FINAL_EOD_CERTIFIED:
        result, final_eod_lineage = _provider_result_from_final_eod_artifact_v1(root=root, day_utc=day_utc, symbols=symbols, generated_at_utc=generated_at)
    else:
        result = fetch_market_data_v1(truth_root=root, day_utc=day_utc, symbols=symbols, symbol_map=symbol_map)
    records = [
        _input_record(
            item_id=item_id,
            day_utc=day_utc,
            retrieved_at_utc=result.timestamp_utc or generated_at,
            row=_row_for_item(result.symbols, item_id),
            required=item_id in required_item_ids,
            provider_request_status=result.request_status,
            provider_failure_reason=_provider_failure_reason_for_item(result=result, item_id=item_id),
            provider_failed=_provider_unavailable_for_item(result=result, item_id=item_id),
            freshness_ttl_seconds=config.cache_ttl_seconds,
            requested_mode=requested_mode,
        )
        for item_id in all_item_ids
    ]
    status = _overall_status(records, required_item_ids)
    downstream_invariant = _downstream_certification_invariant_v1(
        records=records,
        required_item_ids=required_item_ids,
        requested_mode=requested_mode,
        final_eod_lineage=final_eod_lineage,
    )
    return {
        "schema_id": "market_data_inputs",
        "schema_version": "v1",
        "artifact_id": "market_data_inputs_v1",
        "producer_id": PRODUCER_ID,
        "producer_version": PRODUCER_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "retrieved_at_utc": result.timestamp_utc or generated_at,
        "status": status,
        "market_data_mode": requested_mode,
        "certification_mode": "FINAL_EOD_CERTIFICATION" if requested_mode == FINAL_EOD_CERTIFIED else "INTRADAY_OPERATIONAL_RUN",
        "validation_status": "VALID" if status == "READY" else "BLOCKED",
        "source_vendor": result.provider or config.primary or config.fallback or "",
        "provider_config": {
            "primary": config.primary,
            "fallback": config.fallback,
            "allow_delayed": config.allow_delayed,
            "require_current_session": config.require_current_session,
            "require_breadth": config.require_breadth,
            "timeout_seconds": config.timeout_seconds,
            "cache_ttl_seconds": config.cache_ttl_seconds,
            "configured": bool(config.primary or config.fallback),
            "market_data_mode": requested_mode,
        },
        "provider_results": list(result.provider_results),
        "final_eod_certification_status": "VALID" if requested_mode == FINAL_EOD_CERTIFIED and status == "READY" else ("PENDING" if requested_mode == INTRADAY_OPERATIONAL and status == "READY" else "UNAVAILABLE"),
        "final_eod_certification_pending": bool(requested_mode == INTRADAY_OPERATIONAL and status == "READY"),
        "provider_request_status": result.request_status,
        "provider_failure_reason": result.failure_reason,
        "final_eod_artifact_lineage": final_eod_lineage,
        "downstream_certification_invariant": downstream_invariant,
        "operator_downstream_blocker_message": downstream_invariant.get("operator_message", ""),
        "required_market_input_ids": required_item_ids,
        "optional_market_input_ids": optional_item_ids,
        "sleeve_dependencies": sleeve_dependencies,
        "requested_symbols": symbols,
        "fetched_symbols": list(result.fetched_symbols),
        "missing_symbols": list(result.missing_symbols),
        "stale_symbols": list(result.stale_symbols),
        "mapping_missing_symbols": list(result.mapping_missing_symbols),
        "provider_failed_symbols": list(result.provider_failed_symbols),
        "symbol_map_path": str(symbol_map_path),
        "symbol_map_hash": sha256_file_v1(symbol_map_path) if symbol_map_path.exists() else "",
        "sleeve_input_contracts_path": str(contracts_path or ""),
        "sleeve_input_contracts_hash": sha256_file_v1(contracts_path) if contracts_path else "",
        "input_records": records,
        "missing_input_ids": [row["data_item_id"] for row in records if row["validation_status"] in {"MISSING", "UNAVAILABLE_EXTERNAL_SOURCE"} and row["required"]],
        "stale_input_ids": [row["data_item_id"] for row in records if row["validation_status"] == "STALE" and row["required"]],
        "malformed_input_ids": [row["data_item_id"] for row in records if row["validation_status"] == "MALFORMED" and row["required"]],
        "tampered_input_ids": [row["data_item_id"] for row in records if row["validation_status"] == "TAMPERED" and row["required"]],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }



def _downstream_certification_invariant_v1(*, records: list[dict[str, Any]], required_item_ids: list[str], requested_mode: str, final_eod_lineage: dict[str, Any]) -> dict[str, Any]:
    if normalize_market_data_mode_v1(requested_mode) != FINAL_EOD_CERTIFIED:
        return {"status": "NOT_APPLICABLE", "operator_message": ""}
    required_records = [row for row in records if row.get("data_item_id") in set(required_item_ids)]
    invalid = [row for row in required_records if row.get("validation_status") != "VALID"]
    if not invalid:
        return {
            "status": "PASS",
            "operator_message": "US_EQUITIES_EOD certified artifact satisfies downstream market-data inputs.",
            "source_artifact_hash": final_eod_lineage.get("artifact_hash", ""),
            "blocked_input_ids": [],
        }
    blocked_ids = [str(row.get("data_item_id") or "") for row in invalid]
    return {
        "status": "DOWNSTREAM_ONLY_BLOCKER",
        "operator_message": "US_EQUITIES_EOD is certified, but downstream market-data inputs remain blocked for: " + ", ".join(blocked_ids) + ". Review blocked_reasons; do not treat sleeve readiness as complete.",
        "source_artifact_hash": final_eod_lineage.get("artifact_hash", ""),
        "source_artifact_path": final_eod_lineage.get("artifact_path", ""),
        "blocked_input_ids": blocked_ids,
        "blocked_reasons": {str(row.get("data_item_id") or ""): str(row.get("reason") or row.get("validation_status") or "") for row in invalid},
    }

def build_market_data_readiness_v1(*, inputs_payload: dict[str, Any]) -> dict[str, Any]:
    records = inputs_payload.get("input_records") if isinstance(inputs_payload.get("input_records"), list) else []
    required = [row for row in records if isinstance(row, dict) and row.get("required") is True]
    invalid = [row for row in required if row.get("validation_status") != "VALID"]
    status = "READY" if not invalid and required else "BLOCKED"
    return {
        "schema_id": "market_data_readiness",
        "schema_version": "v1",
        "artifact_id": "market_data_readiness_v1",
        "day_utc": str(inputs_payload.get("day_utc") or ""),
        "generated_at_utc": str(inputs_payload.get("generated_at_utc") or ""),
        "status": status,
        "market_data_inputs_path": str(inputs_payload.get("artifact_path") or ""),
        "market_data_inputs_hash": str(inputs_payload.get("artifact_hash") or ""),
        "ready_input_ids": [row["data_item_id"] for row in required if row.get("validation_status") == "VALID"],
        "blocked_input_ids": [row["data_item_id"] for row in invalid],
        "input_statuses": [
            {
                "data_item_id": row.get("data_item_id"),
                "symbol": row.get("symbol"),
                "field_type": row.get("field_type"),
                "validation_status": row.get("validation_status"),
                "reason": row.get("reason"),
                "source_vendor": row.get("source_vendor"),
                "source_timestamp_utc": row.get("source_timestamp_utc"),
                "raw_source_hash": row.get("raw_source_hash"),
                "transformed_value_hash": row.get("transformed_value_hash"),
                "retrieval_timestamp_utc": row.get("retrieval_timestamp_utc") or row.get("retrieved_at_utc"),
                "cache_path": row.get("cache_path") or row.get("source_path"),
                "freshness_ttl_seconds": row.get("freshness_ttl_seconds"),
                "market_data_mode": row.get("market_data_mode"),
                "finalization_status": row.get("finalization_status"),
                "usable_for": row.get("usable_for") if isinstance(row.get("usable_for"), dict) else {},
                "input_classification": row.get("input_classification"),
                "required": bool(row.get("required")),
            }
            for row in records
            if isinstance(row, dict)
        ],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_market_data_inputs_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    out_dir = root / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "market_data_inputs.v1.json", payload)
    artifact_ref_payload = {**payload, "artifact_path": str(json_path), "artifact_hash": sha256_file_v1(json_path)}
    readiness = build_market_data_readiness_v1(inputs_payload=artifact_ref_payload)
    readiness_dir = root / "reports" / READINESS_FAMILY / day_utc
    readiness_json = write_json_v1(readiness_dir / "market_data_readiness.v1.json", readiness)
    readiness_txt = readiness_dir / "market_data_readiness.v1.txt"
    readiness_txt.write_text(render_market_data_readiness_v1(readiness), encoding="utf-8")
    inputs_txt = out_dir / "market_data_inputs.v1.txt"
    inputs_txt.write_text(render_market_data_inputs_v1(payload), encoding="utf-8")
    return {
        "market_data_inputs_json": str(json_path),
        "market_data_inputs_txt": str(inputs_txt),
        "market_data_readiness_json": str(readiness_json),
        "market_data_readiness_txt": str(readiness_txt),
    }


def emit_market_data_inputs_events_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any], artifact_path: Path) -> list[dict[str, Any]]:
    input_paths = [payload.get("symbol_map_path"), payload.get("sleeve_input_contracts_path")]
    input_hashes = contract_input_hashes_for_paths_v1(
        [Path(str(path)) for path in input_paths if path],
        extra={
            "raw_source_hashes": stable_hash_v1({row.get("data_item_id"): row.get("raw_source_hash") for row in payload.get("input_records", []) if isinstance(row, dict)}),
            "provider_request_status": str(payload.get("provider_request_status") or ""),
        },
    )
    return emit_artifact_evidence_transaction_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        artifact_path=artifact_path,
        payload=payload,
        producer_id=PRODUCER_ID,
        producer_version=PRODUCER_VERSION,
        created_at_utc=str(payload.get("generated_at_utc") or ""),
        input_hashes=input_hashes,
        validation_status="VALID" if str(payload.get("validation_status") or "").upper() == "VALID" else "REJECTED",
    )


def render_market_data_inputs_v1(payload: dict[str, Any]) -> str:
    lines = [
        "MARKET DATA INPUTS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"source_vendor: {payload.get('source_vendor')}",
        f"required_inputs: {len(payload.get('required_market_input_ids') or [])}",
        f"missing_input_ids: {', '.join(payload.get('missing_input_ids') or [])}",
        f"stale_input_ids: {', '.join(payload.get('stale_input_ids') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
    ]
    for row in payload.get("input_records") or []:
        lines.append(f"- {row.get('data_item_id')}: {row.get('validation_status')} value={row.get('value')} source={row.get('source_vendor')} ts={row.get('source_timestamp_utc')} reason={row.get('reason')}")
    return "\n".join(lines) + "\n"


def render_market_data_readiness_v1(payload: dict[str, Any]) -> str:
    lines = [
        "MARKET DATA READINESS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"ready_input_ids: {', '.join(payload.get('ready_input_ids') or [])}",
        f"blocked_input_ids: {', '.join(payload.get('blocked_input_ids') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
    ]
    return "\n".join(lines) + "\n"


def _required_market_inputs(contracts: dict[str, Any]) -> tuple[list[str], list[str], dict[str, list[str]]]:
    required: set[str] = set()
    optional: set[str] = set()
    dependencies: dict[str, list[str]] = {}
    for contract in contracts.get("contracts", []) if isinstance(contracts.get("contracts"), list) else []:
        if not isinstance(contract, dict):
            continue
        sleeve_id = str(contract.get("sleeve_id") or "").strip()
        for key, bucket in (("required_inputs", required), ("optional_inputs", optional)):
            for row in contract.get(key, []) if isinstance(contract.get(key), list) else []:
                item_id = str(row.get("data_item_id") or "").strip()
                if item_id.startswith(("market.price.", "market.volatility.")):
                    bucket.add(item_id)
                    dependencies.setdefault(item_id, []).append(sleeve_id)
    return sorted(required), sorted(optional - required), {key: sorted(set(value)) for key, value in sorted(dependencies.items())}




def _provider_unavailable_for_item(*, result: Any, item_id: str) -> bool:
    symbol = normalize_market_symbol_v1(item_id.rsplit(".", 1)[-1])
    failed = {normalize_market_symbol_v1(item) for item in getattr(result, "provider_failed_symbols", ()) or ()}
    if symbol in failed:
        return True
    for attempt in getattr(result, "provider_results", ()) or ():
        if not isinstance(attempt, dict):
            continue
        provider = str(attempt.get("provider") or "").strip().upper()
        missing = {normalize_market_symbol_v1(item) for item in attempt.get("missing_symbols", []) if str(item)}
        reason = str(attempt.get("failure_reason") or "").strip()
        if reason == "MARKET_DATA_PROVIDER_DISABLED":
            continue
        if provider == "CBOE" and symbol != "VIX":
            continue
        if symbol in missing and reason in {"CBOE_SOURCE_UNAVAILABLE", "CBOE_VIX_ROW_NOT_FOUND", "MARKET_DATA_FETCH_FAILED", "MARKET_DATA_REFRESH_TIMEOUT"}:
            return True
    return False


def _provider_failure_reason_for_item(*, result: Any, item_id: str) -> str:
    symbol = normalize_market_symbol_v1(item_id.rsplit(".", 1)[-1])
    for attempt in reversed(tuple(getattr(result, "provider_results", ()) or ())):
        if not isinstance(attempt, dict):
            continue
        provider = str(attempt.get("provider") or "").strip().upper()
        missing = {normalize_market_symbol_v1(item) for item in attempt.get("missing_symbols", []) if str(item)}
        reason = str(attempt.get("failure_reason") or "").strip()
        if reason == "MARKET_DATA_PROVIDER_DISABLED":
            continue
        if provider == "CBOE" and symbol != "VIX":
            continue
        if symbol in missing and reason:
            return reason
    return str(getattr(result, "failure_reason", "") or "")


def _symbols_from_item_ids(item_ids: list[str]) -> list[str]:
    symbols = []
    for item_id in item_ids:
        if item_id.startswith("market.price."):
            symbols.append(item_id.rsplit(".", 1)[-1])
        elif item_id == "market.volatility.VIX":
            symbols.append("VIX")
    return canonicalize_symbol_list_v1(symbols)


def _row_for_item(symbols: dict[str, dict[str, Any]], item_id: str) -> dict[str, Any]:
    symbol = normalize_market_symbol_v1(item_id.rsplit(".", 1)[-1])
    row = symbols.get(symbol)
    return row if isinstance(row, dict) else {}


def _input_record(
    *,
    item_id: str,
    day_utc: str,
    retrieved_at_utc: str,
    row: dict[str, Any],
    required: bool,
    provider_request_status: str = "",
    provider_failure_reason: str = "",
    provider_failed: bool = False,
    freshness_ttl_seconds: int = 86400,
    requested_mode: str = INTRADAY_OPERATIONAL,
) -> dict[str, Any]:
    symbol = normalize_market_symbol_v1(item_id.rsplit(".", 1)[-1])
    field_type = "volatility" if item_id.startswith("market.volatility.") else "price"
    value = row.get("last_price") if row else None
    source_timestamp = str(row.get("data_timestamp_utc") or "") if row else ""
    session_date = str(row.get("market_session_date") or "") if row else ""
    validation, reason = _validation_status(day_utc=day_utc, field_type=field_type, value=value, source_timestamp=source_timestamp, session_date=session_date, row=row, requested_mode=requested_mode)
    if validation == "MISSING" and provider_failed:
        validation = "UNAVAILABLE_EXTERNAL_SOURCE"
        reason = provider_failure_reason or provider_request_status or "UNAVAILABLE_EXTERNAL_SOURCE"
    raw_source_hash = _raw_source_hash(row)
    source_path = str(row.get("source_url_or_path") or "") if row else ""
    transformed = {
        "data_item_id": item_id,
        "symbol": symbol,
        "field_type": field_type,
        "value": value,
        "source_timestamp_utc": source_timestamp,
        "retrieved_at_utc": retrieved_at_utc,
        "retrieval_timestamp_utc": retrieved_at_utc,
        "day_utc": day_utc,
        "source_vendor": str(row.get("provider") or row.get("source") or "") if row else "",
        "market_data_mode": str(row.get("market_data_mode") or "") if row else "",
        "finalization_status": str(row.get("finalization_status") or "") if row else "",
        "usable_for": row.get("usable_for") if isinstance(row.get("usable_for"), dict) else {},
    }
    if validation == "VALID" and not raw_source_hash:
        validation = "TAMPERED"
        reason = "RAW_SOURCE_HASH_MISSING"
    return {
        **transformed,
        "currency": "USD" if field_type == "price" else "",
        "unit": "USD/share" if field_type == "price" else "index_level",
        "source_vendor": str(row.get("provider") or row.get("source") or "") if row else "",
        "source_path": source_path,
        "cache_path": source_path,
        "raw_source_hash": raw_source_hash,
        "transformed_value_hash": stable_hash_v1(transformed),
        "producer_id": PRODUCER_ID,
        "producer_version": PRODUCER_VERSION,
        "schema_id": "market_data_inputs",
        "schema_version": "v1",
        "freshness_ttl_seconds": int(freshness_ttl_seconds or 86400),
        "market_data_mode": str(row.get("market_data_mode") or "") if row else "",
        "finalization_status": str(row.get("finalization_status") or "") if row else "",
        "usable_for": row.get("usable_for") if isinstance(row.get("usable_for"), dict) else {},
        "validation_status": validation,
        "required": bool(required),
        "input_classification": "SLEEVE_CRITICAL" if required else "ADVISORY_ONLY",
        "reason": reason,
    }


def _validation_status(*, day_utc: str, field_type: str, value: Any, source_timestamp: str, session_date: str, row: dict[str, Any], requested_mode: str) -> tuple[str, str]:
    if not row:
        return "MISSING", "NO_PROVIDER_RECORD"
    if not source_timestamp:
        return "MALFORMED", "SOURCE_TIMESTAMP_MISSING"
    if source_timestamp[:10] > day_utc:
        return "MALFORMED", "SOURCE_TIMESTAMP_FUTURE_DATED"
    if session_date != day_utc or str(row.get("freshness_status") or "").upper() == "STALE":
        return "STALE", f"SOURCE_SESSION_NOT_CURRENT:{session_date or 'UNKNOWN'}"
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return "MALFORMED", "VALUE_NOT_NUMERIC"
    if number <= 0:
        return "MALFORMED", "VALUE_NOT_POSITIVE"
    if str(row.get("provider") or row.get("source") or "").upper() in {"", "DEMO", "PLACEHOLDER"}:
        return "MALFORMED", "SOURCE_VENDOR_INVALID"
    finality = str(row.get("data_finality") or "").upper()
    record_mode = str(row.get("market_data_mode") or "").upper()
    if normalize_market_data_mode_v1(requested_mode) == FINAL_EOD_CERTIFIED and finality != "FINAL_EOD":
        return "STALE", "FINAL_EOD_CERTIFICATION_PENDING"
    if normalize_market_data_mode_v1(requested_mode) == INTRADAY_OPERATIONAL and record_mode == "STALE_PRIOR_DAY":
        return "STALE", "STALE_PRIOR_DAY_CANNOT_SATISFY_INTRADAY"
    return "VALID", "Evidence is current, positive, sourced, hash-backed, and valid for the requested market data mode."


def _raw_source_hash(row: dict[str, Any]) -> str:
    if not row:
        return ""
    source_path = Path(str(row.get("source_url_or_path") or ""))
    if source_path.exists() and source_path.is_file():
        return hashlib.sha256(source_path.read_bytes()).hexdigest()
    return str(row.get("source_hash") or "")


def _overall_status(records: list[dict[str, Any]], required_ids: list[str]) -> str:
    required = [row for row in records if row.get("data_item_id") in set(required_ids)]
    if not required:
        return "NO_REQUIRED_INPUTS"
    if all(row.get("validation_status") == "VALID" for row in required):
        return "READY"
    if any(row.get("validation_status") == "VALID" for row in required):
        return "PARTIAL"
    if any(row.get("validation_status") == "STALE" for row in required):
        return "STALE"
    return "BLOCKED"
