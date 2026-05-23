from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.domain_source_registry_v1 import domain_source_contract_v1, render_domain_source_path_v1
from ops.aegis.market_calendar.session_calendar_v1 import session_classification_v1
from ops.aegis.market_data.market_data_mode_v1 import FINAL_EOD_CERTIFIED
from ops.aegis.market_data.market_data_provider_v1 import fetch_market_data_v1, provider_config_from_env_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, normalize_market_symbol_v1
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1
from ops.aegis.universe.canonical_universe_authority_v1 import (
    canonical_universe_authority_path,
    latest_canonical_universe_authority_v1,
)


SCHEMA_VERSION = "v1"

DOMAIN_CONFIG: dict[str, dict[str, Any]] = {
    "US_EQUITIES_EOD": {
        "schema_id": "final_eod_market_data_v1",
        "events_key": "",
        "source_env": "AEGIS_MARKET_DATA_PROVIDER_PRIMARY",
        "source_file_env": "AEGIS_US_EQUITIES_EOD_SOURCE_FILE",
        "alternate_source_file_envs": ["AEGIS_FINAL_EOD_MARKET_DATA_SOURCE_FILE"],
        "required_fields": ["symbol", "date", "open", "high", "low", "close", "volume"],
        "setup_message": "Upload/configure EOD source with AEGIS_US_EQUITIES_EOD_SOURCE_FILE as CSV or JSON, or configure a certification-grade EOD provider.",
    },
    "MACRO_CALENDAR": {
        "schema_id": "macro_calendar_v1",
        "events_key": "events",
        "source_env": "AEGIS_MACRO_CALENDAR_SOURCE_FILE",
        "required_fields": ["event_name", "time", "country", "importance", "source"],
        "setup_message": "Set AEGIS_MACRO_CALENDAR_SOURCE_FILE to a governed JSON export or place the artifact at the required path.",
    },
    "EARNINGS_EVENTS": {
        "schema_id": "earnings_events_v1",
        "events_key": "events",
        "source_env": "AEGIS_EARNINGS_EVENTS_SOURCE_FILE",
        "required_fields": ["symbol", "company", "report_date", "report_time", "confirmed_or_estimated", "source"],
        "setup_message": "Set AEGIS_EARNINGS_EVENTS_SOURCE_FILE to a governed earnings calendar JSON export or place the artifact at the required path.",
    },
    "CORPORATE_ACTIONS": {
        "schema_id": "corporate_actions_v1",
        "events_key": "actions",
        "source_env": "AEGIS_CORPORATE_ACTIONS_SOURCE_FILE",
        "required_fields": ["symbol", "action_type", "effective_date", "source"],
        "setup_message": "Set AEGIS_CORPORATE_ACTIONS_SOURCE_FILE to a governed corporate-actions JSON export or place the artifact at the required path.",
    },
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    clean = {k: v for k, v in dict(payload or {}).items() if k not in {"content_hash", "artifact_hash"}}
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def sha256_file_v1(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json_v1(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json_v1(path: Path, payload: dict[str, Any]) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["content_hash"] = stable_hash_v1(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"json": str(path), "sha256": sha256_file_v1(path), "content_hash": str(payload["content_hash"])}



def _eod_artifact_store_dir(root: Path, day_utc: str) -> Path:
    return root / "reports" / "final_eod_market_data_v1" / day_utc / "artifacts"


def _eod_content_artifact_path(root: Path, day_utc: str, content_hash: str) -> Path:
    return _eod_artifact_store_dir(root, day_utc) / str(content_hash) / "final_eod_market_data.v1.json"


def _eod_current_manifest_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"


def _resolve_eod_payload_path_v1(path: Path) -> tuple[dict[str, Any], Path | None]:
    payload = read_json_v1(path)
    if payload.get("schema_id") == "final_eod_market_data_current_manifest.v1":
        artifact_path = Path(str(payload.get("current_artifact_path") or ""))
        artifact = read_json_v1(artifact_path) if artifact_path.exists() else {}
        return artifact, artifact_path if artifact else path
    return payload, path if payload else None


def _write_immutable_final_eod_artifact_v1(*, root: Path, day_utc: str, manifest_path: Path, payload: dict[str, Any]) -> dict[str, str]:
    payload = dict(payload)
    payload["content_hash"] = stable_hash_v1(payload)
    content_hash = str(payload["content_hash"])
    artifact_path = _eod_content_artifact_path(root, day_utc, content_hash)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    if artifact_path.exists():
        existing_hash = sha256_file_v1(artifact_path)
        expected_body = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        if artifact_path.read_text(encoding="utf-8") != expected_body:
            raise ValueError(f"immutable final EOD artifact hash collision or mutation attempt: {artifact_path}")
    else:
        artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
        existing_hash = sha256_file_v1(artifact_path)
    manifest = {
        "schema_id": "final_eod_market_data_current_manifest.v1",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "updated_at_utc": utc_now_v1(),
        "current_artifact_path": str(artifact_path),
        "current_artifact_sha256": existing_hash,
        "current_artifact_content_hash": content_hash,
        "status": str(payload.get("status") or ""),
        "validation_status": str(payload.get("validation_status") or ""),
        "final_eod_certification_status": str(payload.get("final_eod_certification_status") or ""),
        "final_eod_symbols_count": len(payload.get("final_eod_symbols") or []),
        "requested_symbols_count": len(payload.get("requested_symbols") or []),
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    manifest_paths = write_json_v1(manifest_path, manifest)
    sidecar = manifest_path.with_name("final_eod_market_data.current.v1.json")
    if sidecar != manifest_path:
        write_json_v1(sidecar, manifest)
    return {
        "json": str(artifact_path),
        "sha256": existing_hash,
        "content_hash": content_hash,
        "current_manifest_json": manifest_paths["json"],
        "current_manifest_sha256": manifest_paths["sha256"],
        "current_manifest_content_hash": manifest_paths["content_hash"],
    }

def setup_requirements_v1(*, truth_root: Path | str, day_utc: str, domain_id: str) -> dict[str, Any]:
    did = str(domain_id or "").strip().upper()
    contract = domain_source_contract_v1(did)
    output_path = render_domain_source_path_v1(truth_root=truth_root, day_utc=day_utc, contract=contract) if contract else Path("")
    cfg = DOMAIN_CONFIG.get(did, {})
    return {
        "domain_id": did,
        "day_utc": day_utc,
        "required_output_path": str(output_path),
        "source_env": str(cfg.get("source_env") or ""),
        "source_file_env": str(cfg.get("source_file_env") or cfg.get("source_env") or ""),
        "required_config_keys": [key for key in [cfg.get("source_env"), cfg.get("source_file_env"), *(cfg.get("alternate_source_file_envs") or [])] if key],
        "accepted_format": "JSON object with schema_id/status fields and domain-specific rows.",
        "required_fields": list(cfg.get("required_fields") or []),
        "setup_message": str(cfg.get("setup_message") or contract.get("setup_required_message") or "Configure a governed source for this domain."),
        "source_contract": contract,
    }


def _status_valid(payload: dict[str, Any]) -> bool:
    text = " ".join(str(payload.get(key) or "") for key in ("status", "validation_status", "certification_status", "final_eod_certification_status", "certification_state")).upper()
    return any(token in text for token in ("VALID", "CERTIFIED", "READY", "PASS", "CURRENT"))


def _source_setup_required(*, truth_root: Path, day_utc: str, domain_id: str, reason: str = "SOURCE_SETUP_REQUIRED") -> dict[str, Any]:
    req = setup_requirements_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
    return {
        "ok": False,
        "status": "SOURCE_SETUP_REQUIRED",
        "result_status": "SOURCE_SETUP_REQUIRED",
        "domain_id": domain_id,
        "day_utc": day_utc,
        "reason": reason,
        "message": f"Source setup required for {domain_id}: missing governed source. {req["setup_message"]}",
        "required_output_path": req["required_output_path"],
        "required_config_keys": req["required_config_keys"],
        "accepted_format": req["accepted_format"],
        "required_fields": req["required_fields"],
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def _configured_source_path(domain_id: str) -> Path | None:
    cfg = DOMAIN_CONFIG.get(domain_id, {})
    primary_file_env = str(cfg.get("source_file_env") or cfg.get("source_env") or "")
    env_names = [primary_file_env, *[str(item or "") for item in (cfg.get("alternate_source_file_envs") or [])]]
    for env_name in env_names:
        if not env_name:
            continue
        value = os.environ.get(env_name)
        if value:
            return Path(value).expanduser().resolve()
    return None


def _rows_from_payload(payload: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
    return []


def _value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value not in {None, ""}:
            return str(value)
    return ""


def _validate_rows(domain_id: str, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if domain_id == "MACRO_CALENDAR":
            item = {
                "event_name": _value(row, "event_name", "name", "event"),
                "time": _value(row, "time", "event_time", "time_utc", "datetime_utc"),
                "country": _value(row, "country", "region"),
                "importance": _value(row, "importance", "impact", "priority"),
                "source": _value(row, "source", "provider"),
            }
        elif domain_id == "EARNINGS_EVENTS":
            item = {
                "symbol": _value(row, "symbol", "ticker").upper(),
                "company": _value(row, "company", "company_name", "name"),
                "report_date": _value(row, "report_date", "date", "earnings_date"),
                "report_time": _value(row, "report_time", "time", "timing"),
                "confirmed_or_estimated": _value(row, "confirmed_or_estimated", "confirmed", "estimated", "status"),
                "source": _value(row, "source", "provider"),
            }
        elif domain_id == "CORPORATE_ACTIONS":
            item = {
                "symbol": _value(row, "symbol", "ticker").upper(),
                "action_type": _value(row, "action_type", "type", "corporate_action_type"),
                "effective_date": _value(row, "effective_date", "date", "ex_date"),
                "source": _value(row, "source", "provider"),
            }
        else:
            item = dict(row)
        missing = [field for field in DOMAIN_CONFIG.get(domain_id, {}).get("required_fields", []) if not str(item.get(field) or "").strip()]
        if missing:
            errors.append(f"row_{index}_missing:{','.join(missing)}")
        normalized.append(item)
    if not rows:
        errors.append("NO_ROWS")
    return normalized, errors


def _source_payload_for_domain(*, truth_root: Path, day_utc: str, domain_id: str, output_path: Path) -> tuple[Any, Path | None]:
    configured = _configured_source_path(domain_id)
    if configured and configured.exists() and configured.is_file():
        return read_json_v1(configured), configured
    if output_path.exists() and output_path.is_file():
        return read_json_v1(output_path), output_path
    return {}, configured


def build_external_domain_source_v1(*, truth_root: Path | str, day_utc: str, domain_id: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    did = str(domain_id or "").strip().upper()
    contract = domain_source_contract_v1(did)
    if did not in {"MACRO_CALENDAR", "EARNINGS_EVENTS", "CORPORATE_ACTIONS"} or not contract:
        return _source_setup_required(truth_root=root, day_utc=day_utc, domain_id=did, reason="DOMAIN_SOURCE_CONTRACT_MISSING")
    output_path = render_domain_source_path_v1(truth_root=root, day_utc=day_utc, contract=contract)
    raw, source_path = _source_payload_for_domain(truth_root=root, day_utc=day_utc, domain_id=did, output_path=output_path)
    if not raw:
        return _source_setup_required(truth_root=root, day_utc=day_utc, domain_id=did, reason="CONFIGURED_SOURCE_MISSING")
    rows_key = str(DOMAIN_CONFIG[did]["events_key"])
    rows = _rows_from_payload(raw, (rows_key, "events", "actions", "calendar_rows", "rows"))
    normalized, errors = _validate_rows(did, rows)
    if errors:
        return {
            **_source_setup_required(truth_root=root, day_utc=day_utc, domain_id=did, reason="SOURCE_VALIDATION_FAILED"),
            "status": "INVALID",
            "result_status": "INVALID_SOURCE",
            "errors": errors,
            "message": f"{did} source exists but failed validation: {', '.join(errors[:5])}.",
        }
    payload = {
        "schema_id": DOMAIN_CONFIG[did]["schema_id"],
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": utc_now_v1(),
        "status": "READY",
        "validation_status": "VALID",
        "source": str(source_path or output_path),
        rows_key: normalized,
        "row_count": len(normalized),
        "tracked_symbols": sorted({str(row.get("symbol") or "").upper() for row in normalized if str(row.get("symbol") or "")}),
        "completeness_score": "1.000000",
        "lineage": {
            "source_path": str(source_path or output_path),
            "source_sha256": sha256_file_v1(source_path) if source_path and source_path.exists() else "",
            "source_contract": contract,
        },
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    paths = write_json_v1(output_path, payload)
    return {
        "ok": True,
        "status": "BUILT",
        "result_status": "BUILT",
        "domain_id": did,
        "day_utc": day_utc,
        "artifact_path": paths["json"],
        "artifact_hash": paths["sha256"],
        "content_hash": paths["content_hash"],
        "row_count": len(normalized),
        "message": f"Built {did} source artifact with {len(normalized)} rows.",
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def _repo_root_v1() -> Path:
    return Path(__file__).resolve().parents[3]


def _governed_required_eod_universe_v1(root: Path | None, day_utc: str) -> dict[str, Any]:
    if root is not None:
        try:
            authority = latest_canonical_universe_authority_v1(truth_root=Path(root), day_utc=day_utc)
        except Exception:
            authority = {}
        symbols = canonicalize_symbol_list_v1(authority.get("universe_symbols", []) if isinstance(authority, dict) else [])
        if symbols and str(authority.get("authority_status") or "").upper() == "PASS":
            path = canonical_universe_authority_path(truth_root=Path(root), day_utc=day_utc)
            return {
                "symbols": symbols,
                "source": "canonical_universe_authority_v1",
                "source_artifact_path": str(path if path.exists() else ""),
                "source_hash": str(authority.get("immutable_hash") or ""),
                "symbol_count": len(symbols),
            }
    try:
        symbol_map = build_symbol_map_v1(repo_root=_repo_root_v1(), day_utc=day_utc)
        raw = symbol_map.get("required_symbols") if isinstance(symbol_map.get("required_symbols"), list) else []
        symbols = canonicalize_symbol_list_v1(raw)
    except Exception:
        symbols = []
    return {
        "symbols": symbols,
        "source": "symbol_map_required_symbols",
        "source_artifact_path": "",
        "source_hash": "",
        "symbol_count": len(symbols),
    }


def _required_eod_symbols_v1(day_utc: str, truth_root: Path | None = None) -> list[str]:
    return list(_governed_required_eod_universe_v1(truth_root, day_utc).get("symbols") or [])


def _num_v1(value: Any) -> float | int | None:
    if value in {None, ""}:
        return None
    try:
        number = float(str(value).strip())
    except Exception:
        return None
    return int(number) if number.is_integer() else number


def _manual_eod_rows_v1(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            return [dict(row) for row in csv.DictReader(path.read_text(encoding="utf-8").splitlines())]
        except Exception:
            return []
    payload = read_json_v1(path)
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        symbols = payload.get("symbols")
        if isinstance(symbols, dict):
            rows = []
            for symbol, row in symbols.items():
                if isinstance(row, dict):
                    rows.append({"symbol": symbol, **row})
            return rows
        for key in ("rows", "records", "data", "prices", "ohlcv"):
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(row) for row in value if isinstance(row, dict)]
    return []


def _normalize_manual_eod_source_v1(*, source_path: Path, day_utc: str, required_symbols: list[str]) -> tuple[dict[str, Any], list[str]]:
    rows = _manual_eod_rows_v1(source_path)
    errors: list[str] = []
    by_symbol: dict[str, dict[str, Any]] = {}
    invalid_rows: list[dict[str, str]] = []
    stale_symbols: list[str] = []
    for index, row in enumerate(rows):
        symbol = normalize_market_symbol_v1(_value(row, "symbol", "ticker", "canonical_symbol"))
        session = _value(row, "date", "session_date", "market_session_date", "day_utc", "trading_day")[:10]
        provider = _value(row, "provider", "source_vendor", "source") or "UPLOADED_EOD_SOURCE"
        if not symbol:
            invalid_rows.append({"row": str(index), "symbol": "", "reason": "MISSING_SYMBOL"})
            continue
        open_ = _num_v1(_value(row, "open", "Open"))
        high = _num_v1(_value(row, "high", "High"))
        low = _num_v1(_value(row, "low", "Low"))
        close = _num_v1(_value(row, "close", "Close", "last", "Last"))
        volume = _num_v1(_value(row, "volume", "Volume"))
        missing = [name for name, value in (("date", session), ("open", open_), ("high", high), ("low", low), ("close", close), ("volume", volume)) if value in {None, ""}]
        invalid_ohlcv = bool(open_ is not None and high is not None and low is not None and close is not None and not (float(low) <= min(float(open_), float(close)) <= max(float(open_), float(close)) <= float(high)))
        invalid_volume = bool(volume is not None and float(volume) < 0)
        if missing or invalid_ohlcv or invalid_volume:
            reasons = list(missing)
            if invalid_ohlcv:
                reasons.append("OHLC_RANGE")
            if invalid_volume:
                reasons.append("NEGATIVE_VOLUME")
            invalid_rows.append({"row": str(index), "symbol": symbol, "reason": ",".join(reasons)})
            continue
        if session != day_utc:
            stale_symbols.append(symbol)
        timestamp = _value(row, "timestamp_utc", "source_timestamp_utc", "datetime_utc") or f"{session}T21:00:00Z"
        by_symbol[symbol] = {
            "symbol": symbol,
            "canonical_symbol": symbol,
            "provider": provider,
            "provider_symbol": _value(row, "provider_symbol") or symbol,
            "last_price": close,
            "close": close,
            "open": open_,
            "high": high,
            "low": low,
            "volume": volume,
            "data_timestamp_utc": timestamp,
            "source_timestamp_utc": timestamp,
            "market_session_date": session,
            "source": provider,
            "source_url_or_path": str(source_path),
            "source_hash": sha256_file_v1(source_path),
            "freshness_status": "CURRENT" if session == day_utc else "STALE",
            "data_finality": "FINAL_EOD",
            "market_data_mode": "FINAL_EOD_CERTIFIED",
            "finalization_status": "FINAL" if session == day_utc else "FINAL_UNAVAILABLE",
            "usable_for": {"sleeve_intraday_generation": True, "manual_capture": True, "final_eod_certification": session == day_utc},
            "candidate_generation_eligible": session == day_utc,
            "quality": "HIGH" if session == day_utc else "LOW",
        }
    requested = canonicalize_symbol_list_v1(required_symbols or list(by_symbol))
    final_symbols = sorted(symbol for symbol in requested if symbol in by_symbol and str(by_symbol[symbol].get("freshness_status") or "") == "CURRENT")
    missing_symbols = sorted(set(requested) - set(final_symbols))
    if not rows:
        errors.append("NO_SOURCE_ROWS")
    if missing_symbols:
        errors.append("MISSING_SYMBOL_COVERAGE")
    if stale_symbols:
        errors.append("STALE_PROVIDER_ROWS")
    if invalid_rows:
        errors.append("INVALID_OHLCV")
    status = "CURRENT" if not errors else _eod_failure_status_v1(errors)
    payload = {
        "schema_id": "final_eod_market_data_v1",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "market_session_date": day_utc,
        "generated_at_utc": utc_now_v1(),
        "status": status,
        "validation_status": "VALID" if not errors else "REJECTED",
        "certification_state": "CERTIFIED" if not errors else "FAILED",
        "final_eod_certification_status": "VALID" if not errors else "REJECTED",
        "final_eod_ready": not errors,
        "market_data_mode": "FINAL_EOD_CERTIFIED",
        "source": "UPLOADED_EOD_SOURCE",
        "requested_symbols": requested,
        "fetched_symbols": sorted(by_symbol),
        "final_eod_symbols": final_symbols,
        "symbols": by_symbol,
        "missing_symbols": missing_symbols,
        "stale_symbols": sorted(set(stale_symbols)),
        "invalid_ohlcv_rows": invalid_rows,
        "provider_results": [{
            "provider": "UPLOADED_EOD_SOURCE",
            "request_status": "SUCCESS" if not errors else "FAILED",
            "failure_reason": ",".join(errors),
            "returned_data_date": day_utc,
            "fetched_symbols": final_symbols,
            "missing_symbols": missing_symbols,
            "stale_symbols": sorted(set(stale_symbols)),
        }],
        "provider_coverage_action_item": {
            "status": "OK" if not errors else "PROVIDER_COVERAGE_INCOMPLETE",
            "message": "Uploaded EOD source covers required universe." if not errors else "Uploaded EOD source failed certification coverage validation",
            "required_universe_size": len(requested),
            "covered_count": len(final_symbols),
            "unsupported_symbols": [],
            "timeout_symbols": [],
            "stale_symbols": sorted(set(stale_symbols)),
            "missing_symbols": missing_symbols,
            "recommended_fix": "" if not errors else "Upload/configure EOD source with complete target-day OHLCV for every governed symbol.",
        },
        "certification_eligible": not errors,
        "lineage": {
            "source_path": str(source_path),
            "source_sha256": sha256_file_v1(source_path),
            "source_type": "UPLOADED_CSV" if source_path.suffix.lower() == ".csv" else "UPLOADED_JSON",
            "provider": "UPLOADED_EOD_SOURCE",
            "certification_eligibility": True,
        },
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    return payload, errors


def _legacy_eod_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "market_data_final_eod_v1" / day_utc / "market_data_final_eod.v1.json"


def _canonical_eod_path(root: Path, day_utc: str) -> Path:
    contract = domain_source_contract_v1("US_EQUITIES_EOD")
    path = render_domain_source_path_v1(truth_root=root, day_utc=day_utc, contract=contract) if contract else Path("")
    return path if path else root / "reports" / "final_eod_market_data_v1" / day_utc / "final_eod_market_data.v1.json"


def _symbols_payload_v1(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    symbols = payload.get("symbols")
    if isinstance(symbols, dict):
        return {str(k).upper(): dict(v) for k, v in symbols.items() if isinstance(v, dict)}
    return {}


def _final_eod_symbols_v1(payload: dict[str, Any], day_utc: str) -> set[str]:
    explicit = {str(symbol).upper() for symbol in payload.get("final_eod_symbols", []) if str(symbol)}
    if explicit:
        return explicit
    out: set[str] = set()
    symbol_payload = _symbols_payload_v1(payload)
    for symbol, row in symbol_payload.items():
        if (
            str(row.get("market_session_date") or "") == day_utc
            and str(row.get("freshness_status") or "").upper() == "CURRENT"
            and str(row.get("data_finality") or "").upper() == "FINAL_EOD"
            and bool((row.get("usable_for") if isinstance(row.get("usable_for"), dict) else {}).get("final_eod_certification", True))
        ):
            out.add(symbol)
    if not symbol_payload and _status_valid(payload):
        out.update(str(symbol).upper() for symbol in payload.get("fetched_symbols", []) if str(symbol))
    return out


def _invalid_ohlcv_rows_v1(payload: dict[str, Any], day_utc: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for symbol, row in sorted(_symbols_payload_v1(payload).items()):
        if symbol not in _final_eod_symbols_v1(payload, day_utc):
            continue
        required = ["open", "high", "low", "close"]
        if symbol not in {"VIX", "VIXY"} and str(row.get("data_type") or "").upper() not in {"VOLATILITY", "INDEX", "RATE"}:
            required.append("volume")
        missing = [field for field in required if row.get(field) in {None, ""}]
        if missing:
            rows.append({"symbol": symbol, "missing_fields": ",".join(missing)})
    return rows


def _eod_payload_valid(payload: dict[str, Any], day_utc: str) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not payload:
        errors.append("FINAL_EOD_PAYLOAD_MISSING")
        return False, errors
    status_text = " ".join(str(payload.get(key) or "") for key in ("status", "validation_status", "certification_state", "final_eod_certification_status")).upper()
    requested = {str(symbol).upper() for symbol in payload.get("requested_symbols", []) if str(symbol)}
    final_symbols = _final_eod_symbols_v1(payload, day_utc)
    stale_symbols = {str(symbol).upper() for symbol in payload.get("stale_symbols", []) if str(symbol)}
    stale_symbols.update(symbol for symbol, row in _symbols_payload_v1(payload).items() if str(row.get("freshness_status") or "").upper() == "STALE")
    invalid_rows = _invalid_ohlcv_rows_v1(payload, day_utc)
    if isinstance(payload.get("invalid_ohlcv_rows"), list):
        invalid_rows.extend(row for row in payload.get("invalid_ohlcv_rows", []) if isinstance(row, dict))
    if requested and not requested <= final_symbols:
        errors.append("MISSING_SYMBOL_COVERAGE")
    if requested and stale_symbols & requested:
        errors.append("STALE_PROVIDER_ROWS")
    if invalid_rows:
        errors.append("INVALID_OHLCV")
    session = str(payload.get("market_session_date") or payload.get("day_utc") or day_utc)
    if session != day_utc:
        errors.append("FINAL_EOD_MARKET_SESSION_MISMATCH")
    if not errors and not any(token in status_text for token in ("VALID", "CERTIFIED", "PASS", "READY", "CURRENT")):
        errors.append("FINAL_EOD_STATUS_NOT_VALID")
    return not errors, errors


def _eod_validation_details_v1(payload: dict[str, Any], day_utc: str, errors: list[str]) -> dict[str, Any]:
    requested = sorted({str(symbol).upper() for symbol in payload.get("requested_symbols", []) if str(symbol)})
    final_symbols = sorted(_final_eod_symbols_v1(payload, day_utc))
    final_symbol_set = set(final_symbols)
    requested_set = set(requested)
    stale_symbols = sorted({str(symbol).upper() for symbol in payload.get("stale_symbols", []) if str(symbol)} | {symbol for symbol, row in _symbols_payload_v1(payload).items() if str(row.get("freshness_status") or "").upper() == "STALE"})
    stale_date_symbols = sorted(symbol for symbol in stale_symbols if symbol in requested_set)
    invalid_rows = _invalid_ohlcv_rows_v1(payload, day_utc)
    if isinstance(payload.get("invalid_ohlcv_rows"), list):
        invalid_rows.extend(row for row in payload.get("invalid_ohlcv_rows", []) if isinstance(row, dict))
    provider_results: list[dict[str, Any]] = []
    for row in payload.get("provider_results", []):
        if not isinstance(row, dict):
            continue
        provider_results.append({
            "provider": str(row.get("provider") or ""),
            "request_status": str(row.get("request_status") or row.get("status") or ""),
            "failure_reason": str(row.get("failure_reason") or ""),
            "returned_data_date": str(row.get("returned_data_date") or ""),
            "fetched_symbols": list(row.get("fetched_symbols") or []),
            "missing_symbols": list(row.get("missing_symbols") or []),
            "stale_symbols": list(row.get("stale_symbols") or []),
        })
    if not provider_results and isinstance(payload.get("provider_attempts"), list):
        by_provider: dict[str, dict[str, Any]] = {}
        for attempt in payload.get("provider_attempts", []):
            if not isinstance(attempt, dict):
                continue
            provider = str(attempt.get("provider") or "UNKNOWN")
            row = by_provider.setdefault(provider, {"provider": provider, "request_status": "OBSERVED", "failure_reason": "", "fetched_symbols": [], "missing_symbols": [], "stale_symbols": []})
            symbol = str(attempt.get("symbol") or "").upper()
            status = str(attempt.get("status") or "")
            if status == "SUCCESS":
                row["fetched_symbols"].append(symbol)
            else:
                row["missing_symbols"].append(symbol)
                row["failure_reason"] = row["failure_reason"] or str(attempt.get("rejected_reason") or status)
        provider_results = list(by_provider.values())
    return {
        "validation_errors": list(errors),
        "status_fields": {
            "status": str(payload.get("status") or ""),
            "validation_status": str(payload.get("validation_status") or ""),
            "certification_state": str(payload.get("certification_state") or ""),
            "final_eod_certification_status": str(payload.get("final_eod_certification_status") or ""),
            "final_eod_ready": bool(payload.get("final_eod_ready") is True),
        },
        "market_session": session_classification_v1(domain_id="US_EQUITIES_EOD", day_utc=day_utc),
        "expected_market_session_date": day_utc,
        "actual_market_session_date": str(payload.get("market_session_date") or payload.get("day_utc") or ""),
        "expected_symbol_count": len(requested),
        "actual_final_eod_symbol_count": len(final_symbols),
        "requested_symbols": requested,
        "final_eod_symbols": final_symbols,
        "missing_symbols": sorted(requested_set - final_symbol_set),
        "stale_date_symbols": stale_date_symbols,
        "invalid_ohlcv_rows": invalid_rows,
        "provider_results": provider_results,
        "provider_coverage_plan": payload.get("provider_coverage_plan") if isinstance(payload.get("provider_coverage_plan"), dict) else {},
        "provider_coverage_action_item": payload.get("provider_coverage_action_item") if isinstance(payload.get("provider_coverage_action_item"), dict) else {},
    }


def _eod_failure_status_v1(errors: list[str]) -> str:
    error_set = set(errors)
    if "FINAL_EOD_MARKET_SESSION_MISMATCH" in error_set:
        return "WRONG_MARKET_SESSION"
    if "INVALID_OHLCV" in error_set:
        return "INVALID_OHLCV"
    if "MISSING_SYMBOL_COVERAGE" in error_set:
        return "PROVIDER_INCOMPLETE"
    if "STALE_PROVIDER_ROWS" in error_set:
        return "STALE_PROVIDER_ROWS"
    return "PROVIDER_FAILED"


def _write_rejected_eod_artifact_v1(*, output_path: Path, payload: dict[str, Any], day_utc: str, source_used: Path | None, errors: list[str], provider_error: str) -> dict[str, str]:
    details = _eod_validation_details_v1(payload, day_utc, errors)
    rejected = {
        **(payload if isinstance(payload, dict) else {}),
        "schema_id": "final_eod_market_data_v1",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "trading_day": str((payload or {}).get("trading_day") or (payload or {}).get("market_session_date") or (payload or {}).get("day_utc") or day_utc),
        "market_session_date": str((payload or {}).get("market_session_date") or (payload or {}).get("day_utc") or day_utc),
        "status": _eod_failure_status_v1(errors),
        "validation_status": "REJECTED",
        "final_eod_certification_status": "REJECTED",
        "reason_codes": list(errors),
        "validation_details": details,
        "missing_symbols": details.get("missing_symbols") or [],
        "stale_date_symbols": details.get("stale_date_symbols") or [],
        "invalid_ohlcv_rows": details.get("invalid_ohlcv_rows") or [],
        "provider_build_error": provider_error,
        "generated_at_utc": str((payload or {}).get("generated_at_utc") or utc_now_v1()),
        "lineage": {
            **((payload or {}).get("lineage") if isinstance((payload or {}).get("lineage"), dict) else {}),
            "source_path": str(source_used or output_path),
            "source_sha256": sha256_file_v1(source_used) if source_used and source_used.exists() else "",
        },
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    return write_json_v1(output_path, rejected)



def _valid_eod_symbol_rows_v1(payload: dict[str, Any], day_utc: str, required_symbols: list[str]) -> dict[str, dict[str, Any]]:
    required = {normalize_market_symbol_v1(symbol) for symbol in required_symbols}
    valid: dict[str, dict[str, Any]] = {}
    for symbol, row in _symbols_payload_v1(payload).items():
        canonical = normalize_market_symbol_v1(row.get("canonical_symbol") or row.get("symbol") or symbol)
        if canonical not in required:
            continue
        row_payload = {**row, "canonical_symbol": canonical, "symbol": canonical}
        test_payload = {
            "requested_symbols": [canonical],
            "symbols": {canonical: row_payload},
            "market_session_date": day_utc,
            "status": "CURRENT",
            "validation_status": "VALID",
            "final_eod_certification_status": "VALID",
        }
        if _eod_payload_valid(test_payload, day_utc)[0]:
            valid[canonical] = row_payload
    return valid


def _provider_configured_v1() -> bool:
    cfg = provider_config_from_env_v1()
    return bool(cfg.primary or cfg.fallback or cfg.intraday_provider)


def _provider_fetch_report_from_rows_v1(*, root: Path, day_utc: str, base_payload: dict[str, Any], source_used: Path | None, force_provider_refresh: bool) -> tuple[dict[str, Any], Path | None, str]:
    try:
        symbol_map = build_symbol_map_v1(repo_root=Path(__file__).resolve().parents[3], day_utc=day_utc)
        governed_universe = _governed_required_eod_universe_v1(root, day_utc)
        required_symbols = canonicalize_symbol_list_v1(governed_universe.get("symbols") or [])
        if not required_symbols:
            required_symbols = canonicalize_symbol_list_v1([str(symbol) for symbol in symbol_map.get("required_symbols", []) if str(symbol)])
        existing_valid = {} if force_provider_refresh else _valid_eod_symbol_rows_v1(base_payload or {}, day_utc, list(required_symbols))
        symbols_to_fetch = list(required_symbols) if force_provider_refresh else sorted(set(required_symbols) - set(existing_valid))
        fetch_skipped = not symbols_to_fetch
        result = None
        if symbols_to_fetch:
            if not _provider_configured_v1():
                return {}, None, "FINAL_EOD_MARKET_DATA_PROVIDER_NOT_CONFIGURED"
            result = fetch_market_data_v1(
                truth_root=root,
                day_utc=day_utc,
                symbols=symbols_to_fetch,
                symbol_map=symbol_map,
                config_override=provider_config_from_env_v1(),
            )
        fetched_symbols = dict(existing_valid)
        if result is not None:
            fetched_symbols.update({str(symbol).upper(): dict(row) for symbol, row in (result.symbols or {}).items() if isinstance(row, dict)})
        final_symbols = sorted(symbol for symbol, row in fetched_symbols.items() if _eod_payload_valid({
            "requested_symbols": [symbol],
            "symbols": {symbol: row},
            "market_session_date": day_utc,
            "status": "CURRENT",
            "validation_status": "VALID",
            "final_eod_certification_status": "VALID",
        }, day_utc)[0])
        missing_symbols = sorted(set(required_symbols) - set(final_symbols))
        stale_symbols = sorted(symbol for symbol, row in fetched_symbols.items() if str(row.get("freshness_status") or "").upper() == "STALE")
        provider_results = list((base_payload or {}).get("provider_results") or []) if not force_provider_refresh else []
        provider_attempts = list((base_payload or {}).get("provider_attempts") or []) if not force_provider_refresh else []
        provider_coverage_plan = (base_payload or {}).get("provider_coverage_plan") if isinstance((base_payload or {}).get("provider_coverage_plan"), dict) else {}
        if result is not None:
            provider_results.extend(list(result.provider_results or []))
            provider_attempts.extend(list(result.provider_attempts or []))
            provider_coverage_plan = result.provider_coverage_plan or provider_coverage_plan
        now = utc_now_v1()
        status = "CURRENT" if not missing_symbols and not stale_symbols else "PARTIAL"
        payload = {
            "schema_id": "final_eod_market_data_v1",
            "schema_version": SCHEMA_VERSION,
            "day_utc": day_utc,
            "trading_day": day_utc,
            "market_session_date": day_utc,
            "status": status,
            "validation_status": "VALID" if status == "CURRENT" else "PARTIAL_DATA_AVAILABLE",
            "final_eod_certification_status": "VALID" if status == "CURRENT" else "UNAVAILABLE",
            "final_eod_ready": status == "CURRENT",
            "generated_at_utc": now,
            "requested_symbols": list(required_symbols),
            "governed_universe_source": str(governed_universe.get("source") or ""),
            "governed_universe_artifact_path": str(governed_universe.get("source_artifact_path") or ""),
            "governed_universe_hash": str(governed_universe.get("source_hash") or ""),
            "governed_universe_symbol_count": len(required_symbols),
            "fetched_symbols": sorted(fetched_symbols),
            "final_eod_symbols": final_symbols,
            "missing_symbols": missing_symbols,
            "stale_symbols": stale_symbols,
            "symbols": {symbol: fetched_symbols[symbol] for symbol in sorted(fetched_symbols)},
            "provider_results": provider_results,
            "provider_attempts": provider_attempts,
            "provider_coverage_plan": provider_coverage_plan,
            "provider_coverage_action_item": {
                "status": "OK" if status == "CURRENT" else "PROVIDER_COVERAGE_INCOMPLETE",
                "message": "Provider coverage complete." if status == "CURRENT" else "Provider coverage incomplete",
                "required_universe_size": len(required_symbols),
                "covered_count": len(final_symbols),
                "missing_symbols": missing_symbols,
                "stale_symbols": stale_symbols,
                "timeout_symbols": sorted({str(row.get("symbol") or "").upper() for row in provider_attempts if isinstance(row, dict) and str(row.get("status") or "").upper() in {"TIMEOUT", "RATE_LIMITED", "NOT_ATTEMPTED_DEADLINE_EXHAUSTED"}} - {""}),
                "unsupported_symbols": list((provider_coverage_plan or {}).get("unsupported_symbols") or []),
                "recommended_fix": "" if status == "CURRENT" else "Wait for provider rate limit recovery, configure fallback coverage, or rerun with --force-provider-refresh after resolving provider failures.",
            },
            "provider_fetch_status": "SKIPPED_VALID_ARTIFACT_EXISTS" if fetch_skipped else ("FORCED_PROVIDER_REFRESH" if force_provider_refresh else "FETCHED_MISSING_OR_STALE_SYMBOLS"),
            "provider_fetch_skipped": bool(fetch_skipped),
            "provider_fetch_symbols": symbols_to_fetch,
            "lineage": {
                **((base_payload or {}).get("lineage") if isinstance((base_payload or {}).get("lineage"), dict) else {}),
                "source_path": str(source_used or _canonical_eod_path(root, day_utc)),
                "source_type": "PROVIDER_EOD_CERTIFICATION",
                "provider_priority": ["TIINGO", "CBOE", "ALPHA_VANTAGE"],
                "provider_fetch_status": "SKIPPED_VALID_ARTIFACT_EXISTS" if fetch_skipped else ("FORCED_PROVIDER_REFRESH" if force_provider_refresh else "FETCHED_MISSING_OR_STALE_SYMBOLS"),
            },
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        }
        return payload, source_used or _canonical_eod_path(root, day_utc), ""
    except Exception as exc:
        return {}, None, str(exc)

def _rerun_domain_certification_v1(*, root: Path, day_utc: str) -> dict[str, Any]:
    try:
        from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1, write_domain_certification_report_v1

        report = build_domain_certification_report_v1(truth_root=root, day_utc=day_utc)
        paths = write_domain_certification_report_v1(truth_root=root, day_utc=day_utc, payload=report)
        status = ""
        for row in report.get("domains", []) if isinstance(report.get("domains"), list) else []:
            if isinstance(row, dict) and str(row.get("domain_id") or "") == "US_EQUITIES_EOD":
                status = str(row.get("certification_status") or "")
                break
        return {"ok": True, "paths": paths, "certification_status": status}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _build_provider_final_eod_payload_v1(*, root: Path, day_utc: str, base_payload: dict[str, Any] | None = None, source_used: Path | None = None, force_provider_refresh: bool = False) -> tuple[dict[str, Any], Path | None, str]:
    return _provider_fetch_report_from_rows_v1(root=root, day_utc=day_utc, base_payload=base_payload or {}, source_used=source_used, force_provider_refresh=force_provider_refresh)

def build_us_equities_eod_source_v1(*, truth_root: Path | str, day_utc: str, force_provider_refresh: bool = False, reuse_valid_artifact: bool = True) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    output_path = _canonical_eod_path(root, day_utc)
    source_path = _configured_source_path("US_EQUITIES_EOD")
    force_provider_rebuild = bool(force_provider_refresh) or str(os.environ.get("AEGIS_US_EQUITIES_EOD_FORCE_PROVIDER") or "").strip().lower() in {"1", "true", "yes", "on"}
    reuse_valid_artifact = bool(reuse_valid_artifact) and str(os.environ.get("AEGIS_US_EQUITIES_EOD_REUSE_VALID_ARTIFACT") or "true").strip().lower() not in {"0", "false", "no", "off"}
    payload: dict[str, Any] = {}
    source_used: Path | None = None
    provider_error = ""
    loaded_existing_artifact = False
    if source_path and source_path.exists() and source_path.is_file():
        required_symbols = _required_eod_symbols_v1(day_utc, truth_root=root)
        payload, _manual_errors = _normalize_manual_eod_source_v1(source_path=source_path, day_utc=day_utc, required_symbols=required_symbols)
        source_used = source_path
    elif source_path and not source_path.exists():
        return {
            **_source_setup_required(truth_root=root, day_utc=day_utc, domain_id="US_EQUITIES_EOD", reason="CONFIGURED_EOD_SOURCE_FILE_MISSING"),
            "status": "SOURCE_SETUP_REQUIRED",
            "result_status": "SOURCE_SETUP_REQUIRED",
            "message": f"Upload/configure EOD source: configured file does not exist: {source_path}",
        }
    else:
        if reuse_valid_artifact and not force_provider_rebuild:
            for candidate in [output_path, _legacy_eod_path(root, day_utc)]:
                if candidate and candidate.exists() and candidate.is_file():
                    value, resolved_candidate = _resolve_eod_payload_path_v1(candidate)
                    if isinstance(value, dict) and value:
                        payload = value
                        source_used = resolved_candidate or candidate
                        loaded_existing_artifact = True
                        break
            if payload:
                required_symbols = canonicalize_symbol_list_v1(_required_eod_symbols_v1(day_utc, truth_root=root))
                payload = {**payload, "requested_symbols": required_symbols}
        if not payload:
            payload, source_used, provider_error = _build_provider_final_eod_payload_v1(root=root, day_utc=day_utc, base_payload=payload, source_used=source_used, force_provider_refresh=force_provider_rebuild)
            loaded_existing_artifact = False
    valid, errors = _eod_payload_valid(payload, day_utc)
    valid_artifact_reused = bool(valid and loaded_existing_artifact and source_used and not source_path and reuse_valid_artifact and not force_provider_rebuild)
    configured_source_invalid = bool(source_path and source_used and source_path == source_used and errors)
    if not valid and not configured_source_invalid:
        rebuilt_payload, rebuilt_source, provider_error = _build_provider_final_eod_payload_v1(root=root, day_utc=day_utc, base_payload=payload, source_used=source_used, force_provider_refresh=force_provider_rebuild)
        if rebuilt_payload:
            payload, source_used = rebuilt_payload, rebuilt_source
            loaded_existing_artifact = False
            valid, errors = _eod_payload_valid(payload, day_utc)
    if not payload and provider_error:
        return {
            **_source_setup_required(truth_root=root, day_utc=day_utc, domain_id="US_EQUITIES_EOD", reason="FINAL_EOD_PROVIDER_BUILD_FAILED"),
            "status": "FAILED",
            "result_status": "FAILED",
            "message": f"Final EOD market data build failed: {provider_error}",
            "failure_reason": provider_error,
        }
    if not valid:
        validation_details = _eod_validation_details_v1(payload, day_utc, errors)
        paths = _write_rejected_eod_artifact_v1(output_path=output_path, payload=payload, day_utc=day_utc, source_used=source_used, errors=errors, provider_error=provider_error)
        return {
            **_source_setup_required(truth_root=root, day_utc=day_utc, domain_id="US_EQUITIES_EOD", reason="FINAL_EOD_VALIDATION_FAILED"),
            "status": _eod_failure_status_v1(errors),
            "result_status": "FAILED",
            "artifact_path": paths["json"],
            "artifact_hash": paths["sha256"],
            "content_hash": paths["content_hash"],
            "message": f"Final EOD market data failed validation: {', '.join(errors)}.",
            "failure_reason": ",".join(errors),
            "missing_symbols": validation_details.get("missing_symbols") or [],
            "stale_date_symbols": validation_details.get("stale_date_symbols") or [],
            "invalid_ohlcv_rows": validation_details.get("invalid_ohlcv_rows") or [],
            "provider_results": validation_details.get("provider_results") or [],
            "validation_details": validation_details,
            "provider_build_error": provider_error,
        }
    normalized = {
        **payload,
        "schema_id": "final_eod_market_data_v1",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "trading_day": str(payload.get("trading_day") or payload.get("market_session_date") or day_utc),
        "market_session_date": str(payload.get("market_session_date") or day_utc),
        "status": str(payload.get("status") or "CURRENT"),
        "validation_status": "VALID",
        "final_eod_certification_status": "VALID",
        "generated_at_utc": str(payload.get("generated_at_utc") or utc_now_v1()),
        "lineage": {
            **(payload.get("lineage") if isinstance(payload.get("lineage"), dict) else {}),
            "source_path": str(source_used or output_path),
            "source_sha256": sha256_file_v1(source_used) if source_used and source_used.exists() else "",
            "provider_fetch_status": "SKIPPED_VALID_ARTIFACT_EXISTS" if valid_artifact_reused else (payload.get("provider_fetch_status") or (payload.get("lineage") if isinstance(payload.get("lineage"), dict) else {}).get("provider_fetch_status") or "UNKNOWN"),
        },
        "provider_fetch_status": "SKIPPED_VALID_ARTIFACT_EXISTS" if valid_artifact_reused else (payload.get("provider_fetch_status") or (payload.get("lineage") if isinstance(payload.get("lineage"), dict) else {}).get("provider_fetch_status") or "UNKNOWN"),
        "provider_fetch_skipped": bool(valid_artifact_reused or payload.get("provider_fetch_skipped") is True),
        "provider_fetch_symbols": [] if valid_artifact_reused else list(payload.get("provider_fetch_symbols") or []),
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    if valid_artifact_reused:
        paths = {
            "json": str(source_used or output_path),
            "sha256": sha256_file_v1(source_used) if source_used and source_used.exists() else sha256_file_v1(output_path),
            "content_hash": str(payload.get("content_hash") or stable_hash_v1(payload)),
        }
    else:
        paths = _write_immutable_final_eod_artifact_v1(root=root, day_utc=day_utc, manifest_path=output_path, payload=normalized)
        if _legacy_eod_path(root, day_utc) != output_path and not _legacy_eod_path(root, day_utc).exists():
            write_json_v1(_legacy_eod_path(root, day_utc), {**normalized, "schema_id": "market_data_final_eod_v1"})
    certification_result = _rerun_domain_certification_v1(root=root, day_utc=day_utc)
    return {
        "ok": True,
        "status": "CERTIFIED_FROM_ARTIFACT" if normalized.get("provider_fetch_skipped") else "BUILT",
        "result_status": "CERTIFIED_FROM_ARTIFACT" if normalized.get("provider_fetch_skipped") else "BUILT",
        "domain_id": "US_EQUITIES_EOD",
        "day_utc": day_utc,
        "artifact_path": paths["json"],
        "artifact_hash": paths["sha256"],
        "content_hash": paths["content_hash"],
        "certification_rerun": certification_result,
        "certification_status": certification_result.get("certification_status", "") if certification_result.get("ok") else "",
        "message": "Confirmed final EOD market data artifact and re-ran domain certification." if normalized.get("provider_fetch_skipped") else "Built final EOD market data artifact and re-ran domain certification.",
        "provider_fetch_status": normalized.get("provider_fetch_status") or "UNKNOWN",
        "provider_fetch_skipped": bool(normalized.get("provider_fetch_skipped") is True),
        "provider_fetch_symbols": list(normalized.get("provider_fetch_symbols") or []),
        "broker_submit_transmit_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def build_domain_source_artifact_v1(*, truth_root: Path | str, day_utc: str, domain_id: str) -> dict[str, Any]:
    did = str(domain_id or "").strip().upper()
    if did == "US_EQUITIES_EOD":
        return build_us_equities_eod_source_v1(truth_root=truth_root, day_utc=day_utc)
    if did in {"MACRO_CALENDAR", "EARNINGS_EVENTS", "CORPORATE_ACTIONS"}:
        return build_external_domain_source_v1(truth_root=truth_root, day_utc=day_utc, domain_id=did)
    return _source_setup_required(truth_root=Path(truth_root).expanduser().resolve(), day_utc=day_utc, domain_id=did, reason="UNSUPPORTED_DOMAIN_SOURCE")
