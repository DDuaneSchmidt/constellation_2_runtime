from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA_ID = "immutable_market_data_snapshot"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "immutable_market_data_snapshot_v1"

PROVISIONAL_INTRADAY = "PROVISIONAL_INTRADAY"
PARTIAL_DATA_AVAILABLE = "PARTIAL_DATA_AVAILABLE"
STALE = "STALE"
CERTIFICATION_PENDING = "CERTIFICATION_PENDING"
CERTIFIED = "CERTIFIED"
INVALID = "INVALID"

CERTIFICATION_STATES = {
    PROVISIONAL_INTRADAY,
    PARTIAL_DATA_AVAILABLE,
    STALE,
    CERTIFICATION_PENDING,
    CERTIFIED,
    INVALID,
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")


def stable_hash_v1(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def _symbols(value: Any) -> list[str]:
    return sorted({str(symbol or "").strip().upper() for symbol in (value if isinstance(value, list) else []) if str(symbol or "").strip()})


def certification_state_from_market_report_v1(report: Mapping[str, Any]) -> str:
    explicit = str(report.get("certification_state") or "").strip().upper()
    if explicit in CERTIFICATION_STATES:
        return explicit
    freshness = str(report.get("freshness_state") or report.get("validation_status") or "").strip().upper()
    final_status = str(report.get("final_eod_certification_status") or "").strip().upper()
    final_ready = bool(report.get("final_eod_ready") is True or final_status in {"VALID", "PASS", "CERTIFIED"})
    if final_ready:
        return CERTIFIED
    if freshness == "VALIDATED_CURRENT_DAY":
        return CERTIFICATION_PENDING
    if freshness in {"PENDING_VENDOR_DATA"}:
        return CERTIFICATION_PENDING
    if freshness in {"PARTIAL_DATA_AVAILABLE"}:
        return PARTIAL_DATA_AVAILABLE
    if freshness in {"READ_ONLY_PRIOR_DAY_FALLBACK"}:
        return STALE
    if str(report.get("status") or "").upper() in {"FAILED", "INVALID"}:
        return INVALID
    return PROVISIONAL_INTRADAY


def candidate_lane_for_certification_state_v1(certification_state: str) -> str:
    return "CERTIFIED" if str(certification_state or "").upper() == CERTIFIED else "PROVISIONAL"


def execution_eligible_for_certification_state_v1(certification_state: str) -> bool:
    return str(certification_state or "").upper() == CERTIFIED


def build_market_data_snapshot_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    market_report: Mapping[str, Any],
    captured_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    certification_state = certification_state_from_market_report_v1(market_report)
    requested_symbols = _symbols(market_report.get("requested_symbols"))
    fetched_symbols = _symbols(market_report.get("fetched_symbols"))
    missing_symbols = _symbols(market_report.get("missing_symbols"))
    stale_symbols = _symbols(market_report.get("stale_symbols"))
    denominator = max(1, len(requested_symbols))
    completeness_score = round(max(0, len(set(requested_symbols) & set(fetched_symbols)) - len(stale_symbols)) / denominator, 6)
    content_basis = {
        "day_utc": day_utc,
        "vendor": str(market_report.get("vendor") or market_report.get("source") or ""),
        "source": str(market_report.get("source") or ""),
        "certification_state": certification_state,
        "freshness_state": str(market_report.get("freshness_state") or ""),
        "validation_status": str(market_report.get("validation_status") or ""),
        "requested_symbols": requested_symbols,
        "fetched_symbols": fetched_symbols,
        "missing_symbols": missing_symbols,
        "stale_symbols": stale_symbols,
        "symbols": market_report.get("symbols") if isinstance(market_report.get("symbols"), dict) else {},
        "normalized_records": market_report.get("normalized_records") if isinstance(market_report.get("normalized_records"), list) else [],
        "market_calendar": market_report.get("market_calendar") if isinstance(market_report.get("market_calendar"), dict) else {},
    }
    content_hash = stable_hash_v1(content_basis)
    snapshot_id = f"market-data-snapshot:{day_utc}:{certification_state}:{content_hash[:16]}"
    snapshot_path = root / "reports" / REPORT_FAMILY / day_utc / f"{snapshot_id}.json"
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "vendor": str(market_report.get("vendor") or market_report.get("source") or "UNKNOWN"),
        "source": str(market_report.get("source") or "UNKNOWN"),
        "trading_day": day_utc,
        "captured_at": str(captured_at_utc or market_report.get("generated_at_utc") or _now()),
        "completeness_score": completeness_score,
        "freshness_state": str(market_report.get("freshness_state") or ""),
        "certification_state": certification_state,
        "content_hash": content_hash,
        "lineage": {
            "market_report_artifact_id": str(market_report.get("artifact_id") or "aegis_market_data_v1"),
            "market_report_path": str(market_report.get("artifact_path") or ""),
            "market_report_generated_at_utc": str(market_report.get("generated_at_utc") or ""),
            "provider_attempts_artifact": str(market_report.get("provider_attempts_artifact") or ""),
            "dataset_snapshot_id": str(market_report.get("dataset_snapshot_id") or ""),
        },
        "requested_symbols": requested_symbols,
        "fetched_symbols": fetched_symbols,
        "missing_symbols": missing_symbols,
        "stale_symbols": stale_symbols,
        "market_calendar": market_report.get("market_calendar") if isinstance(market_report.get("market_calendar"), dict) else {},
        "candidate_lane": candidate_lane_for_certification_state_v1(certification_state),
        "execution_eligible": execution_eligible_for_certification_state_v1(certification_state),
        "read_only": not execution_eligible_for_certification_state_v1(certification_state),
        "safety": {
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_enabled": False,
        },
        "artifact_path": str(snapshot_path),
    }
    payload["artifact_hash"] = stable_hash_v1(payload)
    return payload


def write_market_data_snapshot_v1(*, truth_root: Path | str, snapshot: Mapping[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    day = str(snapshot["trading_day"])
    snapshot_id = str(snapshot["snapshot_id"])
    path = root / "reports" / REPORT_FAMILY / day / f"{snapshot_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(snapshot)
    payload["artifact_path"] = str(path)
    data = _json_bytes(payload) + b"\n"
    if path.exists() and path.read_bytes() != data:
        raise ValueError(f"immutable market-data snapshot collision: {path}")
    if not path.exists():
        path.write_bytes(data)
    index_path = path.parent / "index.json"
    entries = []
    if index_path.exists():
        try:
            current = json.loads(index_path.read_text(encoding="utf-8"))
            entries = current.get("snapshots") if isinstance(current.get("snapshots"), list) else []
        except Exception:
            entries = []
    by_id = {str(row.get("snapshot_id")): dict(row) for row in entries if isinstance(row, dict)}
    by_id[snapshot_id] = {
        "snapshot_id": snapshot_id,
        "path": str(path),
        "hash": stable_hash_v1(payload),
        "certification_state": str(payload.get("certification_state") or ""),
        "candidate_lane": str(payload.get("candidate_lane") or ""),
        "captured_at": str(payload.get("captured_at") or ""),
    }
    index_payload = {
        "schema_id": "immutable_market_data_snapshot_index",
        "schema_version": "v1",
        "trading_day": day,
        "snapshot_count": len(by_id),
        "snapshots": [by_id[key] for key in sorted(by_id)],
        "updated_at_utc": _now(),
    }
    index_path.write_bytes(_json_bytes(index_payload) + b"\n")
    return {"json": str(path), "index": str(index_path), "snapshot_id": snapshot_id, "hash": stable_hash_v1(payload)}


def read_market_data_snapshot_v1(*, truth_root: Path | str, day_utc: str, snapshot_id: str) -> dict[str, Any]:
    path = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / f"{snapshot_id}.json"
    return json.loads(path.read_text(encoding="utf-8"))
