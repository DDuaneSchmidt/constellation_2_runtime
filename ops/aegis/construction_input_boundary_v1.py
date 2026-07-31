from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.paper_session_ledger_v1 import resolve_scheduled_paper_session_v1

REPORT_FAMILY = "aegis_construction_input_boundary_v1"
REPORT_FILENAME = "construction_input_boundary.v1.json"
SCHEMA_ID = "aegis_construction_input_boundary"
SCHEMA_VERSION = "v1"

READY_FOR_CONSTRUCTION = "READY_FOR_CONSTRUCTION"
CONSTRUCTED = "CONSTRUCTED"
REJECTED_BY_CONSTRUCTION = "REJECTED_BY_CONSTRUCTION"
CONSTRUCTION_BLOCKED_MARKET_DATA = "CONSTRUCTION_BLOCKED_MARKET_DATA"
CONSTRUCTION_BLOCKED_MISSING_CONTRACT = "CONSTRUCTION_BLOCKED_MISSING_CONTRACT"
CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD = "CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD"
NOT_ELIGIBLE_FOR_CONSTRUCTION = "NOT_ELIGIBLE_FOR_CONSTRUCTION"
BOUNDARY_VIOLATION_SILENT_OMISSION = "BOUNDARY_VIOLATION_SILENT_OMISSION"

BLOCKED_STATUSES = {
    CONSTRUCTION_BLOCKED_MARKET_DATA,
    CONSTRUCTION_BLOCKED_MISSING_CONTRACT,
    CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD,
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def construction_input_boundary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows(payload: Mapping[str, Any], *keys: str) -> list[Mapping[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
    return []


def _text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _symbol(row: Mapping[str, Any]) -> str:
    return _text(row.get("symbol"), row.get("ticker")).upper()


def _candidate_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_id"), row.get("candidate_contract_id"), row.get("trade_candidate_id"), row.get("id"))


def _candidate_contract_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_contract_id"), row.get("candidate_id"), row.get("contract_id"))


def _index_rows(rows: list[Mapping[str, Any]]) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    by_candidate: dict[str, Mapping[str, Any]] = {}
    by_contract: dict[str, Mapping[str, Any]] = {}
    by_symbol: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = _candidate_id(row)
        ccid = _candidate_contract_id(row)
        sym = _symbol(row)
        if cid:
            by_candidate[cid] = row
        if ccid:
            by_contract[ccid] = row
        if sym:
            by_symbol[sym] = row
    return by_candidate, by_contract, by_symbol


def _lookup(row: Mapping[str, Any], indexes: tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]) -> Mapping[str, Any]:
    by_candidate, by_contract, by_symbol = indexes
    return by_candidate.get(_candidate_id(row)) or by_contract.get(_candidate_contract_id(row)) or by_symbol.get(_symbol(row)) or {}


def _market_records(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in _rows(payload, "input_records"):
        sym = _symbol(row)
        if sym:
            out[sym] = row
    return out


def _valid_market(row: Mapping[str, Any], day_utc: str) -> bool:
    if not row:
        return False
    status = str(row.get("validation_status") or row.get("status") or "").upper()
    day = str(row.get("day_utc") or row.get("market_session_date") or row.get("source_timestamp_utc") or row.get("timestamp_utc") or "")[:10]
    return status == "VALID" and (not day or day == day_utc)


def _market_bound(row: Mapping[str, Any]) -> bool:
    return bool(row) and str(row.get("validation_status") or row.get("status") or "").upper() not in {"", "MISSING", "UNAVAILABLE_EXTERNAL_SOURCE"}


def _missing_required_fields(candidate: Mapping[str, Any], contract: Mapping[str, Any]) -> list[str]:
    missing: set[str] = set()
    source = contract or candidate
    if not _text(_candidate_id(source)):
        missing.add("candidate_id")
    if not _text(_candidate_contract_id(source)):
        missing.add("candidate_contract_id")
    if not _symbol(source):
        missing.add("symbol")
    if not _text(source.get("direction"), source.get("proposed_direction")):
        missing.add("direction")
    explicit = candidate.get("missing_required_fields") if isinstance(candidate.get("missing_required_fields"), list) else []
    for item in explicit:
        if str(item or "").strip():
            missing.add(str(item).strip())
    return sorted(missing)


def _construction_result_indexes(construction: Mapping[str, Any]) -> tuple[tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]], tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]], tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]]:
    constructed = _rows(construction, "constructed_paper_trades")
    skipped = _rows(construction, "skipped_candidates")
    diagnostics = _rows(construction, "market_data_diagnostics")
    return _index_rows(constructed), _index_rows(skipped), _index_rows(diagnostics)


def _classify_row(*, candidate: Mapping[str, Any], day_utc: str, contract: Mapping[str, Any], contract_boundary: Mapping[str, Any], market: Mapping[str, Any], constructed: Mapping[str, Any], skipped: Mapping[str, Any], diagnostic: Mapping[str, Any], construction_exists: bool) -> tuple[str, str, str]:
    if constructed:
        missing = constructed.get("missing_fields") if isinstance(constructed.get("missing_fields"), list) else []
        if missing:
            return CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD, "INCOMPLETE_CONSTRUCTION", "Constructed row is incomplete: " + ", ".join(str(item) for item in missing)
        return CONSTRUCTED, "CONSTRUCTED", "Constructed by paper_trade_construction_v1."
    if skipped:
        reason = _text(skipped.get("skip_reason_code"), skipped.get("status"), skipped.get("missing_field"))
        if "MARKET_DATA" in reason or str(skipped.get("missing_field") or "").startswith("market_data"):
            return CONSTRUCTION_BLOCKED_MARKET_DATA, "CONSTRUCTION_BLOCKED", reason or "Construction skipped because market data is not current/valid."
        return REJECTED_BY_CONSTRUCTION, "REJECTED_BY_CONSTRUCTION", reason or "Construction skipped by construction artifact."
    if not _market_bound(market) or not _valid_market(market, day_utc):
        reason = _text(market.get("reason"), market.get("validation_status"), diagnostic.get("status"), "Market data is missing or not valid for the construction day.")
        return CONSTRUCTION_BLOCKED_MARKET_DATA, "CONSTRUCTION_BLOCKED", reason
    if not contract:
        if contract_boundary:
            reason = _text(contract_boundary.get("boundary_reason"), contract_boundary.get("boundary_status"))
            return CONSTRUCTION_BLOCKED_MISSING_CONTRACT, "CONSTRUCTION_BLOCKED", "Candidate has valid market data but no candidate contract in aegis_candidate_contracts_v1. Contract boundary: " + reason
        return CONSTRUCTION_BLOCKED_MISSING_CONTRACT, "CONSTRUCTION_BLOCKED", "Candidate has valid market data but no candidate contract in aegis_candidate_contracts_v1."
    missing_fields = _missing_required_fields(candidate, contract)
    if missing_fields:
        return CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD, "CONSTRUCTION_BLOCKED", "Missing required construction fields: " + ", ".join(missing_fields)
    if construction_exists:
        return BOUNDARY_VIOLATION_SILENT_OMISSION, "OMITTED", "Candidate is ready for construction but has no constructed, rejected, blocked, or not-eligible result."
    return READY_FOR_CONSTRUCTION, "READY_FOR_CONSTRUCTION", "Candidate has contract and market data but construction has not run."


def build_construction_input_boundary_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated = generated_at_utc or _now_iso()
    lifecycle_path = root / "reports" / "aegis_candidate_lifecycle_projection_v1" / day / "candidate_lifecycle_projection.v1.json"
    packet_path = root / "reports" / "aegis_candidate_review_packet_v1" / day / "candidate_review_packet.v1.json"
    contracts_path = root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json"
    market_path = root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    contract_boundary_path = root / "reports" / "aegis_contract_generation_boundary_v1" / day / "contract_generation_boundary.v1.json"
    construction_path = root / "reports" / "paper_trade_construction_v1" / day / "paper_trade_construction.v1.json"

    lifecycle = _read_json(lifecycle_path)
    packet = _read_json(packet_path)
    contracts = _read_json(contracts_path)
    market_inputs = _read_json(market_path)
    contract_boundary_payload = _read_json(contract_boundary_path)
    construction = _read_json(construction_path)
    official_session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)

    current_candidates = _rows(lifecycle, "current_session_candidates")
    packet_rows = _rows(packet, "review_candidates", "paper_candidates", "candidate_trades", "candidates")
    contract_rows = _rows(contracts, "candidate_contracts", "contracts")
    packet_indexes = _index_rows(packet_rows)
    contract_indexes = _index_rows(contract_rows)
    contract_boundary_indexes = _index_rows(_rows(contract_boundary_payload, "boundary_rows"))
    market_by_symbol = _market_records(market_inputs)
    constructed_indexes, skipped_indexes, diagnostic_indexes = _construction_result_indexes(construction)
    construction_exists = bool(construction)

    rows_out: list[dict[str, Any]] = []
    for candidate in current_candidates:
        sym = _symbol(candidate)
        market = market_by_symbol.get(sym, {}) if sym else {}
        packet_row = _lookup(candidate, packet_indexes)
        contract = _lookup(candidate, contract_indexes)
        contract_boundary = _lookup(candidate, contract_boundary_indexes)
        constructed = _lookup(candidate, constructed_indexes)
        skipped = _lookup(candidate, skipped_indexes)
        diagnostic = _lookup(candidate, diagnostic_indexes)
        attempted = bool(constructed or skipped or diagnostic)
        boundary_status, construction_result, boundary_reason = _classify_row(
            candidate=candidate,
            day_utc=day,
            contract=contract,
            contract_boundary=contract_boundary,
            market=market,
            constructed=constructed,
            skipped=skipped,
            diagnostic=diagnostic,
            construction_exists=construction_exists,
        )
        rows_out.append({
            "symbol": sym,
            "candidate_id": _candidate_id(candidate),
            "candidate_contract_id": _candidate_contract_id(candidate),
            "paper_session_id": _text(candidate.get("paper_session_id"), lifecycle.get("paper_session_id"), official_session.get("paper_session_id")),
            "in_current_session_candidates": True,
            "in_candidate_review_packet": bool(packet_row),
            "candidate_contract_present": bool(contract),
            "market_data_bound": _market_bound(market),
            "market_data_valid": _valid_market(market, day),
            "construction_attempted": attempted,
            "construction_result": construction_result,
            "boundary_status": boundary_status,
            "boundary_reason": boundary_reason,
            "market_data_validation_status": str(market.get("validation_status") or ""),
            "market_data_reason": str(market.get("reason") or ""),
            "construction_missing_field": str(diagnostic.get("missing_field") or skipped.get("missing_field") or ""),
        })

    attempted_count = sum(1 for row in rows_out if row["construction_attempted"])
    constructed_count = sum(1 for row in rows_out if row["boundary_status"] == CONSTRUCTED)
    rejected_count = sum(1 for row in rows_out if row["boundary_status"] == REJECTED_BY_CONSTRUCTION)
    blocked_count = sum(1 for row in rows_out if row["boundary_status"] in BLOCKED_STATUSES)
    not_eligible_count = sum(1 for row in rows_out if row["boundary_status"] == NOT_ELIGIBLE_FOR_CONSTRUCTION)
    violation_rows = [row for row in rows_out if row["boundary_status"] == BOUNDARY_VIOLATION_SILENT_OMISSION]
    silent_omission_count = len(violation_rows)
    current_count = len(rows_out)
    formula_violation = max(0, current_count - (attempted_count + rejected_count + blocked_count + not_eligible_count))
    if formula_violation and not silent_omission_count:
        silent_omission_count = formula_violation

    status = "BOUNDARY_VIOLATION_SILENT_OMISSION" if silent_omission_count else "PASS"
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": generated,
        "status": status,
        "paper_session_id": _text(lifecycle.get("paper_session_id"), construction.get("paper_session_id"), packet.get("paper_session_id"), official_session.get("paper_session_id")),
        "current_session_candidate_count": current_count,
        "candidate_review_packet_count": len(packet_rows),
        "candidate_contract_count": len(contract_rows),
        "market_data_bound_count": sum(1 for row in rows_out if row["market_data_bound"]),
        "market_data_valid_count": sum(1 for row in rows_out if row["market_data_valid"]),
        "construction_attempted_count": attempted_count,
        "constructed_count": constructed_count,
        "rejected_count": rejected_count,
        "blocked_count": blocked_count,
        "not_eligible_count": not_eligible_count,
        "silent_omission_count": silent_omission_count,
        "blocked_missing_contract_count": sum(1 for row in rows_out if row["boundary_status"] == CONSTRUCTION_BLOCKED_MISSING_CONTRACT),
        "blocked_market_data_count": sum(1 for row in rows_out if row["boundary_status"] == CONSTRUCTION_BLOCKED_MARKET_DATA),
        "blocked_missing_required_field_count": sum(1 for row in rows_out if row["boundary_status"] == CONSTRUCTION_BLOCKED_MISSING_REQUIRED_FIELD),
        "source_artifacts": {
            "candidate_lifecycle_projection_v1": str(lifecycle_path),
            "candidate_review_packet_v1": str(packet_path),
            "aegis_candidate_contracts_v1": str(contracts_path),
            "aegis_contract_generation_boundary_v1": str(contract_boundary_path),
            "market_data_inputs_v1": str(market_path),
            "paper_trade_construction_v1": str(construction_path),
        },
        "boundary_rows": rows_out,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_construction_input_boundary_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    return write_json_v1(construction_input_boundary_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payload))


def build_and_write_construction_input_boundary_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_construction_input_boundary_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_construction_input_boundary_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path


def render_construction_input_boundary_v1(payload: Mapping[str, Any]) -> str:
    session = str(payload.get("paper_session_id") or "")
    lines = [
        f"Construction input boundary for {session}:",
        f"- current-session candidates: {payload.get('current_session_candidate_count', 0)}",
        f"- candidate review packet: {payload.get('candidate_review_packet_count', 0)}",
        f"- candidate contracts: {payload.get('candidate_contract_count', 0)}",
        f"- market-data valid: {payload.get('market_data_valid_count', 0)}",
        f"- construction attempted: {payload.get('construction_attempted_count', 0)}",
        f"- constructed: {payload.get('constructed_count', 0)}",
        f"- blocked missing contract: {payload.get('blocked_missing_contract_count', 0)}",
        f"- blocked market data: {payload.get('blocked_market_data_count', 0)}",
        f"- silent omissions: {payload.get('silent_omission_count', 0)}",
    ]
    if int(payload.get("silent_omission_count") or 0) > 0:
        lines.append(f"BOUNDARY VIOLATION: {payload.get('silent_omission_count')} current-session candidates were omitted without explicit construction result.")
    return "\n".join(lines) + "\n"
