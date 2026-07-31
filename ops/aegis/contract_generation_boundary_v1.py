from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.paper_session_ledger_v1 import resolve_scheduled_paper_session_v1

REPORT_FAMILY = "aegis_contract_generation_boundary_v1"
REPORT_FILENAME = "contract_generation_boundary.v1.json"
SCHEMA_ID = "aegis_contract_generation_boundary"
SCHEMA_VERSION = "v1"

READY_FOR_CONTRACT_GENERATION = "READY_FOR_CONTRACT_GENERATION"
VALID_CONTRACT = "VALID_CONTRACT"
CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE = "CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE"
CONTRACT_BLOCKED_MISSING_MARKET_DATA = "CONTRACT_BLOCKED_MISSING_MARKET_DATA"
CONTRACT_REJECTED_BY_POLICY = "CONTRACT_REJECTED_BY_POLICY"
NOT_ELIGIBLE_FOR_CONTRACT = "NOT_ELIGIBLE_FOR_CONTRACT"
CONTRACT_INPUT_BOUNDARY_VIOLATION = "CONTRACT_INPUT_BOUNDARY_VIOLATION"

BLOCKED_STATUSES = {
    CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE,
    CONTRACT_BLOCKED_MISSING_MARKET_DATA,
}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def contract_generation_boundary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
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


def _raw_signal_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("raw_signal_id"), row.get("intent_id"), row.get("source_signal_id"))


def _index_rows(
    rows: list[Mapping[str, Any]],
) -> tuple[
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
]:
    by_candidate: dict[str, Mapping[str, Any]] = {}
    by_contract: dict[str, Mapping[str, Any]] = {}
    by_symbol: dict[str, Mapping[str, Any]] = {}
    by_raw_signal: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        cid = _candidate_id(row)
        ccid = _candidate_contract_id(row)
        sym = _symbol(row)
        raw = _raw_signal_id(row)
        if cid:
            by_candidate[cid] = row
        if ccid:
            by_contract[ccid] = row
        if sym:
            by_symbol[sym] = row
        if raw:
            by_raw_signal[raw] = row
    return by_candidate, by_contract, by_symbol, by_raw_signal


def _lookup(
    row: Mapping[str, Any],
    indexes: tuple[
        dict[str, Mapping[str, Any]],
        dict[str, Mapping[str, Any]],
        dict[str, Mapping[str, Any]],
        dict[str, Mapping[str, Any]],
    ],
) -> Mapping[str, Any]:
    by_candidate, by_contract, by_symbol, by_raw_signal = indexes
    return (
        by_candidate.get(_candidate_id(row))
        or by_contract.get(_candidate_contract_id(row))
        or by_raw_signal.get(_raw_signal_id(row))
        or by_symbol.get(_symbol(row))
        or {}
    )


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


def _contract_present(row: Mapping[str, Any]) -> bool:
    return bool(row) and str(row.get("contract_validation_status") or row.get("contract_status") or "VALID").upper() == "VALID"


def _classify_row(
    *,
    signal: Mapping[str, Any],
    signal_boundary: Mapping[str, Any],
    contract: Mapping[str, Any],
    rejected: Mapping[str, Any],
    market: Mapping[str, Any],
    day_utc: str,
    contracts_exists: bool,
) -> tuple[str, str, str]:
    if _contract_present(contract):
        return VALID_CONTRACT, VALID_CONTRACT, "Valid candidate contract exists in aegis_candidate_contracts_v1."
    if rejected:
        reason = _text(rejected.get("rejection_reason"), rejected.get("contract_validation_status"), "Candidate contract was rejected by contract generation.")
        return CONTRACT_REJECTED_BY_POLICY, "CONTRACT_REJECTED", reason
    if not _market_bound(market) or not _valid_market(market, day_utc):
        reason = _text(market.get("reason"), market.get("validation_status"), "Current-session candidate does not have valid market data for contract generation.")
        return CONTRACT_BLOCKED_MISSING_MARKET_DATA, "CONTRACT_BLOCKED", reason
    if not signal:
        if signal_boundary:
            reason = _text(signal_boundary.get("boundary_reason"), signal_boundary.get("boundary_status"))
            return (
                CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE,
                "CONTRACT_INPUT_BOUNDARY_BLOCKED",
                "Candidate has valid market data but is absent from signal_evidence_graph_v1, so candidate contract generation did not attempt it. Signal boundary: " + reason,
            )
        return (
            CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE,
            "CONTRACT_INPUT_BOUNDARY_BLOCKED",
            "Candidate has valid market data but is absent from signal_evidence_graph_v1, so candidate contract generation did not attempt it.",
        )
    if contracts_exists:
        return (
            CONTRACT_INPUT_BOUNDARY_VIOLATION,
            "CONTRACT_INPUT_BOUNDARY_BLOCKED",
            "Candidate is present in signal evidence with valid market data but has no valid or rejected contract result.",
        )
    return READY_FOR_CONTRACT_GENERATION, "CONTRACT_INPUT_BOUNDARY_BLOCKED", "Candidate has signal evidence and valid market data, but candidate contracts have not run."


def build_contract_generation_boundary_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated = generated_at_utc or _now_iso()

    lifecycle_path = root / "reports" / "aegis_candidate_lifecycle_projection_v1" / day / "candidate_lifecycle_projection.v1.json"
    market_path = root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    signal_path = root / "reports" / "aegis_signal_evidence_graph_v1" / day / "signal_evidence_graph.v1.json"
    signal_boundary_path = root / "reports" / "aegis_signal_evidence_boundary_v1" / day / "signal_evidence_boundary.v1.json"
    contracts_path = root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json"

    lifecycle = _read_json(lifecycle_path)
    market_inputs = _read_json(market_path)
    signal_graph = _read_json(signal_path)
    signal_boundary_payload = _read_json(signal_boundary_path)
    contracts = _read_json(contracts_path)
    official_session = resolve_scheduled_paper_session_v1(truth_root=root, day_utc=day)

    current_candidates = _rows(lifecycle, "current_session_candidates")
    market_by_symbol = _market_records(market_inputs)
    signal_rows = _rows(signal_graph, "signals")
    contract_rows = _rows(contracts, "candidate_contracts", "contracts")
    rejected_rows = _rows(contracts, "rejected_raw_signals", "rejected_contracts")

    signal_indexes = _index_rows(signal_rows)
    signal_boundary_indexes = _index_rows(_rows(signal_boundary_payload, "boundary_rows"))
    contract_indexes = _index_rows(contract_rows)
    rejected_indexes = _index_rows(rejected_rows)
    contracts_exists = bool(contracts)

    rows_out: list[dict[str, Any]] = []
    for candidate in current_candidates:
        sym = _symbol(candidate)
        market = market_by_symbol.get(sym, {}) if sym else {}
        signal = _lookup(candidate, signal_indexes)
        signal_boundary = _lookup(candidate, signal_boundary_indexes)
        contract = _lookup(candidate, contract_indexes)
        rejected = _lookup(candidate, rejected_indexes)
        attempted = bool(signal or contract or rejected)
        boundary_status, contract_outcome, boundary_reason = _classify_row(
            signal=signal,
            signal_boundary=signal_boundary,
            contract=contract,
            rejected=rejected,
            market=market,
            day_utc=day,
            contracts_exists=contracts_exists,
        )
        rows_out.append(
            {
                "symbol": sym,
                "candidate_id": _candidate_id(candidate),
                "candidate_contract_id": _candidate_contract_id(contract) or _candidate_contract_id(candidate),
                "paper_session_id": _text(candidate.get("paper_session_id"), lifecycle.get("paper_session_id"), official_session.get("paper_session_id")),
                "in_current_session_candidates": True,
                "market_data_bound": _market_bound(market),
                "market_data_valid": _valid_market(market, day),
                "in_signal_evidence_graph": bool(signal),
                "raw_signal_id": _text(_raw_signal_id(signal), _raw_signal_id(candidate)),
                "contract_generation_attempted": attempted,
                "contract_present": _contract_present(contract),
                "contract_outcome": contract_outcome,
                "boundary_status": boundary_status,
                "boundary_reason": boundary_reason,
                "market_data_validation_status": str(market.get("validation_status") or ""),
                "market_data_reason": str(market.get("reason") or ""),
                "contract_rejection_reason": str(rejected.get("rejection_reason") or ""),
            }
        )

    valid_contract_count = sum(1 for row in rows_out if row["boundary_status"] == VALID_CONTRACT)
    blocked_count = sum(1 for row in rows_out if row["boundary_status"] in BLOCKED_STATUSES)
    rejected_count = sum(1 for row in rows_out if row["boundary_status"] == CONTRACT_REJECTED_BY_POLICY)
    not_eligible_count = sum(1 for row in rows_out if row["boundary_status"] == NOT_ELIGIBLE_FOR_CONTRACT)
    violation_count = sum(1 for row in rows_out if row["boundary_status"] == CONTRACT_INPUT_BOUNDARY_VIOLATION)
    silent_omission_count = 0
    status = CONTRACT_INPUT_BOUNDARY_VIOLATION if violation_count else "PASS"

    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": generated,
        "status": status,
        "paper_session_id": _text(lifecycle.get("paper_session_id"), contracts.get("paper_session_id"), official_session.get("paper_session_id")),
        "current_session_candidate_count": len(rows_out),
        "market_data_bound_count": sum(1 for row in rows_out if row["market_data_bound"]),
        "market_data_valid_count": sum(1 for row in rows_out if row["market_data_valid"]),
        "signal_evidence_graph_signal_count": len(signal_rows),
        "contract_generation_attempted_count": sum(1 for row in rows_out if row["contract_generation_attempted"]),
        "valid_contract_count": valid_contract_count,
        "blocked_count": blocked_count,
        "rejected_count": rejected_count,
        "not_eligible_count": not_eligible_count,
        "silent_omission_count": silent_omission_count,
        "blocked_missing_signal_evidence_count": sum(1 for row in rows_out if row["boundary_status"] == CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE),
        "blocked_missing_market_data_count": sum(1 for row in rows_out if row["boundary_status"] == CONTRACT_BLOCKED_MISSING_MARKET_DATA),
        "contract_input_boundary_violation_count": violation_count,
        "valid_market_uncontracted_symbols": [
            row["symbol"]
            for row in rows_out
            if row["market_data_valid"] and not row["contract_present"]
        ],
        "source_artifacts": {
            "candidate_lifecycle_projection_v1": str(lifecycle_path),
            "market_data_inputs_v1": str(market_path),
            "signal_evidence_graph_v1": str(signal_path),
            "aegis_signal_evidence_boundary_v1": str(signal_boundary_path),
            "aegis_candidate_contracts_v1": str(contracts_path),
        },
        "boundary_rows": rows_out,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_contract_generation_boundary_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    return write_json_v1(contract_generation_boundary_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payload))


def build_and_write_contract_generation_boundary_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_contract_generation_boundary_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_contract_generation_boundary_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "artifact_path": str(path)}, path


def render_contract_generation_boundary_v1(payload: Mapping[str, Any]) -> str:
    session = str(payload.get("paper_session_id") or "")
    lines = [
        f"Contract generation for {session}:",
        f"- current-session candidates: {payload.get('current_session_candidate_count', 0)}",
        f"- market-data valid: {payload.get('market_data_valid_count', 0)}",
        f"- signal evidence graph signals: {payload.get('signal_evidence_graph_signal_count', 0)}",
        f"- contract generation attempted: {payload.get('contract_generation_attempted_count', 0)}",
        f"- valid contracts: {payload.get('valid_contract_count', 0)}",
        f"- blocked missing signal evidence: {payload.get('blocked_missing_signal_evidence_count', 0)}",
        f"- blocked missing market data: {payload.get('blocked_missing_market_data_count', 0)}",
        f"- silent omissions: {payload.get('silent_omission_count', 0)}",
    ]
    missing_signal_symbols = [
        str(row.get("symbol") or "")
        for row in payload.get("boundary_rows", [])
        if isinstance(row, Mapping) and row.get("boundary_status") == CONTRACT_BLOCKED_MISSING_SIGNAL_EVIDENCE
    ]
    if missing_signal_symbols:
        lines.append("- market-data-valid but uncontracted symbols: " + ", ".join(missing_signal_symbols))
    if int(payload.get("contract_input_boundary_violation_count") or 0) > 0:
        lines.append(f"CONTRACT INPUT BOUNDARY VIOLATION: {payload.get('contract_input_boundary_violation_count')} current-session candidates had signal evidence but no explicit contract result.")
    return "\n".join(lines) + "\n"
