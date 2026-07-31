from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_candidate_lineage_forensics_v1"
CURRENT_GOVERNED_DAY = "2026-05-26"

SEARCH_FILENAMES = {
    "candidate_lifecycle.v1.json",
    "candidate_outcomes.v1.json",
    "candidate_contracts.v1.json",
    "signal_evidence_graph.v1.json",
    "candidate_lineage.v1.json",
    "candidate_consumption_audit.v1.json",
    "manual_trade_packet.v1.json",
    "manual_execution_receipt.v1.json",
    "operator_execution_queue.v1.json",
    "manual_trade_receipt.v1.json",
    "shadow_candidate_arbitration.v1.json",
    "paper_trade_golden_path.v1.json",
    "candidate_generation_manifest.v1.json",
    "sleeve_evaluation.v1.json",
    "sleeve_evaluation_rollup.v1.json",
    "captured_ticket_history.v1.json",
    "manual_capture_record.v1.json",
}

MATCH_KEYS = {
    "symbol",
    "underlying_symbol",
    "intent_symbol",
    "artifact_symbol",
    "requested_symbol",
    "producer_requested_symbol",
}

ID_KEYS = (
    "candidate_id",
    "raw_signal_id",
    "intent_id",
    "raw_intent_id",
    "recommended_trade_id",
    "selected_exposure_intent_id",
    "receipt_id",
)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _day_from_path(path: Path) -> str:
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _family_from_path(path: Path) -> str:
    parts = list(path.parts)
    if "reports" in parts:
        idx = parts.index("reports")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    if "manual_trade_receipts" in parts:
        return "manual_trade_receipts"
    return path.parent.name


def _symbol_value_matches(value: Any, symbol: str) -> bool:
    target = symbol.upper()
    if isinstance(value, str):
        return value.upper() == target
    if isinstance(value, list):
        return any(isinstance(item, str) and item.upper() == target for item in value)
    return False


def _contains_symbol(value: Any, symbol: str) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in MATCH_KEYS and _symbol_value_matches(item, symbol):
                return True
            if isinstance(item, (dict, list)) and _contains_symbol(item, symbol):
                return True
    elif isinstance(value, list):
        return any(_contains_symbol(item, symbol) for item in value if isinstance(item, (dict, list)))
    return False


def _find_symbol_rows(value: Any, symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        direct = any(str(key) in MATCH_KEYS and _symbol_value_matches(item, symbol) for key, item in value.items())
        if direct:
            rows.append(value)
        for item in value.values():
            rows.extend(_find_symbol_rows(item, symbol))
    elif isinstance(value, list):
        for item in value:
            rows.extend(_find_symbol_rows(item, symbol))
    return rows


def _first_from_rows(rows: list[dict[str, Any]], *keys: str) -> str:
    for row in rows:
        for key in keys:
            value = _text(row.get(key))
            if value:
                return value
    return ""


def _paths_by_family(matches: list[dict[str, Any]], family: str) -> list[str]:
    return sorted({str(row["path"]) for row in matches if row.get("family") == family})


def _evidence_matches_for_symbol(*, truth_root: Path, symbol: str, day_utc: str | None = None) -> list[dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    paths = [
        path
        for path in root.rglob("*.json")
        if path.name in SEARCH_FILENAMES or path.name.endswith(".manual_trade_receipt.v1.json")
    ]
    paths.extend(root.rglob("*.jsonl"))
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(paths):
        if str(path) in seen:
            continue
        seen.add(str(path))
        if "__pycache__" in path.parts:
            continue
        found_day = _day_from_path(path)
        if day_utc and found_day != day_utc:
            continue
        payload: Any
        if path.suffix == ".jsonl":
            payload = _read_jsonl(path)
        else:
            payload = _read_json(path)
        if payload is None or not _contains_symbol(payload, symbol):
            continue
        rows = _find_symbol_rows(payload, symbol)
        family = _family_from_path(path)
        matches.append(
            {
                "path": str(path),
                "family": family,
                "day_utc": found_day,
                "schema_id": _text(payload.get("schema_id")) if isinstance(payload, dict) else "",
                "artifact_id": _text(payload.get("artifact_id")) if isinstance(payload, dict) else "",
                "match_count": len(rows),
                "candidate_ids": sorted({_text(row.get("candidate_id")) for row in rows if _text(row.get("candidate_id"))}),
                "raw_signal_ids": sorted({_text(row.get("raw_signal_id")) for row in rows if _text(row.get("raw_signal_id"))}),
                "intent_ids": sorted({_text(row.get("intent_id") or row.get("raw_intent_id") or row.get("selected_exposure_intent_id")) for row in rows if _text(row.get("intent_id") or row.get("raw_intent_id") or row.get("selected_exposure_intent_id"))}),
                "sleeve_ids": sorted({_text(row.get("sleeve_id") or row.get("engine_id")) for row in rows if _text(row.get("sleeve_id") or row.get("engine_id"))}),
                "statuses": sorted({_text(row.get("status") or row.get("current_status") or row.get("contract_validation_status") or row.get("review_status") or row.get("verification_status") or row.get("result")) for row in rows if _text(row.get("status") or row.get("current_status") or row.get("contract_validation_status") or row.get("review_status") or row.get("verification_status") or row.get("result"))}),
                "rows": [_summarize_row(row) for row in rows[:5]],
            }
        )
    return sorted(matches, key=lambda row: (row.get("day_utc") or "", row.get("family") or "", row.get("path") or ""))


def _summarize_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "candidate_id",
        "raw_signal_id",
        "intent_id",
        "raw_intent_id",
        "selected_exposure_intent_id",
        "symbol",
        "sleeve_id",
        "engine_id",
        "entry_reference_price",
        "entry_reference_price_source_path",
        "contract_validation_status",
        "promotion_status",
        "review_status",
        "receipt_type",
        "status",
        "current_status",
        "result",
    ]
    return {key: row.get(key) for key in keys if key in row and row.get(key) not in (None, "", [])}


def _select_day(matches: list[dict[str, Any]], requested_day: str | None) -> str:
    if requested_day:
        return requested_day
    priority = {
        "aegis_candidate_contracts_v1": 5,
        "candidate_lineage_v1": 4,
        "manual_trade_packet_v1": 4,
        "captured_ticket_history_v1": 8,
        "manual_trade_receipts": 5,
        "manual_execution_receipt_v1": 5,
        "sleeve_evaluation_kernel_v1": 1,
    }
    families_by_day: dict[str, set[str]] = {}
    for row in matches:
        day = _text(row.get("day_utc"))
        family = _text(row.get("family"))
        if day and family:
            families_by_day.setdefault(day, set()).add(family)
    scores = {day: sum(priority.get(family, 1) for family in families) for day, families in families_by_day.items()}
    if not scores:
        return CURRENT_GOVERNED_DAY
    return sorted(scores.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]


def _status_for_family(matches: list[dict[str, Any]], family: str) -> str:
    rows = [row for row in matches if row.get("family") == family]
    if not rows:
        return "MISSING"
    statuses = {status.upper() for row in rows for status in _safe_list(row.get("statuses")) if _text(status)}
    if any(status in {"VALID", "PASS", "PROMOTED", "READY", "RECORDED", "VERIFIED"} for status in statuses):
        return "PRESENT"
    return "PRESENT_UNKNOWN_STATUS"


def _derive_lineage(*, symbol: str, day_matches: list[dict[str, Any]], current_matches: list[dict[str, Any]], all_matches: list[dict[str, Any]], selected_day: str) -> dict[str, Any]:
    rows = [summary for match in day_matches for summary in _safe_list(match.get("rows")) if isinstance(summary, dict)]
    candidate_id = _first_from_rows(rows, "candidate_id", "selected_exposure_intent_id", "recommended_trade_id", "intent_id")
    raw_signal_id = _first_from_rows(rows, "raw_signal_id", "intent_id", "raw_intent_id", "selected_exposure_intent_id")
    sleeve_id = _first_from_rows(rows, "sleeve_id", "engine_id")
    contract_paths = _paths_by_family(day_matches, "aegis_candidate_contracts_v1")
    graph_paths = _paths_by_family(day_matches, "aegis_signal_evidence_graph_v1")
    packet_paths = _paths_by_family(day_matches, "manual_trade_packet_v1")
    receipt_paths = _paths_by_family(day_matches, "manual_execution_receipt_v1") + _paths_by_family(day_matches, "manual_trade_receipts")
    if not receipt_paths:
        linked_receipts = [
            row for row in all_matches
            if row.get("family") in {"manual_execution_receipt_v1", "manual_trade_receipts"}
            and _text(row.get("day_utc")) >= selected_day
        ]
        receipt_paths = [str(row.get("path")) for row in sorted(linked_receipts, key=lambda item: (_text(item.get("day_utc")), _text(item.get("path"))))[:1] if row.get("path")]
    simulated_paths = [row["path"] for row in day_matches if "paper_rehearsal" in row["path"] or any("SIMULATED_PAPER" == status for status in _safe_list(row.get("statuses")))]
    entry_row = next((row for row in rows if _text(row.get("entry_reference_price")) or _text(row.get("entry_reference_price_source_path"))), {})
    has_contract = bool(contract_paths)
    has_graph = bool(graph_paths)
    has_rehearsal = bool(simulated_paths) or "SIMULATOR" in sleeve_id.upper()
    has_legacy_capture = bool(_paths_by_family(day_matches, "captured_ticket_history_v1") or _paths_by_family(day_matches, "manual_trade_receipts"))
    has_current_governed = bool(_paths_by_family(current_matches, "aegis_candidate_contracts_v1") and _paths_by_family(current_matches, "aegis_signal_evidence_graph_v1"))
    if has_rehearsal:
        classification = "SIMULATED_OR_REHEARSAL"
    elif has_contract and has_graph:
        classification = "GOVERNED"
    elif has_legacy_capture or day_matches:
        classification = "LEGACY_OR_PARTIAL"
    else:
        classification = "MISSING"
    if selected_day != CURRENT_GOVERNED_DAY and classification != "MISSING":
        staleness = "STALE_RELATIVE_TO_CURRENT_GOVERNED_DAY"
    else:
        staleness = "CURRENT_DAY"
    missing = []
    required = {
        "raw_signal_id": raw_signal_id,
        "signal_evidence_graph_path": graph_paths[0] if graph_paths else "",
        "candidate_contract_path": contract_paths[0] if contract_paths else "",
        "review_queue_path": _paths_by_family(day_matches, "operator_execution_queue_v1"),
        "paper_packet_path": packet_paths,
        "receipt_or_outcome_path": receipt_paths or _paths_by_family(day_matches, "aegis_candidate_lifecycle_v1"),
    }
    for key, value in required.items():
        if not value:
            missing.append(key)
    return {
        "candidate_id": candidate_id,
        "day_utc": selected_day,
        "source_sleeve": sleeve_id,
        "raw_signal_id": raw_signal_id,
        "candidate_contract_path": contract_paths[0] if contract_paths else "",
        "signal_evidence_graph_path": graph_paths[0] if graph_paths else "",
        "entry_reference_price": _text(entry_row.get("entry_reference_price")),
        "entry_reference_price_source": _text(entry_row.get("entry_reference_price_source_path")),
        "arbitration_status": _status_for_family(day_matches, "shadow_candidate_arbitration_v1"),
        "promotion_status": _first_from_rows(rows, "promotion_status") or _status_for_family(day_matches, "candidate_lineage_v1"),
        "review_packet_path": packet_paths[0] if packet_paths else "",
        "receipt_outcome_path": receipt_paths[0] if receipt_paths else "",
        "classification": classification,
        "staleness_status": staleness,
        "current_2026_05_26_governed_candidate_present_for_symbol": has_current_governed,
        "missing_lineage_fields": missing,
        "does_not_treat_rehearsal_as_real": not has_rehearsal or classification == "SIMULATED_OR_REHEARSAL",
    }


def build_candidate_lineage_forensics_v1(*, truth_root: Path, symbol: str, day_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    normalized_symbol = symbol.strip().upper()
    all_matches = _evidence_matches_for_symbol(truth_root=root, symbol=normalized_symbol)
    selected_day = _select_day(all_matches, day_utc)
    day_matches = [row for row in all_matches if row.get("day_utc") == selected_day]
    current_matches = [row for row in all_matches if row.get("day_utc") == CURRENT_GOVERNED_DAY]
    lineage = _derive_lineage(symbol=normalized_symbol, day_matches=day_matches, current_matches=current_matches, all_matches=all_matches, selected_day=selected_day)
    payload = {
        "schema_id": "aegis_candidate_lineage_forensics",
        "schema_version": "v1",
        "artifact_id": f"{REPORT_FAMILY}:{normalized_symbol}:{selected_day}",
        "generated_at_utc": now_utc_v1(),
        "symbol": normalized_symbol,
        "day_utc": selected_day,
        "truth_root": str(root),
        "forensic_summary": lineage,
        "current_governed_path_comparison": {
            "day_utc": CURRENT_GOVERNED_DAY,
            "expected_path": ["raw_signal", "signal_evidence_graph", "candidate_contract", "review_queue", "paper_packet", "receipt"],
            "symbol_has_current_governed_contract": lineage["current_2026_05_26_governed_candidate_present_for_symbol"],
            "current_day_matching_artifact_count": len(current_matches),
            "current_day_matching_artifacts": [{k: row[k] for k in ("family", "path", "statuses") if k in row} for row in current_matches[:50]],
        },
        "matching_artifact_count": len(day_matches),
        "matching_artifacts": day_matches,
        "all_symbol_match_days": sorted({row.get("day_utc") for row in all_matches if row.get("day_utc")}),
        "safety": {
            "read_only_forensics": True,
            "trade_advice_allowed": False,
            "manual_trade_capture_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "policy_gates_modified": False,
        },
    }
    return payload


def candidate_lineage_forensics_path_v1(*, truth_root: Path, day_utc: str, symbol: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / f"{symbol.strip().lower()}_candidate_forensics.v1.json"


def write_candidate_lineage_forensics_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    path = candidate_lineage_forensics_path_v1(truth_root=truth_root, day_utc=str(payload["day_utc"]), symbol=str(payload["symbol"]))
    return write_json_v1(path, payload)
