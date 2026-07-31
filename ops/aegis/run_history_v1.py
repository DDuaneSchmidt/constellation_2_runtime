from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import write_json_v1

REPORT_FAMILY = "aegis_run_history_v1"
FILENAME = "run_history.v1.json"


def run_history_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / FILENAME


def read_run_history_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = run_history_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_run_history_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(run_history_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def append_candidate_diagnostics_run_history_v1(*, truth_root: Path, day_utc: str, command: str, diagnostics: dict[str, Any], diagnostics_path: str = "", candidate_contracts_path: str = "", paper_review_queue_path: str = "") -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    existing = read_run_history_v1(truth_root=root, day_utc=day_utc)
    rows = existing.get("runs") if isinstance(existing.get("runs"), list) else []
    diagnostics_completed_at = _completed_at(diagnostics, diagnostics_path, candidate_contracts_path, paper_review_queue_path)
    diagnostics_started_at = _diagnostics_started_at(diagnostics)
    started_at = _actual_run_started_at(diagnostics)
    market_snapshot_time = _market_snapshot_time(root=root, day_utc=day_utc)
    candidate_snapshot_time = _candidate_snapshot_time(root=root, day_utc=day_utc)
    contracts_payload = _read_json(Path(candidate_contracts_path)) if candidate_contracts_path else {}
    queue_payload = _read_json(Path(paper_review_queue_path)) if paper_review_queue_path else {}
    contract_rows = contracts_payload.get("candidate_contracts") if isinstance(contracts_payload.get("candidate_contracts"), list) else []
    rejected_contract_rows = contracts_payload.get("rejected_raw_signals") if isinstance(contracts_payload.get("rejected_raw_signals"), list) else []
    queue_rows = queue_payload.get("rows") if isinstance(queue_payload.get("rows"), list) else []
    candidate_contracts_created = _int(contracts_payload.get("candidates_created"), len(contract_rows))
    diagnostic_candidate_outputs = _int(diagnostics.get("diagnostic_candidate_outputs"), _int(diagnostics.get("total_candidates_generated"), candidate_contracts_created))
    diagnostics_candidates_generated = _int(diagnostics.get("total_candidates_generated"), candidate_contracts_created)
    valid_candidate_contracts = _int(diagnostics.get("valid_candidate_contracts"), candidate_contracts_created)
    rejected_candidate_contracts = _int(diagnostics.get("rejected_candidate_contracts"), _int(contracts_payload.get("candidates_rejected"), len(rejected_contract_rows)))
    reviewable_current_day_candidates = valid_candidate_contracts
    carried_forward_candidates = sum(1 for row in queue_rows if isinstance(row, dict) and str(row.get("originating_day") or row.get("day_utc") or day_utc) != day_utc)
    mismatch_status, mismatch_reason = _artifact_mismatch(diagnostic_candidate_outputs=diagnostic_candidate_outputs, valid_candidate_contracts=valid_candidate_contracts, rejected_candidate_contracts=rejected_candidate_contracts, contracts_payload=contracts_payload)
    run_id = _run_id(day_utc=day_utc, command=command, completed_at=diagnostics_completed_at, diagnostics_path=diagnostics_path)
    row = {
        "run_id": run_id,
        "started_at": started_at,
        "run_start": started_at or "NOT_RECORDED",
        "completed_at": diagnostics_completed_at,
        "market_snapshot_time": market_snapshot_time,
        "candidate_snapshot_time": candidate_snapshot_time,
        "diagnostics_started_at": diagnostics_started_at or "NOT_RECORDED",
        "diagnostics_completed_at": diagnostics_completed_at,
        "command": command,
        "target_day": day_utc,
        "status": _status(diagnostics),
        "classification": str(diagnostics.get("operator_interpretation") or "UNKNOWN"),
        "sleeves_expected": _int(diagnostics.get("total_sleeves_expected"), 0),
        "sleeves_run": _int(diagnostics.get("total_sleeves_run"), 0),
        "raw_signals": _int(diagnostics.get("total_raw_signals"), 0),
        "diagnostic_candidate_outputs": diagnostic_candidate_outputs,
        "candidate_contracts_created": candidate_contracts_created,
        "diagnostics_candidates_generated": diagnostics_candidates_generated,
        "valid_candidate_contracts": valid_candidate_contracts,
        "rejected_candidate_contracts": rejected_candidate_contracts,
        "reviewable_current_day_candidates": reviewable_current_day_candidates,
        "carried_forward_candidates": carried_forward_candidates,
        "rejected_count": _int(diagnostics.get("total_candidates_rejected"), 0),
        "diagnostics_path": diagnostics_path,
        "candidate_contracts_path": candidate_contracts_path,
        "paper_review_queue_path": paper_review_queue_path,
        "source_hashes": _source_hashes([diagnostics_path, candidate_contracts_path, paper_review_queue_path]),
        "artifact_mismatch": bool(mismatch_status),
        "artifact_mismatch_status": mismatch_status,
        "artifact_mismatch_reason": mismatch_reason,
        "mismatch_reason": mismatch_reason,
        "mismatch_explanation": _mismatch_explanation(status=mismatch_status, reason=mismatch_reason, contracts_payload=contracts_payload, candidate_contracts_path=candidate_contracts_path, diagnostics_path=diagnostics_path),
        "paper_only": True,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    deduped = [item for item in rows if isinstance(item, dict) and item.get("run_id") != run_id]
    deduped.append(row)
    deduped.sort(key=lambda item: str(item.get("completed_at") or item.get("started_at") or ""))
    payload = {
        "schema_id": "aegis_run_history",
        "schema_version": "v1",
        "artifact_id": "aegis_run_history_v1",
        "day_utc": day_utc,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "run_count": len(deduped),
        "latest_run_id": str(deduped[-1].get("run_id") or "") if deduped else "",
        "runs": deduped,
        "source_artifacts": [path for path in [diagnostics_path, candidate_contracts_path, paper_review_queue_path] if path],
        "safety": {
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        },
    }
    write_run_history_v1(truth_root=root, day_utc=day_utc, payload=payload)
    return payload


def latest_run_row_v1(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("runs") if isinstance(payload.get("runs"), list) else []
    valid = [row for row in rows if isinstance(row, dict)]
    if not valid:
        return {}
    latest_run_id = str(payload.get("latest_run_id") or "")
    if latest_run_id:
        for row in valid:
            if str(row.get("run_id") or "") == latest_run_id:
                return row
    return sorted(valid, key=lambda row: str(row.get("completed_at") or row.get("candidate_snapshot_time") or row.get("started_at") or ""))[-1]


def _actual_run_started_at(diagnostics: dict[str, Any]) -> str:
    for key in ("actual_run_started_at", "operational_run_started_at", "run_started_at_utc", "started_at_utc"):
        if diagnostics.get(key):
            return str(diagnostics[key])
    return ""


def _diagnostics_started_at(diagnostics: dict[str, Any]) -> str:
    for key in ("diagnostics_started_at", "diagnostics_started_at_utc"):
        if diagnostics.get(key):
            return str(diagnostics[key])
    return ""


def _market_snapshot_time(*, root: Path, day_utc: str) -> str:
    for path in (
        root / "reports" / "event_market_snapshot_v1" / day_utc / "event_market_snapshot.v1.json",
        root / "reports" / "aegis_market_data_v1" / day_utc / "market_data.v1.json",
    ):
        payload = _read_json(path)
        for key in ("market_snapshot_time", "snapshot_time", "data_timestamp_utc", "generated_at_utc", "generated_at"):
            if payload.get(key):
                return str(payload[key])
    return ""


def _candidate_snapshot_time(*, root: Path, day_utc: str) -> str:
    payload = _read_json(root / "reports" / "operator_state_snapshot_v1" / day_utc / "operator_state_snapshot.v1.json")
    candidates = []
    for key in ("operator_today_projection", "current_day_status", "latest_run_summary"):
        value = payload.get(key)
        if isinstance(value, dict):
            candidates.append(value)
    current_truth = payload.get("current_operator_truth") if isinstance(payload.get("current_operator_truth"), dict) else {}
    current_truth_day = current_truth.get("current_day_status") if isinstance(current_truth.get("current_day_status"), dict) else {}
    if current_truth_day:
        candidates.append(current_truth_day)
    candidates.append(payload)
    candidate_keys = ("candidate_snapshot_time", "candidate_snapshot_generated_at", "candidate_snapshot_timestamp")
    for item in candidates:
        for key in candidate_keys:
            if item.get(key):
                return str(item[key])
    for item in candidates:
        for key in ("produced_at_utc", "generated_at_utc"):
            if item.get(key):
                return str(item[key])
    return ""


def _status(diagnostics: dict[str, Any]) -> str:
    interpretation = str(diagnostics.get("operator_interpretation") or "").upper()
    status = str(diagnostics.get("candidate_generation_status") or "UNKNOWN").upper()
    if interpretation in {"DATA_BLOCKED", "PARTIAL_RUN", "PARTIAL_CONTEXT"}:
        return "PARTIAL"
    if status in {"PARTIAL", "DATA_BLOCKED"}:
        return "PARTIAL"
    if status in {"RAN", "SUCCESS", "COMPLETED"}:
        return "SUCCESS"
    return status


def _completed_at(diagnostics: dict[str, Any], *paths: str) -> str:
    for key in ("completed_at_utc", "run_completed_at_utc", "generated_at_utc", "generated_at", "timestamp_utc"):
        if diagnostics.get(key):
            return str(diagnostics[key])
    nested = diagnostics.get("candidate_contracts") if isinstance(diagnostics.get("candidate_contracts"), dict) else {}
    for key in ("generated_at_utc", "generated_at"):
        if nested.get(key):
            return str(nested[key])
    mtimes = []
    for raw in paths:
        try:
            path = Path(raw)
            if path.exists():
                mtimes.append(datetime.fromtimestamp(path.stat().st_mtime, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"))
        except Exception:
            pass
    return sorted(mtimes)[-1] if mtimes else datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_id(*, day_utc: str, command: str, completed_at: str, diagnostics_path: str) -> str:
    digest = hashlib.sha256(f"{day_utc}|{command}|{completed_at}|{diagnostics_path}".encode("utf-8")).hexdigest()[:16]
    return f"aegis-run:{day_utc}:{digest}"


def _artifact_mismatch(*, diagnostic_candidate_outputs: int, valid_candidate_contracts: int, rejected_candidate_contracts: int, contracts_payload: dict[str, Any]) -> tuple[str, str]:
    if diagnostic_candidate_outputs > valid_candidate_contracts and valid_candidate_contracts == 0 and rejected_candidate_contracts > 0:
        reasons = contracts_payload.get("rejection_reasons") if isinstance(contracts_payload.get("rejection_reasons"), list) else []
        top_reason = str((reasons[0] if reasons else {}).get("reason") or "CONTRACT_VALIDATION_REJECTED")
        return "DIAGNOSTIC_OUTPUTS_REJECTED_BY_CONTRACT_VALIDATION", top_reason
    if diagnostic_candidate_outputs != valid_candidate_contracts:
        return "DIAGNOSTIC_OUTPUT_COUNT_DIFFERS_FROM_VALID_CONTRACTS", "DIAGNOSTIC_OUTPUT_COUNT_DIFFERS_FROM_VALID_CONTRACTS"
    return "", ""


def _mismatch_explanation(*, status: str, reason: str, contracts_payload: dict[str, Any], candidate_contracts_path: str, diagnostics_path: str) -> dict[str, Any]:
    if not status:
        return {}
    reasons = contracts_payload.get("rejection_reasons") if isinstance(contracts_payload.get("rejection_reasons"), list) else []
    rejected = contracts_payload.get("rejected_raw_signals") if isinstance(contracts_payload.get("rejected_raw_signals"), list) else []
    sample = next((row for row in rejected if isinstance(row, dict)), {})
    return {
        "status": status,
        "reason": reason,
        "operator_message": "Diagnostics found candidate-like outputs, but 0 passed candidate contract validation.",
        "rejection_reasons": reasons,
        "sample_rejected_symbols": [str(row.get("symbol") or "") for row in rejected[:10] if isinstance(row, dict)],
        "sample_stale_diagnostics": _sample_stale_diagnostics(sample),
        "price_timestamp": str(sample.get("price_timestamp") or sample.get("entry_reference_price_timestamp_utc") or ""),
        "candidate_snapshot_timestamp": str(sample.get("candidate_snapshot_timestamp") or sample.get("candidate_snapshot_timestamp_utc") or ""),
        "freshness_window_seconds": sample.get("freshness_window_seconds"),
        "stale_by_seconds": sample.get("stale_by_seconds"),
        "freshness_policy_mode": str(sample.get("freshness_policy_mode") or ""),
        "stale_reason": str(sample.get("stale_reason") or ""),
        "evidence_path": candidate_contracts_path or diagnostics_path,
        "next_repair_action": "Refresh current-session entry reference price evidence, then run signal evidence graph, candidate contracts, candidate diagnostics, paper review queue, and canonical operator state.",
    }


def _sample_stale_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    freshness = row.get("entry_reference_price_freshness") if isinstance(row.get("entry_reference_price_freshness"), dict) else {}
    return {
        "symbol": str(row.get("symbol") or ""),
        "raw_signal_id": str(row.get("raw_signal_id") or ""),
        "price_timestamp": str(row.get("price_timestamp") or row.get("entry_reference_price_timestamp_utc") or freshness.get("price_timestamp") or ""),
        "candidate_snapshot_timestamp": str(row.get("candidate_snapshot_timestamp") or row.get("candidate_snapshot_timestamp_utc") or freshness.get("candidate_snapshot_timestamp") or ""),
        "freshness_window_seconds": row.get("freshness_window_seconds", freshness.get("freshness_window_seconds")),
        "stale_by_seconds": row.get("stale_by_seconds", freshness.get("stale_by_seconds")),
        "freshness_policy_mode": str(row.get("freshness_policy_mode") or freshness.get("freshness_policy_mode") or ""),
        "stale_reason": str(row.get("stale_reason") or freshness.get("stale_reason") or ""),
    }


def _source_hashes(paths: list[str]) -> dict[str, str]:
    out = {}
    for raw in paths:
        if not raw:
            continue
        path = Path(raw)
        if not path.exists():
            continue
        out[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(fallback)
