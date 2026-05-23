from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1, read_evidence_events_v1
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1
from ops.aegis.trade_lifecycle.captured_ticket_history_v1 import (
    build_captured_ticket_history_v1,
    latest_capture_record_for_ticket_v1,
)


SCHEMA_ID = "trade_ticket_lineage"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "trade_ticket_lineage_v1"
REPORT_FILENAME = "trade_ticket_lineage.v1.json"

MARKET_SCHEMA_ID = "market_freshness_evidence"
PAPER_INTENT_SCHEMA_ID = "paper_intent_evidence"
CONVERSION_SCHEMA_ID = "paper_conversion_evidence"
SUBMIT_SCHEMA_ID = "submit_boundary_precheck"

LINEAGE_STATUSES = {
    "ACTIVE_CURRENT",
    "STALE_CONTRACT",
    "STALE_RUNTIME",
    "MISSING_SUBMIT_BOUNDARY",
    "MISSING_MARKET_FRESHNESS",
    "MISSING_PAPER_INTENT",
    "MISSING_CONVERSION",
    "HISTORICAL_READ_ONLY",
    "CAPTURED_HISTORICAL",
    "INVALIDATED",
}


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def sha256_file_v1(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json_object_v1(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def source_ref_v1(path: Path | None, artifact_id: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    exists = bool(path and path.exists() and path.is_file())
    return {
        "artifact_id": artifact_id,
        "path": str(path or ""),
        "exists": exists,
        "sha256": sha256_file_v1(path) if exists else "",
        "schema_id": str((payload or {}).get("schema_id") or ""),
    }


def canonical_construction_contract_v1(construction: Mapping[str, Any]) -> dict[str, Any]:
    stable_fields = [
        "selected_exposure_intent_id",
        "symbol",
        "direction",
        "sleeve_id",
        "engine_id",
        "source_day",
        "source_run_id",
        "portfolio_gate_decision_id",
        "market_data_snapshot_id",
        "market_data_latest_session",
        "entry_reference_price",
        "entry_reference_source",
        "suggested_quantity",
        "suggested_notional",
        "allocation_percent",
        "stop_price",
        "invalidation_level",
        "stop_policy_source",
        "stop_policy_id",
        "stop_loss_bps",
        "invalidation_policy",
        "expected_holding_days",
        "risk_per_share",
        "max_loss_estimate",
        "estimated_notional_risk_pct",
        "risk_budget_source",
    ]
    source_artifacts = []
    for row in construction.get("source_artifacts") if isinstance(construction.get("source_artifacts"), list) else []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("artifact_id") or "") in {"submit_boundary_status_v1", "aegis_runtime_evaluation", "exposure_intent_conversion", "capital_authority_allocation_v1", "allocation_decision_v1"}:
            continue
        source_artifacts.append(
            {
                "artifact_id": str(row.get("artifact_id") or ""),
                "path": str(row.get("path") or ""),
                "sha256": str(row.get("sha256") or ""),
                "schema_id": str(row.get("schema_id") or ""),
            }
        )
    contract = {
        "schema_id": "paper_trade_construction_contract",
        "schema_version": "v1",
        "contract_scope": "manual_capture_ticket_construction",
        "fields": {field: construction.get(field) for field in stable_fields},
        "source_artifacts": sorted(source_artifacts, key=lambda row: (row["artifact_id"], row["path"])),
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
    }
    contract_hash = stable_hash_v1(contract)
    return {
        **contract,
        "construction_contract_id": "construction-contract:" + contract_hash[:24],
        "construction_contract_hash": contract_hash,
    }


def ticket_id_v1(construction: Mapping[str, Any]) -> str:
    payload = {
        "selected_exposure_intent_id": str(construction.get("selected_exposure_intent_id") or ""),
        "symbol": str(construction.get("symbol") or "").upper(),
        "sleeve_id": str(construction.get("sleeve_id") or construction.get("engine_id") or ""),
        "source_day": str(construction.get("source_day") or ""),
        "source_run_id": str(construction.get("source_run_id") or ""),
        "construction_contract_hash": str(construction.get("construction_contract_hash") or canonical_construction_contract_v1(construction)["construction_contract_hash"]),
    }
    return "ticket:" + stable_hash_v1(payload)[:24]


def evidence_dir_v1(*, truth_root: Path | str, family: str, day_utc: str, ticket_id: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / family / str(day_utc) / ticket_id.replace(":", "_")


def trade_ticket_lineage_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    return evidence_dir_v1(truth_root=truth_root, family=REPORT_FAMILY, day_utc=day_utc, ticket_id=ticket_id) / REPORT_FILENAME


def market_freshness_evidence_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    return evidence_dir_v1(truth_root=truth_root, family="market_freshness_evidence_v1", day_utc=day_utc, ticket_id=ticket_id) / "market_freshness_evidence.v1.json"


def paper_intent_evidence_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    return evidence_dir_v1(truth_root=truth_root, family="paper_intent_evidence_v1", day_utc=day_utc, ticket_id=ticket_id) / "paper_intent_evidence.v1.json"


def paper_conversion_evidence_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    return evidence_dir_v1(truth_root=truth_root, family="paper_conversion_evidence_v1", day_utc=day_utc, ticket_id=ticket_id) / "paper_conversion_evidence.v1.json"


def submit_boundary_precheck_path_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> Path:
    return evidence_dir_v1(truth_root=truth_root, family="submit_boundary_precheck_v1", day_utc=day_utc, ticket_id=ticket_id) / "submit_boundary_precheck.v1.json"


def _source_by_artifact(construction: Mapping[str, Any], artifact_id: str) -> tuple[dict[str, Any], Path | None]:
    for row in construction.get("source_artifacts") if isinstance(construction.get("source_artifacts"), list) else []:
        if isinstance(row, Mapping) and str(row.get("artifact_id") or "") == artifact_id:
            path_text = str(row.get("path") or "")
            return dict(row), Path(path_text).expanduser().resolve() if path_text else None
    return {}, None


def _latest_conversion_report_v1(*, truth_root: Path, day_utc: str, selected_exposure_intent_id: str) -> tuple[Path | None, dict[str, Any]]:
    base = truth_root / "reports" / "exposure_intent_paper_submission_package_v1" / day_utc
    matches: list[tuple[int, Path, dict[str, Any]]] = []
    if base.exists():
        for path in base.glob("*/exposure_intent_paper_submission_package.v1.json"):
            payload = read_json_object_v1(path)
            if str(payload.get("exposure_intent_id") or payload.get("selected_exposure_intent_id") or "") == selected_exposure_intent_id:
                matches.append((path.stat().st_mtime_ns, path.resolve(), payload))
    if not matches:
        return None, {}
    _mtime, path, payload = sorted(matches, key=lambda row: (row[0], str(row[1])))[-1]
    return path, payload


def build_market_freshness_evidence_v1(*, truth_root: Path | str, construction: Mapping[str, Any], generated_at_utc: str | None = None) -> dict[str, Any]:
    generated = generated_at_utc or now_utc_v1()
    day = str(construction.get("source_day") or generated[:10])
    ticket_id = ticket_id_v1(construction)
    symbol = str(construction.get("symbol") or "").upper()
    market_ref, market_path = _source_by_artifact(construction, "market_data_snapshot_v1")
    observed = str(construction.get("market_data_latest_session") or "")
    required = day
    stale_symbols = [] if symbol and observed == required and str(market_ref.get("sha256") or construction.get("market_data_snapshot_id") or "") else [symbol or "UNKNOWN"]
    status = "VALID" if not stale_symbols else "INVALID"
    payload = {
        "schema_id": MARKET_SCHEMA_ID,
        "schema_version": "v1",
        "artifact_id": "",
        "ticket_id": ticket_id,
        "day_utc": day,
        "runtime_evaluation_hash": str(construction.get("runtime_evaluation_hash") or ""),
        "required_symbols": [symbol] if symbol else [],
        "price_source_hashes": {symbol: str(market_ref.get("sha256") or construction.get("market_data_snapshot_id") or "")} if symbol else {},
        "source_timestamps": {symbol: observed} if symbol else {},
        "freshness_ttl": "same_session",
        "stale_symbols": stale_symbols,
        "missing_symbols": [] if symbol else ["UNKNOWN"],
        "validation_status": status,
        "source_artifacts": [source_ref_v1(market_path, "market_data_snapshot_v1", {})],
        "generated_at_utc": generated,
    }
    payload["artifact_id"] = f"market_freshness_evidence_v1:{day}:{stable_hash_v1(payload)[:20]}"
    payload["evidence_hash"] = stable_hash_v1({**payload, "evidence_hash": ""})
    return payload


def build_paper_intent_evidence_v1(*, construction: Mapping[str, Any], generated_at_utc: str | None = None) -> dict[str, Any]:
    generated = generated_at_utc or now_utc_v1()
    day = str(construction.get("source_day") or generated[:10])
    ticket_id = ticket_id_v1(construction)
    selected_id = str(construction.get("selected_exposure_intent_id") or "")
    payload = {
        "schema_id": PAPER_INTENT_SCHEMA_ID,
        "schema_version": "v1",
        "artifact_id": "",
        "ticket_id": ticket_id,
        "day_utc": day,
        "runtime_evaluation_hash": str(construction.get("runtime_evaluation_hash") or ""),
        "paper_intent_id": "paper-intent:" + stable_hash_v1({"ticket_id": ticket_id, "selected_id": selected_id})[:24] if selected_id else "",
        "selected_exposure_intent_id": selected_id,
        "intended_action_category": "MANUAL_CAPTURE_RECORD_APPEND",
        "manual_capture_only": True,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "operator_boundary_constraints": ["append_manual_capture_record_only", "no_broker_submit", "no_ticket_mutation"],
        "validation_status": "VALID" if selected_id else "INVALID",
        "generated_at_utc": generated,
    }
    payload["artifact_id"] = f"paper_intent_evidence_v1:{day}:{stable_hash_v1(payload)[:20]}"
    payload["evidence_hash"] = stable_hash_v1({**payload, "evidence_hash": ""})
    return payload


def build_paper_conversion_evidence_v1(*, construction: Mapping[str, Any], truth_root: Path | str | None = None, generated_at_utc: str | None = None) -> dict[str, Any]:
    generated = generated_at_utc or now_utc_v1()
    day = str(construction.get("source_day") or generated[:10])
    ticket_id = ticket_id_v1(construction)
    conversion_ref, conversion_path = _source_by_artifact(construction, "exposure_intent_conversion")
    selected_ref, selected_path = _source_by_artifact(construction, "selected_exposure_intent")
    conversion = read_json_object_v1(conversion_path)
    selected_id = str(construction.get("selected_exposure_intent_id") or "")
    if not conversion and truth_root and selected_id:
        latest_path, latest_conversion = _latest_conversion_report_v1(truth_root=Path(truth_root).expanduser().resolve(), day_utc=day, selected_exposure_intent_id=selected_id)
        if latest_path is not None:
            conversion_path = latest_path
            conversion = latest_conversion
            conversion_ref = source_ref_v1(latest_path, "exposure_intent_conversion", latest_conversion)
    conversion_id = str(conversion.get("exposure_intent_id") or conversion.get("selected_exposure_intent_id") or "")
    selected_matches = bool(selected_id and conversion_id == selected_id)
    raw_status = str(conversion.get("status") or "").upper()
    blocker = str(conversion.get("blocker_code") or "")
    valid_statuses = {"PASS", "READY", "COMPLETE", "COMPLETED", "VALID"}
    valid = selected_matches and bool(conversion) and (raw_status in valid_statuses or bool(conversion.get("paper_trade_intent_created") is True)) and not blocker
    contract = canonical_construction_contract_v1(construction)
    payload = {
        "schema_id": CONVERSION_SCHEMA_ID,
        "schema_version": "v1",
        "artifact_id": "",
        "ticket_id": ticket_id,
        "day_utc": day,
        "runtime_evaluation_hash": str(construction.get("runtime_evaluation_hash") or ""),
        "conversion_id": "paper-conversion:" + stable_hash_v1({"ticket_id": ticket_id, "conversion_hash": conversion_ref.get("sha256", "")})[:24] if conversion else "",
        "selected_exposure_intent_id": selected_id,
        "candidate_to_ticket_conversion_trace": {
            "conversion_artifact_status": raw_status,
            "conversion_artifact_blocker_code": blocker,
            "conversion_matches_selected_exposure": selected_matches,
            "selected_exposure_intent_id": selected_id,
            "conversion_exposure_intent_id": conversion_id,
        },
        "source_candidate_artifact_hash": str(selected_ref.get("sha256") or ""),
        "conversion_artifact_hash": str(conversion_ref.get("sha256") or ""),
        "conversion_artifact_path": str(conversion_path or ""),
        "construction_contract_hash": contract["construction_contract_hash"],
        "allocation_status": str(conversion.get("capital_authority_status") or ""),
        "allocation_artifact_path": str(conversion.get("capital_authority_path") or ""),
        "allocation_artifact_hash": str(conversion.get("capital_authority_hash") or ""),
        "allocation_limit_used": conversion.get("allocation_limit_used") if isinstance(conversion.get("allocation_limit_used"), dict) else {},
        "risk_contract_status": str(conversion.get("risk_contract_status") or ""),
        "risk_contract_path": str(conversion.get("risk_contract_path") or ""),
        "risk_contract_hash": str(conversion.get("risk_contract_hash") or ""),
        "risk_contract_runtime_evaluation_hash": str(conversion.get("risk_contract_runtime_evaluation_hash") or ""),
        "risk_contract_construction_contract_hash": str(conversion.get("risk_contract_construction_contract_hash") or ""),
        "risk_contract_capital_allocation_hash": str(conversion.get("risk_contract_capital_allocation_hash") or ""),
        "risk_measure": str(conversion.get("risk_measure") or ""),
        "risk_measure_definition": conversion.get("risk_measure_definition") if isinstance(conversion.get("risk_measure_definition"), dict) else {},
        "risk_limit_used": conversion.get("risk_limit_used") if isinstance(conversion.get("risk_limit_used"), dict) else {},
        "candidate_identity_status": str(conversion.get("candidate_identity_status") or ""),
        "target_day_admission_status": str(conversion.get("target_day_admission_status") or ""),
        "target_day_admission_path": str(conversion.get("target_day_admission_path") or ""),
        "target_day_admission_hash": str(conversion.get("target_day_admission_hash") or ""),
        "day_activation_status": str(conversion.get("day_activation_status") or ""),
        "day_activation_path": str(conversion.get("day_activation_path") or ""),
        "day_activation_hash": str(conversion.get("day_activation_hash") or ""),
        "global_context_status": str(conversion.get("global_context_status") or ""),
        "global_context_path": str(conversion.get("global_context_path") or ""),
        "global_context_hash": str(conversion.get("global_context_hash") or ""),
        "economic_state_status": str(conversion.get("economic_state_status") or ""),
        "economic_state_path": str(conversion.get("economic_state_path") or ""),
        "economic_state_hash": str(conversion.get("economic_state_hash") or ""),
        "economic_state_build_path": str(conversion.get("economic_state_build_path") or ""),
        "cash_ledger_path": str(conversion.get("cash_ledger_path") or ""),
        "cash_ledger_hash": str(conversion.get("cash_ledger_hash") or ""),
        "cash_ledger_source_type": str(conversion.get("cash_ledger_source_type") or ""),
        "positions_snapshot_path": str(conversion.get("positions_snapshot_path") or ""),
        "positions_snapshot_hash": str(conversion.get("positions_snapshot_hash") or ""),
        "positions_source_type": str(conversion.get("positions_source_type") or ""),
        "position_lifecycle_path": str(conversion.get("position_lifecycle_path") or ""),
        "position_lifecycle_hash": str(conversion.get("position_lifecycle_hash") or ""),
        "accounting_nav_path": str(conversion.get("accounting_nav_path") or ""),
        "accounting_nav_hash": str(conversion.get("accounting_nav_hash") or ""),
        "accounting_nav_source_type": str(conversion.get("accounting_nav_source_type") or ""),
        "candidate_identity_set_path": str(conversion.get("candidate_identity_set_path") or ""),
        "candidate_identity_set_hash": str(conversion.get("candidate_identity_set_hash") or ""),
        "expected_candidate_id": str(conversion.get("expected_candidate_id") or ""),
        "actual_phasec_candidate_id": str(conversion.get("actual_phasec_candidate_id") or ""),
        "actual_phasec_order_plan_path": str(conversion.get("actual_phasec_order_plan_path") or ""),
        "stale_order_plan_reason": str(conversion.get("stale_order_plan_reason") or ""),
        "risk_quantity_notional_trace": {
            "entry_reference_price": construction.get("entry_reference_price") or "",
            "suggested_quantity": construction.get("suggested_quantity"),
            "suggested_notional": construction.get("suggested_notional") or "",
            "stop_price": construction.get("stop_price") or "",
            "risk_per_share": construction.get("risk_per_share") or "",
            "max_loss_estimate": construction.get("max_loss_estimate") or "",
        },
        "validation_status": "VALID" if valid else "INVALID",
        "blocker_codes": [] if valid else [code for code in ["CONVERSION_MISSING" if not conversion else "", "CONVERSION_MISMATCH" if conversion and not selected_matches else "", blocker] if code],
        "source_artifacts": [
            source_ref_v1(selected_path, "selected_exposure_intent", {}),
            source_ref_v1(conversion_path, "exposure_intent_conversion", conversion),
        ],
        "source_event_ids": _event_ids_for_schemas(root=Path(truth_root).expanduser().resolve(), day=day, schemas={CONVERSION_SCHEMA_ID}) if truth_root else [],
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "generated_at_utc": generated,
    }
    payload["artifact_id"] = f"paper_conversion_evidence_v1:{day}:{stable_hash_v1(payload)[:20]}"
    payload["evidence_hash"] = stable_hash_v1({**payload, "evidence_hash": ""})
    return payload


def write_ticket_evidence_set_v1(*, truth_root: Path | str, construction: Mapping[str, Any], generated_at_utc: str | None = None, emit_events: bool = True) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(construction.get("source_day") or (generated_at_utc or now_utc_v1())[:10])
    ticket_id = ticket_id_v1(construction)
    evidences = {
        "market_freshness": build_market_freshness_evidence_v1(truth_root=root, construction=construction, generated_at_utc=generated_at_utc),
        "paper_intent": build_paper_intent_evidence_v1(construction=construction, generated_at_utc=generated_at_utc),
        "conversion": build_paper_conversion_evidence_v1(construction=construction, truth_root=root, generated_at_utc=generated_at_utc),
    }
    paths = {
        "market_freshness": market_freshness_evidence_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id),
        "paper_intent": paper_intent_evidence_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id),
        "conversion": paper_conversion_evidence_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id),
    }
    for name, path in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_json_bytes_v1(evidences[name]) + b"\n")
        if emit_events:
            _emit_artifact_events(root=root, day=day, path=path, payload=evidences[name], producer="ops.aegis.trade_ticket_lineage_v1")
    return {"ticket_id": ticket_id, "evidences": evidences, "paths": {key: str(path) for key, path in paths.items()}}


def build_trade_ticket_lineage_v1(
    *,
    truth_root: Path | str,
    construction: Mapping[str, Any],
    generated_at_utc: str | None = None,
    require_submit_boundary: bool = True,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = generated_at_utc or now_utc_v1()
    day = str(construction.get("source_day") or generated[:10])
    contract = canonical_construction_contract_v1(construction)
    ticket_id = ticket_id_v1({**dict(construction), **contract})
    captured_record = latest_capture_record_for_ticket_v1(
        truth_root=root,
        day_utc=day,
        ticket_id=ticket_id,
        selected_exposure_intent_id=str(construction.get("selected_exposure_intent_id") or ""),
    )
    if captured_record:
        history = build_captured_ticket_history_v1(truth_root=root, day_utc=day, capture_record=captured_record)
        original_lineage = captured_record.get("trade_ticket_lineage") if isinstance(captured_record.get("trade_ticket_lineage"), Mapping) else {}
        evidence_status = dict(original_lineage.get("evidence_status") if isinstance(original_lineage.get("evidence_status"), Mapping) else {})
        evidence_status["submit_boundary"] = "CAPTURE_TIME_VALIDATED"
        history_paths = [str(item) for item in history.get("source_artifact_paths") or [] if str(item)]
        original_paths = [str(item) for item in original_lineage.get("source_artifact_paths") or [] if str(item)]
        payload = {
            **dict(original_lineage),
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "artifact_id": "",
            "ticket_id": ticket_id,
            "exposure_id": str(construction.get("selected_exposure_intent_id") or original_lineage.get("exposure_id") or ""),
            "sleeve_id": str(construction.get("sleeve_id") or construction.get("engine_id") or original_lineage.get("sleeve_id") or ""),
            "symbol": str(construction.get("symbol") or original_lineage.get("symbol") or "").upper(),
            "side": str(original_lineage.get("side") or _side_from_direction(str(construction.get("direction") or ""))),
            "day_utc": day,
            "runtime_evaluation_hash": str(history.get("original_runtime_evaluation_hash") or original_lineage.get("runtime_evaluation_hash") or ""),
            "current_runtime_evaluation_hash": str(read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day).get("deterministic_output_hash") or ""),
            "submit_boundary_hash": str(history.get("original_submit_boundary_hash") or original_lineage.get("submit_boundary_hash") or ""),
            "construction_contract_hash": str(history.get("original_construction_contract_hash") or original_lineage.get("construction_contract_hash") or contract["construction_contract_hash"]),
            "paper_trade_construction_id": str(history.get("original_paper_trade_construction_id") or original_lineage.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
            "source_artifact_paths": sorted(set([*original_paths, *history_paths])),
            "source_event_ids": sorted(set([*[str(item) for item in original_lineage.get("source_event_ids") or [] if str(item)], *[str(item) for item in history.get("event_ids") or [] if str(item)]])),
            "lineage_status": "CAPTURED_HISTORICAL",
            "lifecycle_state": "CAPTURED_HISTORICAL",
            "historical_state": "CAPTURED_HISTORICAL",
            "blocker_codes": [],
            "editable": False,
            "read_only": True,
            "historical_read_only": True,
            "captured_read_only": True,
            "capture_record_id": str(history.get("capture_record_id") or ""),
            "capture_event_ids": [str(item) for item in history.get("event_ids") or [] if str(item)],
            "captured_at_utc": str(history.get("captured_at_utc") or ""),
            "operator_id": str(history.get("operator_id") or ""),
            "capture_time_lineage_hash": str(history.get("original_lineage_hash") or ""),
            "capture_time_submit_boundary_hash": str(history.get("original_submit_boundary_hash") or ""),
            "post_capture_authority": "IMMUTABLE_CAPTURE_EVIDENCE",
            "current_runtime_revalidation_required": False,
            "submit_boundary_revalidation_required": False,
            "evidence_status": evidence_status,
            "generated_at_utc": str(captured_record.get("created_at_utc") or history.get("record_created_at_utc") or generated),
            "broker_execution_allowed": False,
            "order_routing_allowed": False,
            "autonomous_execution_allowed": False,
        }
        payload["artifact_id"] = f"trade_ticket_lineage_v1:{day}:{stable_hash_v1(payload)[:20]}"
        payload["lineage_hash"] = stable_hash_v1({**payload, "lineage_hash": ""})
        return payload
    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=day)
    runtime_eval = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day)
    runtime_hash = str(runtime_eval.get("deterministic_output_hash") or "")
    construction_runtime_hash = str(construction.get("runtime_evaluation_hash") or runtime_hash)

    market_path = market_freshness_evidence_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id)
    paper_intent_path = paper_intent_evidence_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id)
    conversion_path = paper_conversion_evidence_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id)
    submit_path = submit_boundary_precheck_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id)
    market = read_json_object_v1(market_path)
    paper_intent = read_json_object_v1(paper_intent_path)
    conversion = read_json_object_v1(conversion_path)
    submit = read_json_object_v1(submit_path)

    blockers: list[str] = []
    if runtime_hash and construction_runtime_hash and construction_runtime_hash != runtime_hash:
        blockers.append("STALE_RUNTIME")
    if not runtime_hash:
        blockers.append("STALE_RUNTIME")
    if str(market.get("validation_status") or "") != "VALID":
        blockers.append("MISSING_MARKET_FRESHNESS")
    if str(paper_intent.get("validation_status") or "") != "VALID":
        blockers.append("MISSING_PAPER_INTENT")
    if str(conversion.get("validation_status") or "") != "VALID":
        blockers.append("MISSING_CONVERSION")
    submit_valid = str(submit.get("validation_status") or "") == "VALIDATED" and str(submit.get("ticket_id") or "") == ticket_id
    if require_submit_boundary and not submit_valid:
        blockers.append("MISSING_SUBMIT_BOUNDARY")

    status = blockers[0] if blockers else "ACTIVE_CURRENT"
    if status not in LINEAGE_STATUSES:
        status = "INVALIDATED"
    source_paths = [str(path) for path in [market_path, paper_intent_path, conversion_path, submit_path] if path.exists()]
    event_ids = _event_ids_for_schemas(root=root, day=day, schemas={MARKET_SCHEMA_ID, PAPER_INTENT_SCHEMA_ID, CONVERSION_SCHEMA_ID, SUBMIT_SCHEMA_ID, SCHEMA_ID})
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "ticket_id": ticket_id,
        "exposure_id": str(construction.get("selected_exposure_intent_id") or ""),
        "sleeve_id": str(construction.get("sleeve_id") or construction.get("engine_id") or ""),
        "symbol": str(construction.get("symbol") or "").upper(),
        "side": _side_from_direction(str(construction.get("direction") or "")),
        "day_utc": day,
        "sleeve_run_id": str(construction.get("source_run_id") or ""),
        "runtime_evaluation_hash": runtime_hash,
        "universe_hash": str(construction.get("universe_hash") or ""),
        "market_data_hash": str(construction.get("market_data_snapshot_id") or ""),
        "construction_contract_id": contract["construction_contract_id"],
        "construction_contract_hash": contract["construction_contract_hash"],
        "construction_contract": contract,
        "paper_trade_construction_id": str(construction.get("construction_id") or ""),
        "submit_boundary_id": str(submit.get("submit_boundary_id") or ""),
        "submit_boundary_hash": str(submit.get("submit_boundary_hash") or submit.get("evidence_hash") or ""),
        "market_freshness_id": str(market.get("artifact_id") or ""),
        "paper_intent_id": str(paper_intent.get("paper_intent_id") or ""),
        "conversion_id": str(conversion.get("conversion_id") or ""),
        "allocation_status": str(conversion.get("allocation_status") or ""),
        "allocation_artifact_path": str(conversion.get("allocation_artifact_path") or ""),
        "allocation_artifact_hash": str(conversion.get("allocation_artifact_hash") or ""),
        "allocation_limit_used": conversion.get("allocation_limit_used") if isinstance(conversion.get("allocation_limit_used"), dict) else {},
        "risk_contract_status": str(conversion.get("risk_contract_status") or ""),
        "risk_contract_path": str(conversion.get("risk_contract_path") or ""),
        "risk_contract_hash": str(conversion.get("risk_contract_hash") or ""),
        "risk_measure": str(conversion.get("risk_measure") or ""),
        "risk_measure_definition": conversion.get("risk_measure_definition") if isinstance(conversion.get("risk_measure_definition"), dict) else {},
        "risk_limit_used": conversion.get("risk_limit_used") if isinstance(conversion.get("risk_limit_used"), dict) else {},
        "candidate_identity_status": str(conversion.get("candidate_identity_status") or ""),
        "target_day_admission_status": str(conversion.get("target_day_admission_status") or ""),
        "target_day_admission_path": str(conversion.get("target_day_admission_path") or ""),
        "target_day_admission_hash": str(conversion.get("target_day_admission_hash") or ""),
        "day_activation_status": str(conversion.get("day_activation_status") or ""),
        "day_activation_path": str(conversion.get("day_activation_path") or ""),
        "day_activation_hash": str(conversion.get("day_activation_hash") or ""),
        "global_context_status": str(conversion.get("global_context_status") or ""),
        "global_context_path": str(conversion.get("global_context_path") or ""),
        "global_context_hash": str(conversion.get("global_context_hash") or ""),
        "economic_state_status": str(conversion.get("economic_state_status") or ""),
        "economic_state_path": str(conversion.get("economic_state_path") or ""),
        "economic_state_hash": str(conversion.get("economic_state_hash") or ""),
        "economic_state_build_path": str(conversion.get("economic_state_build_path") or ""),
        "cash_ledger_path": str(conversion.get("cash_ledger_path") or ""),
        "cash_ledger_hash": str(conversion.get("cash_ledger_hash") or ""),
        "cash_ledger_source_type": str(conversion.get("cash_ledger_source_type") or ""),
        "positions_snapshot_path": str(conversion.get("positions_snapshot_path") or ""),
        "positions_snapshot_hash": str(conversion.get("positions_snapshot_hash") or ""),
        "positions_source_type": str(conversion.get("positions_source_type") or ""),
        "position_lifecycle_path": str(conversion.get("position_lifecycle_path") or ""),
        "position_lifecycle_hash": str(conversion.get("position_lifecycle_hash") or ""),
        "accounting_nav_path": str(conversion.get("accounting_nav_path") or ""),
        "accounting_nav_hash": str(conversion.get("accounting_nav_hash") or ""),
        "accounting_nav_source_type": str(conversion.get("accounting_nav_source_type") or ""),
        "candidate_identity_set_path": str(conversion.get("candidate_identity_set_path") or ""),
        "candidate_identity_set_hash": str(conversion.get("candidate_identity_set_hash") or ""),
        "expected_candidate_id": str(conversion.get("expected_candidate_id") or ""),
        "actual_phasec_candidate_id": str(conversion.get("actual_phasec_candidate_id") or ""),
        "actual_phasec_order_plan_path": str(conversion.get("actual_phasec_order_plan_path") or ""),
        "stale_order_plan_reason": str(conversion.get("stale_order_plan_reason") or ""),
        "source_artifact_paths": source_paths,
        "source_event_ids": event_ids,
        "lineage_status": status,
        "blocker_codes": sorted(set(blockers)),
        "editable": status == "ACTIVE_CURRENT",
        "read_only": status != "ACTIVE_CURRENT",
        "historical_read_only": status != "ACTIVE_CURRENT",
        "evidence_status": {
            "market_freshness": str(market.get("validation_status") or "MISSING"),
            "paper_intent": str(paper_intent.get("validation_status") or "MISSING"),
            "conversion": str(conversion.get("validation_status") or "MISSING"),
            "allocation": str(conversion.get("allocation_status") or "MISSING"),
            "risk_contract": str(conversion.get("risk_contract_status") or "MISSING"),
            "candidate_identity": str(conversion.get("candidate_identity_status") or "MISSING"),
            "target_day_admission": str(conversion.get("target_day_admission_status") or "MISSING"),
            "day_activation": str(conversion.get("day_activation_status") or "MISSING"),
            "global_context": str(conversion.get("global_context_status") or "MISSING"),
            "economic_state": str(conversion.get("economic_state_status") or "MISSING"),
            "cash_ledger": str(conversion.get("cash_ledger_source_type") or "MISSING"),
            "positions": str(conversion.get("positions_source_type") or "MISSING"),
            "accounting_nav": str(conversion.get("accounting_nav_source_type") or "MISSING"),
            "submit_boundary": str(submit.get("validation_status") or "MISSING"),
        },
        "generated_at_utc": generated,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
    }
    payload["artifact_id"] = f"trade_ticket_lineage_v1:{day}:{stable_hash_v1(payload)[:20]}"
    payload["lineage_hash"] = stable_hash_v1({**payload, "lineage_hash": ""})
    return payload


def write_trade_ticket_lineage_v1(*, truth_root: Path | str, payload: Mapping[str, Any], emit_events: bool = True) -> Path:
    root = Path(truth_root).expanduser().resolve()
    day = str(payload.get("day_utc") or "")
    ticket_id = str(payload.get("ticket_id") or "")
    path = trade_ticket_lineage_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    if emit_events:
        _emit_artifact_events(root=root, day=day, path=path, payload=dict(payload), producer="ops.aegis.trade_ticket_lineage_v1")
    return path


def read_trade_ticket_lineage_v1(*, truth_root: Path | str, day_utc: str, ticket_id: str) -> dict[str, Any]:
    return read_json_object_v1(trade_ticket_lineage_path_v1(truth_root=truth_root, day_utc=day_utc, ticket_id=ticket_id))


def _manual_capture_blocker_v1(
    *,
    code: str,
    message: str,
    ticket_id: str = "",
    lineage: Mapping[str, Any] | None = None,
    expected_hash: str = "",
    received_hash: str = "",
    exact_blocker: str = "",
    recommended_ui_action: str = "Refresh ticket and retry from the current governed ticket.",
) -> dict[str, Any]:
    current_hash = str((lineage or {}).get("lineage_hash") or "")
    return {
        "allowed": False,
        "ok": False,
        "blocker_code": code,
        "blocker_codes": [code],
        "error_code": code,
        "message": message,
        "human_message": message,
        "exact_blocker": exact_blocker or code,
        "ticket_id": ticket_id,
        "current_ticket_id": ticket_id,
        "current_ticket_hash": current_hash,
        "expected_hash": expected_hash,
        "received_hash": received_hash,
        "recommended_ui_action": recommended_ui_action,
        "lineage": dict(lineage or {}),
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
    }


def manual_capture_save_blockers_v1(*, truth_root: Path | str, day_utc: str, request_payload: Mapping[str, Any], current_construction: Mapping[str, Any]) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    requested_ticket_id = str(request_payload.get("ticket_id") or "").strip()
    current_ticket_id = requested_ticket_id or ticket_id_v1(current_construction)
    client_bound = any(
        str(request_payload.get(key) or "").strip()
        for key in ("ticket_id", "runtime_evaluation_hash", "ticket_lineage_hash", "lineage_hash", "submit_boundary_hash")
    )
    if client_bound and not requested_ticket_id:
        return _manual_capture_blocker_v1(
            code="REFRESH_REQUIRED",
            message="This ticket changed since the page loaded. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            exact_blocker="TICKET_ID_MISSING",
        )

    lineage = read_trade_ticket_lineage_v1(truth_root=root, day_utc=day_utc, ticket_id=current_ticket_id)
    if not lineage:
        lineage = build_trade_ticket_lineage_v1(truth_root=root, construction=current_construction, require_submit_boundary=True)
        write_trade_ticket_lineage_v1(truth_root=root, payload=lineage)
        current_ticket_id = str(lineage.get("ticket_id") or current_ticket_id)

    requested_lineage_hash = str(request_payload.get("ticket_lineage_hash") or request_payload.get("lineage_hash") or "").strip()
    current_lineage_hash = str(lineage.get("lineage_hash") or "")
    if client_bound and (not requested_lineage_hash or requested_lineage_hash != current_lineage_hash):
        return _manual_capture_blocker_v1(
            code="REFRESH_REQUIRED",
            message="This ticket changed since the page loaded. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            expected_hash=current_lineage_hash,
            received_hash=requested_lineage_hash,
            exact_blocker="STALE_TICKET_LINEAGE",
        )

    requested_runtime_hash = str(request_payload.get("runtime_evaluation_hash") or "").strip()
    current_runtime_hash = str(lineage.get("runtime_evaluation_hash") or "")
    if client_bound and (not requested_runtime_hash or requested_runtime_hash != current_runtime_hash):
        return _manual_capture_blocker_v1(
            code="STALE_RUNTIME_EVALUATION",
            message="This ticket uses a stale RuntimeEvaluation. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            expected_hash=current_runtime_hash,
            received_hash=requested_runtime_hash,
            exact_blocker="STALE_RUNTIME_EVALUATION",
        )

    canonical_runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    canonical_runtime_hash = str(canonical_runtime.get("deterministic_output_hash") or canonical_runtime.get("runtime_evaluation_hash") or "")
    if canonical_runtime_hash and current_runtime_hash and current_runtime_hash != canonical_runtime_hash:
        return _manual_capture_blocker_v1(
            code="STALE_RUNTIME_EVALUATION",
            message="This ticket was generated under a RuntimeEvaluation that is no longer current. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            expected_hash=canonical_runtime_hash,
            received_hash=current_runtime_hash,
            exact_blocker="STALE_RUNTIME_EVALUATION",
        )

    requested_submit_hash = str(request_payload.get("submit_boundary_hash") or "").strip()
    current_submit_hash = str(lineage.get("submit_boundary_hash") or "")
    if client_bound and (not requested_submit_hash or requested_submit_hash != current_submit_hash):
        return _manual_capture_blocker_v1(
            code="REFRESH_REQUIRED",
            message="The submit-boundary proof changed since the page loaded. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            expected_hash=current_submit_hash,
            received_hash=requested_submit_hash,
            exact_blocker="SUBMIT_BOUNDARY_HASH_MISMATCH",
        )

    requested_construction_id = str(request_payload.get("paper_trade_construction_id") or request_payload.get("construction_id") or "").strip()
    lineage_construction_id = str(lineage.get("paper_trade_construction_id") or "")
    if requested_construction_id and lineage_construction_id and requested_construction_id != lineage_construction_id:
        return _manual_capture_blocker_v1(
            code="CONSTRUCTION_CONTRACT_MISMATCH",
            message="This ticket changed since the page loaded. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            expected_hash=lineage_construction_id,
            received_hash=requested_construction_id,
            exact_blocker="paper_trade_construction_id does not match ticket lineage",
        )

    requested_contract_hash = str(request_payload.get("construction_contract_hash") or "").strip()
    lineage_contract_hash = str(lineage.get("construction_contract_hash") or "")
    if requested_contract_hash and lineage_contract_hash and requested_contract_hash != lineage_contract_hash:
        return _manual_capture_blocker_v1(
            code="CONSTRUCTION_CONTRACT_MISMATCH",
            message="The construction contract changed since the page loaded. Refresh to use the current governed ticket.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            expected_hash=lineage_contract_hash,
            received_hash=requested_contract_hash,
            exact_blocker="CONSTRUCTION_CONTRACT_HASH_MISMATCH",
        )

    status = str(lineage.get("lineage_status") or "")
    if status != "ACTIVE_CURRENT":
        code = status if status in LINEAGE_STATUSES else "NOT_ACTIVE_CURRENT"
        blocker = _manual_capture_blocker_v1(
            code=code,
            message=f"Manual capture save requires a capture-ready trade ticket; current lineage_status={status or 'MISSING'}.",
            ticket_id=current_ticket_id,
            lineage=lineage,
            exact_blocker=code,
            recommended_ui_action="Show the ticket read-only until the lineage becomes ACTIVE_CURRENT.",
        )
        blocker["blocker_codes"] = [code, *[str(item) for item in lineage.get("blocker_codes") or [] if str(item) != code]]
        return blocker

    evidence = lineage.get("evidence_status") if isinstance(lineage.get("evidence_status"), Mapping) else {}
    if str(evidence.get("submit_boundary") or "") != "VALIDATED":
        return _manual_capture_blocker_v1(code="SUBMIT_BOUNDARY_INVALID", message="Submit-boundary precheck is not validated for this ticket.", ticket_id=current_ticket_id, lineage=lineage, expected_hash="VALIDATED", received_hash=str(evidence.get("submit_boundary") or "MISSING"))
    if str(evidence.get("market_freshness") or "") != "VALID":
        return _manual_capture_blocker_v1(code="MARKET_FRESHNESS_MISSING", message="Market freshness evidence is missing or invalid.", ticket_id=current_ticket_id, lineage=lineage)
    if str(evidence.get("paper_intent") or "") != "VALID":
        return _manual_capture_blocker_v1(code="PAPER_INTENT_MISSING", message="Paper intent evidence is missing or invalid.", ticket_id=current_ticket_id, lineage=lineage)
    if str(evidence.get("conversion") or "") != "VALID":
        return _manual_capture_blocker_v1(code="CONVERSION_MISSING", message="Conversion evidence is missing or invalid.", ticket_id=current_ticket_id, lineage=lineage)

    return {"allowed": True, "blocker_code": "", "blocker_codes": [], "message": "ACTIVE_CURRENT lineage validated.", "ticket_id": current_ticket_id, "lineage": lineage, "manual_capture_only": True, "submit_boundary_status": "VALIDATED", "ib_api_handshake_required": False, "broker_submit_required": False}


def append_manual_action_event_v1(*, truth_root: Path | str, day_utc: str, accepted: bool, record_or_request: Mapping[str, Any], lineage: Mapping[str, Any], blocker_codes: list[str] | None = None, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    created = generated_at_utc or now_utc_v1()
    event_type = "ManualActionAccepted" if accepted else "ManualActionBlocked"
    record_hash = stable_hash_v1(dict(record_or_request))
    event = {
        "event_id": event_type + ":" + stable_hash_v1({"record_hash": record_hash, "lineage_hash": lineage.get("lineage_hash"), "created": created})[:32],
        "event_type": event_type,
        "run_id": f"manual-capture:{day_utc}",
        "parent_run_id": str(lineage.get("ticket_id") or ""),
        "day_utc": day_utc,
        "created_at_utc": created,
        "producer": "ops.aegis.operator_state.manual_capture_record_v1",
        "producer_version": "v1",
        "git_sha": "UNKNOWN",
        "schema_id": "manual_capture_record",
        "schema_version": "v1",
        "input_hashes": {
            "request_hash": record_hash,
            "lineage_hash": str(lineage.get("lineage_hash") or ""),
            "runtime_evaluation_hash": str(lineage.get("runtime_evaluation_hash") or ""),
            "blocker_codes": ",".join(blocker_codes or []),
        },
        "output_hashes": {"record_hash": record_hash} if accepted else {},
        "artifact_paths": [str(path) for path in lineage.get("source_artifact_paths") or []],
        "validation_status": "ACCEPTED" if accepted else "BLOCKED:" + ",".join(blocker_codes or []),
        "previous_event_hash": "",
        "event_hash": "",
    }
    return append_evidence_event_v1(truth_root=root, day_utc=day_utc, event=event)


def _emit_artifact_events(*, root: Path, day: str, path: Path, payload: dict[str, Any], producer: str) -> None:
    output_hash = sha256_file_v1(path)
    base = {
        "run_id": f"{producer}:{payload.get('schema_id')}:{day}",
        "parent_run_id": str(payload.get("ticket_id") or ""),
        "day_utc": day,
        "created_at_utc": str(payload.get("generated_at_utc") or now_utc_v1()),
        "producer": producer,
        "producer_version": "v1",
        "git_sha": "UNKNOWN",
        "schema_id": str(payload.get("schema_id") or ""),
        "schema_version": str(payload.get("schema_version") or "v1"),
        "input_hashes": {"payload_hash": stable_hash_v1({**payload, "evidence_hash": "", "lineage_hash": ""})},
        "output_hashes": {str(path): output_hash},
        "artifact_paths": [str(path)],
    }
    validation_status = str(payload.get("validation_status") or "VALID")
    for event_type, status in (("EvidenceProduced", "PRODUCED"), ("EvidenceValidated" if validation_status in {"VALID", "VALIDATED"} else "EvidenceRejected", validation_status)):
        event = {
            **base,
            "event_id": event_type + ":" + stable_hash_v1({**base, "status": status})[:32],
            "event_type": event_type,
            "validation_status": status,
            "previous_event_hash": "",
            "event_hash": "",
        }
        try:
            append_evidence_event_v1(truth_root=root, day_utc=day, event=event)
        except ValueError:
            pass


def _event_ids_for_schemas(*, root: Path, day: str, schemas: set[str]) -> list[str]:
    try:
        events = read_evidence_events_v1(truth_root=root, day_utc=day)
    except Exception:
        return []
    return [str(row.get("event_id") or "") for row in events if str(row.get("schema_id") or "") in schemas and row.get("event_id")]


def _side_from_direction(direction: str) -> str:
    text = direction.strip().upper()
    if text == "SHORT":
        return "SELL_SHORT"
    if text in {"SELL", "BUY", "COVER"}:
        return text
    return "BUY" if text in {"LONG", ""} else text
