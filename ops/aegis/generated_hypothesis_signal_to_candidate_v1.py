from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_signal_to_candidate_v1"
FILENAME = "generated_hypothesis_signal_to_candidate.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_SIGNAL_TO_CANDIDATE_PROOF_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_NAME = "Oil shock reversals across energy ETFs"
OIL_SIGNAL_ID_PREFIX = "c2_oil_shock_reversal_"

REQUIRED_SIGNAL_FIELDS = [
    "raw_signal_id",
    "direction",
    "instrument_type",
    "symbol",
    "governance_status",
    "risk_per_trade",
]

SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_forced_candidates": True,
    "no_candidate_mutation": True,
    "no_paper_observation_mutation": True,
    "no_outcome_mutation": True,
    "no_allocation_mutation": True,
    "safety_gates_changed": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_signal_to_candidate_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_signal_to_candidate_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    paths = _paths(root, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items() if path.suffix == ".json"}
    row = _classify(payloads, paths)
    body: dict[str, Any] = {
        "schema_id": "aegis_generated_hypothesis_signal_to_candidate",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": str(day_utc),
        "target_day": str(day_utc),
        "computed_at_utc": _now(),
        "oil_shock": row,
        "summary": {
            "signal_to_candidate_status": row["signal_to_candidate_status"],
            "raw_signal_id": row["raw_signal_id"],
            "candidate_contract_status": row["candidate_contract_status"],
            "candidate_contract_count": row["candidate_contract_count"],
            "rejection_reason_codes": row["rejection_reason_codes"],
            "missing_fields": row["missing_fields"],
            "candidate_flow_advanced": row["candidate_flow_advanced"],
            "paper_observation_flow_advanced": row["paper_observation_flow_advanced"],
            "remaining_blocker": row["remaining_blocker"],
            "david_action_required": row["david_action_required"],
        },
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "safety_statement": "Signal-to-candidate proof is read-only and creates no signals, candidate contracts, paper observations, outcomes, allocations, trades, broker actions, or safety-gate changes.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["content_hash"] = stable_hash_v1(_without_time(body))
    return body


def write_generated_hypothesis_signal_to_candidate_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_signal_to_candidate_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_signal_to_candidate_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "candidate_producer": report_path_v1(root, "aegis_oil_shock_candidate_producer_v1", day, "oil_shock_candidate_producer.v1.json"),
        "signal_evidence_graph": report_path_v1(root, "aegis_signal_evidence_graph_v1", day, "signal_evidence_graph.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "entry_reference_price_certification": report_path_v1(root, "aegis_entry_reference_price_certification_v1", day, "entry_reference_price_certification.v1.json"),
        "candidate_construction": report_path_v1(root, "aegis_oil_shock_candidate_construction_v1", day, "oil_shock_candidate_construction.v1.json"),
        "governance_bridge": report_path_v1(root, "aegis_generated_hypothesis_governance_bridge_v1", day, "generated_hypothesis_governance_bridge.v1.json"),
        "paper_setup_bridge": report_path_v1(root, "aegis_generated_hypothesis_paper_setup_bridge_v1", day, "generated_hypothesis_paper_setup_bridge.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"),
    }


def _classify(payloads: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    producer = _dict(payloads.get("candidate_producer"))
    construction = _oil_row(payloads.get("candidate_construction"))
    governance = _oil_row(payloads.get("governance_bridge"))
    paper_setup = _oil_row(payloads.get("paper_setup_bridge"))
    signal = _oil_signal(payloads.get("signal_evidence_graph"))
    contract = _oil_contract(payloads.get("candidate_contracts"))
    rejection = _oil_rejection(payloads.get("candidate_contracts"))
    lifecycle_rows = [row for row in _list(_dict(payloads.get("candidate_lifecycle")).get("rows")) if _is_oil(row)]

    raw_signal_id = _text(signal.get("raw_signal_id")) or _producer_raw_signal_id(producer)
    producer_status = _text(producer.get("producer_status") or producer.get("last_evaluation_status"))
    signal_missing = [field for field in REQUIRED_SIGNAL_FIELDS if not _text(signal.get(field))]
    price_edge = _entry_price_edge(signal)
    entry_status = _text(price_edge.get("entry_reference_price_certification_status") or contract.get("entry_reference_price_certification_status"))
    rejection_codes: list[str] = []
    missing_fields: list[str] = []
    contract_status = "ABSENT"
    status = "RAW_SIGNAL_MISSING"
    blocker = "RAW_SIGNAL_MISSING"
    next_step = "run Oil Shock producer before signal-to-candidate proof"

    if producer_status == "NO_MARKET_SETUP" and not raw_signal_id:
        status = "NO_MARKET_SETUP"
        blocker = "NO_MARKET_SETUP"
        next_step = "wait for qualifying Oil Shock market setup"
    elif not raw_signal_id:
        status = "RAW_SIGNAL_MISSING"
        blocker = "RAW_SIGNAL_MISSING"
    elif not signal:
        status = "SIGNAL_EVIDENCE_GRAPH_MISSING"
        blocker = "SIGNAL_EVIDENCE_GRAPH_MISSING"
        rejection_codes = ["RAW_SIGNAL_NOT_PRESENT_IN_SIGNAL_EVIDENCE_GRAPH"]
        next_step = "rerun signal evidence graph after Oil Shock producer"
    elif signal_missing:
        status = "SIGNAL_SCHEMA_INCOMPLETE"
        blocker = "SIGNAL_SCHEMA_INCOMPLETE"
        missing_fields = [f"signal.{field}" for field in signal_missing]
        next_step = "repair generated-hypothesis signal schema before candidate contracts"
    elif _upper(construction.get("candidate_construction_policy_id") or governance.get("candidate_construction_policy_id")) == "":
        status = "CANDIDATE_CONSTRUCTION_POLICY_MISSING"
        blocker = "CANDIDATE_CONSTRUCTION_POLICY_MISSING"
        missing_fields = ["candidate_construction_policy_id"]
        next_step = "complete governed candidate construction policy mapping"
    elif _upper(governance.get("governance_bridge_status")) not in {"", "GOVERNANCE_BRIDGE_READY"}:
        status = "GOVERNANCE_POLICY_MISSING"
        blocker = "GOVERNANCE_POLICY_MISSING"
        missing_fields = [str(item) for item in _list(governance.get("missing_fields")) if _text(item)]
        next_step = "repair generated-hypothesis governance bridge"
    elif entry_status and entry_status != "CERTIFIED":
        status = "ENTRY_PRICE_CERTIFICATION_FAILED"
        blocker = "ENTRY_PRICE_CERTIFICATION_FAILED"
        rejection_codes = [str(item) for item in _list(price_edge.get("entry_reference_price_certification_reason_codes")) if _text(item)] or [entry_status]
        next_step = "repair entry reference price certification before candidate contracts"
    elif contract:
        contract_status = _text(contract.get("contract_validation_status") or "VALID")
        if contract_status == "VALID":
            status = "CANDIDATE_CONTRACT_CREATED"
            blocker = "NONE"
            next_step = "route candidate contract through entry certification and paper lifecycle"
        else:
            status = "CANDIDATE_CONTRACT_REJECTED"
            blocker = "CANDIDATE_CONTRACT_REJECTED"
            rejection_codes = [_text(contract.get("rejection_reason"))] if _text(contract.get("rejection_reason")) else []
            missing_fields = [str(item) for item in _list(contract.get("missing_contract_fields")) if _text(item)]
            next_step = _text(contract.get("next_repair_action")) or "preserve candidate-contract rejection and repair upstream evidence"
    elif rejection:
        contract_status = "REJECTED"
        status = "CANDIDATE_CONTRACT_REJECTED"
        blocker = "CANDIDATE_CONTRACT_REJECTED"
        rejection_codes = [_text(rejection.get("rejection_reason"))] if _text(rejection.get("rejection_reason")) else []
        missing_fields = [str(item) for item in _list(rejection.get("missing_contract_fields")) if _text(item)]
        next_step = _text(rejection.get("next_repair_action")) or "preserve candidate-contract rejection and repair upstream evidence"
    else:
        status = "SIGNAL_EVIDENCE_GRAPH_MISSING"
        blocker = "STALE_CANDIDATE_CONTRACTS"
        rejection_codes = ["CANDIDATE_CONTRACT_ABSENT_AFTER_VALID_SIGNAL", "RERUN_CANDIDATE_CONTRACTS_AFTER_GENERATED_PRODUCER"]
        next_step = "rerun candidate contracts after generated-hypothesis producers"

    raw_count = int(producer.get("raw_signal_count") or (1 if raw_signal_id else 0) or 0)
    candidate_contract_count = 1 if contract else 0
    return {
        "hypothesis_id": _text(governance.get("hypothesis_id") or paper_setup.get("hypothesis_id") or construction.get("hypothesis_id") or OIL_HYPOTHESIS_ID),
        "hypothesis_name": _text(governance.get("hypothesis_name") or paper_setup.get("hypothesis_name") or construction.get("hypothesis_name") or OIL_NAME),
        "raw_signal_id": raw_signal_id,
        "raw_signal_present": bool(raw_signal_id),
        "raw_signal_source_artifact": _text(signal.get("source_artifact_path") or producer.get("source_artifact_paths", {}).get("candidate_producer") or paths["candidate_producer"]),
        "signal_schema_status": "COMPLETE" if signal and not signal_missing else ("MISSING" if not signal else "INCOMPLETE"),
        "signal_direction": _text(signal.get("direction")),
        "signal_instrument_type": _text(signal.get("instrument_type")),
        "signal_symbol_or_basket": _text(signal.get("symbol")),
        "governance_status": _text(signal.get("governance_status") or governance.get("governance_bridge_status")),
        "candidate_construction_policy_id": _text(construction.get("candidate_construction_policy_id") or governance.get("candidate_construction_policy_id")),
        "risk_policy_id": _text(construction.get("risk_policy_id") or governance.get("risk_policy_id") or paper_setup.get("risk_policy_id")),
        "exit_policy_id": _text(construction.get("exit_policy_id") or governance.get("exit_policy_id") or paper_setup.get("exit_policy_id")),
        "entry_reference_price_status": _text(contract.get("entry_reference_price_status") or ("VALID" if _text(price_edge.get("value")) else "")),
        "entry_reference_price_certification_status": entry_status,
        "candidate_contract_status": contract_status,
        "candidate_contract_id": _text(contract.get("candidate_id")),
        "signal_to_candidate_status": status,
        "rejection_reason_codes": [code for code in rejection_codes if code],
        "missing_fields": missing_fields,
        "raw_signal_count": raw_count,
        "candidate_contract_count": candidate_contract_count,
        "candidate_flow_advanced": status == "CANDIDATE_CONTRACT_CREATED",
        "paper_observation_flow_advanced": bool(lifecycle_rows),
        "paper_observation_count": len(lifecycle_rows),
        "remaining_blocker": blocker,
        "next_expected_step": next_step,
        "david_action_required": False,
        "ui_message": _ui_message(status, rejection_codes),
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        **SAFETY,
    }


def _oil_row(payload: Any) -> dict[str, Any]:
    payload = _dict(payload)
    row = payload.get("oil_shock")
    return dict(row) if isinstance(row, Mapping) else payload


def _oil_signal(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("signals")):
        if _is_oil(row):
            return dict(row)
    return {}


def _oil_contract(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("candidate_contracts")):
        if _is_oil(row):
            return dict(row)
    return {}


def _oil_rejection(payload: Any) -> dict[str, Any]:
    for row in _list(_dict(payload).get("rejected_raw_signals")):
        if _is_oil(row):
            return dict(row)
    return {}


def _is_oil(row: Any) -> bool:
    if not isinstance(row, Mapping):
        return False
    hay = " ".join(_text(row.get(key)) for key in ("hypothesis_id", "hypothesis_name", "display_name", "sleeve_id", "raw_signal_id", "intent_id", "candidate_id", "symbol")).lower()
    return OIL_HYPOTHESIS_ID in hay or "oil_shock" in hay or "oil shock" in hay or _text(row.get("raw_signal_id")).lower().startswith(OIL_SIGNAL_ID_PREFIX)


def _producer_raw_signal_id(producer: Mapping[str, Any]) -> str:
    for key in ("output_intents", "candidate_signals", "raw_signals"):
        for row in _list(producer.get(key)):
            if isinstance(row, Mapping) and _is_oil(row):
                return _text(row.get("raw_signal_id") or row.get("intent_id"))
    sleeve = _dict(producer.get("sleeve_evaluation"))
    for row in _list(sleeve.get("output_intents")):
        if isinstance(row, Mapping) and _is_oil(row):
            return _text(row.get("raw_signal_id") or row.get("intent_id"))
    batch = _dict(sleeve.get("exposure_intent_batch"))
    for row in _list(batch.get("output_intents")):
        if isinstance(row, Mapping) and _is_oil(row):
            return _text(row.get("raw_signal_id") or row.get("intent_id"))
    return ""


def _entry_price_edge(signal: Mapping[str, Any]) -> dict[str, Any]:
    for row in _list(signal.get("required_evidence")):
        if isinstance(row, Mapping) and _text(row.get("purpose")) == "ENTRY_REFERENCE_PRICE":
            return dict(row)
    return {}


def _ui_message(status: str, reasons: list[str]) -> str:
    if status == "CANDIDATE_CONTRACT_CREATED":
        return "Oil Shock candidate contract created through normal Aegis gates."
    if status == "CANDIDATE_CONTRACT_REJECTED":
        reason = ", ".join(code for code in reasons if code) or "no reason reported"
        return f"Oil Shock raw signal was rejected by candidate contracts: {reason}."
    if status == "NO_MARKET_SETUP":
        return "Oil Shock producer ran; no qualifying candidate setup."
    return "Oil Shock signal-to-candidate proof requires Aegis system repair. No David action required."


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _without_time(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _without_time(item) for key, item in value.items() if key not in {"computed_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_time(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
