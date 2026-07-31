from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_generated_hypothesis_candidate_to_paper_v1"
FILENAME = "generated_hypothesis_candidate_to_paper.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_CANDIDATE_TO_PAPER_PROOF_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_NAME = "Oil shock reversals across energy ETFs"
OIL_SIGNAL_ID_PREFIX = "c2_oil_shock_reversal_"

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
    "no_validation_mutation": True,
    "no_allocation_mutation": True,
    "safety_gates_changed": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "outcome_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_candidate_to_paper_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_generated_hypothesis_candidate_to_paper_v1(*, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    paths = _paths(root, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    row = _trace(payloads, paths, str(day_utc), computed_at_utc or _now())
    body: dict[str, Any] = {
        "schema_id": "aegis_generated_hypothesis_candidate_to_paper",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": str(day_utc),
        "target_day": str(day_utc),
        "computed_at_utc": row["computed_at_utc"],
        "oil_shock": row,
        "summary": {
            "candidate_to_paper_status": row["candidate_to_paper_status"],
            "candidate_id": row["candidate_id"],
            "candidate_contract_status": row["candidate_contract_status"],
            "entry_reference_price_status": row["entry_reference_price_status"],
            "paper_construction_status": row["paper_construction_status"],
            "auto_promotion_status": row["auto_promotion_status"],
            "paper_observation_created": row["paper_observation_created"],
            "outcome_row_created": row["outcome_row_created"],
            "validation_sample_eligible": row["validation_sample_eligible"],
            "current_stop_stage": row["current_stop_stage"],
            "current_stop_reason": row["current_stop_reason"],
            "david_action_required": row["david_action_required"],
            "paper_observation_flow_advanced": row["paper_observation_created"],
        },
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: file_hash_v1(path) for key, path in sorted(paths.items())},
        "safety_statement": "Generated hypothesis candidate-to-paper proof is read-only and creates no candidates, paper observations, outcomes, validation samples, trades, broker actions, allocations, or safety-gate changes.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    body["content_hash"] = stable_hash_v1(_without_time(body))
    return body


def write_generated_hypothesis_candidate_to_paper_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_candidate_to_paper_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_candidate_to_paper_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "signal_to_candidate": report_path_v1(root, "aegis_generated_hypothesis_signal_to_candidate_v1", day, "generated_hypothesis_signal_to_candidate.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "entry_reference_price_certification": report_path_v1(root, "aegis_entry_reference_price_certification_v1", day, "entry_reference_price_certification.v1.json"),
        "paper_trade_construction": report_path_v1(root, "paper_trade_construction_v1", day, "paper_trade_construction.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"),
        "paper_position_ledger": report_path_v1(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
    }


def _trace(payloads: Mapping[str, Any], paths: Mapping[str, Path], day: str, computed_at: str) -> dict[str, Any]:
    proof = _oil_row(payloads.get("signal_to_candidate"))
    contract = _oil_contract(payloads.get("candidate_contracts"), proof)
    rejection = _oil_rejection(payloads.get("candidate_contracts"), proof)
    candidate_id = _text(proof.get("candidate_contract_id") or contract.get("candidate_id") or rejection.get("candidate_id"))
    raw_signal_id = _text(proof.get("raw_signal_id") or contract.get("raw_signal_id") or rejection.get("raw_signal_id"))
    entry = _find_entry(payloads.get("entry_reference_price_certification"), raw_signal_id, contract)
    construction = _find_construction(payloads.get("paper_trade_construction"), candidate_id, raw_signal_id)
    lifecycle = _find_lifecycle(payloads.get("candidate_lifecycle"), candidate_id, raw_signal_id)
    position = _find_position(payloads.get("paper_position_ledger"), candidate_id, _text(lifecycle.get("paper_position_id")))
    outcome = _find_outcome(payloads.get("outcome_registry"), candidate_id, _text(lifecycle.get("outcome_id")))
    sample = _find_sample(payloads.get("validation_samples"), candidate_id, _text(outcome.get("outcome_id")))

    contract_status = _contract_status(contract, rejection)
    rejection_reasons = _reason_codes(rejection) if rejection else ([] if contract_status == "CANDIDATE_CONTRACT_VALID" else _reason_codes(contract))
    entry_status = _entry_status(entry, contract)
    paper_status = _paper_status(construction, lifecycle)
    auto_status = _auto_status(lifecycle, position)
    paper_position_id = _text(lifecycle.get("paper_position_id") or position.get("position_id"))
    paper_observation_created = bool(paper_position_id or auto_status == "AUTO_PROMOTED_TO_PAPER_TRACKING")
    outcome_created = _outcome_advancement_created(outcome, lifecycle)

    statuses = [contract_status, entry_status, paper_status, auto_status]
    current_stop_stage, current_stop_reason = _stop_reason(
        contract_status=contract_status,
        entry_status=entry_status,
        paper_status=paper_status,
        auto_status=auto_status,
        rejection_reasons=rejection_reasons,
        entry=entry,
        construction=construction,
        lifecycle=lifecycle,
        paper_observation_created=paper_observation_created,
        outcome_created=outcome_created,
    )
    candidate_to_paper_status = _overall_status(contract_status, entry_status, paper_status, auto_status, paper_observation_created)
    source_paths = {key: str(path) for key, path in sorted(paths.items())}
    source_hashes = {key: file_hash_v1(path) for key, path in sorted(paths.items())}

    return {
        "hypothesis_id": _text(proof.get("hypothesis_id") or contract.get("hypothesis_id") or rejection.get("hypothesis_id") or OIL_HYPOTHESIS_ID),
        "hypothesis_name": _text(proof.get("hypothesis_name") or OIL_NAME),
        "sleeve_id": _text(contract.get("sleeve_id") or rejection.get("sleeve_id") or lifecycle.get("sleeve_id") or construction.get("sleeve_id") or proof.get("sleeve_id")),
        "raw_signal_id": raw_signal_id,
        "candidate_id": candidate_id,
        "candidate_to_paper_status": candidate_to_paper_status,
        "candidate_contract_status": contract_status,
        "candidate_contract_rejection_reasons": rejection_reasons,
        "entry_reference_price_status": entry_status,
        "entry_reference_price": _text(contract.get("entry_reference_price") or entry.get("price") or construction.get("entry_reference_price") or lifecycle.get("entry_reference_price")),
        "paper_construction_status": paper_status,
        "paper_construction_missing_fields": [str(x) for x in _list(construction.get("missing_fields")) if _text(x)],
        "auto_promotion_status": auto_status,
        "paper_position_id": paper_position_id,
        "paper_observation_created": paper_observation_created,
        "outcome_row_created": outcome_created,
        "outcome_id": _text(outcome.get("outcome_id") or lifecycle.get("outcome_id")),
        "validation_sample_eligible": bool(outcome_created and not sample),
        "validation_sample_status": "VALIDATION_SAMPLE_PRESENT" if sample else ("VALIDATION_SAMPLE_PENDING" if outcome_created else "NOT_APPLICABLE"),
        "current_stop_stage": current_stop_stage,
        "current_stop_reason": current_stop_reason,
        "status_sequence": statuses,
        "reason_codes": sorted({code for code in _reason_codes(entry) + _reason_codes(construction) + _reason_codes(lifecycle) + rejection_reasons if code}),
        "david_action_required": False,
        "source_artifact_paths": source_paths,
        "source_artifact_hashes": source_hashes,
        "computed_at_utc": computed_at,
        **SAFETY,
    }


def _contract_status(contract: Mapping[str, Any], rejection: Mapping[str, Any]) -> str:
    status = _upper(contract.get("contract_validation_status"))
    if status == "VALID":
        return "CANDIDATE_CONTRACT_VALID"
    if status or rejection:
        return "CANDIDATE_CONTRACT_REJECTED"
    return "BLOCKED"


def _entry_status(entry: Mapping[str, Any], contract: Mapping[str, Any]) -> str:
    status = _upper(entry.get("certification_status") or contract.get("entry_reference_price_certification_status"))
    if status == "CERTIFIED":
        return "ENTRY_REFERENCE_CERTIFIED"
    if status:
        return "ENTRY_REFERENCE_FAILED"
    return "BLOCKED"


def _paper_status(construction: Mapping[str, Any], lifecycle: Mapping[str, Any]) -> str:
    status = _upper(construction.get("construction_status") or lifecycle.get("paper_construction_status") or construction.get("trade_construction_status"))
    if status in {"CONSTRUCTED", "READY", "PAPER_CONSTRUCTION_READY"}:
        return "PAPER_CONSTRUCTION_READY"
    if status:
        return "PAPER_CONSTRUCTION_FAILED"
    return "BLOCKED"


def _auto_status(lifecycle: Mapping[str, Any], position: Mapping[str, Any]) -> str:
    if position or _text(lifecycle.get("paper_position_id")):
        return "AUTO_PROMOTED_TO_PAPER_TRACKING"
    status = _upper(lifecycle.get("auto_promotion_status") or lifecycle.get("promotion_status"))
    if status == "AUTO_PROMOTED_TO_PAPER_TRACKING":
        return "AUTO_PROMOTED_TO_PAPER_TRACKING"
    if status:
        return "AUTO_PROMOTION_BLOCKED"
    return "BLOCKED"


def _overall_status(contract_status: str, entry_status: str, paper_status: str, auto_status: str, paper_observation_created: bool) -> str:
    if paper_observation_created:
        return "PAPER_OBSERVATION_CREATED"
    if contract_status == "CANDIDATE_CONTRACT_REJECTED":
        return "CANDIDATE_CONTRACT_REJECTED"
    if entry_status == "ENTRY_REFERENCE_FAILED":
        return "ENTRY_REFERENCE_FAILED"
    if paper_status == "PAPER_CONSTRUCTION_FAILED":
        return "PAPER_CONSTRUCTION_FAILED"
    if auto_status == "AUTO_PROMOTION_BLOCKED":
        return "AUTO_PROMOTION_BLOCKED"
    return "BLOCKED"


def _stop_reason(*, contract_status: str, entry_status: str, paper_status: str, auto_status: str, rejection_reasons: list[str], entry: Mapping[str, Any], construction: Mapping[str, Any], lifecycle: Mapping[str, Any], paper_observation_created: bool, outcome_created: bool) -> tuple[str, str]:
    if contract_status == "CANDIDATE_CONTRACT_REJECTED":
        return "candidate contract", ";".join(rejection_reasons) or "Candidate contract was rejected by the normal candidate-contract artifact."
    if entry_status == "ENTRY_REFERENCE_FAILED":
        return "entry reference price", ";".join(_reason_codes(entry)) or _text(entry.get("certification_status")) or "Entry reference price certification failed."
    if paper_status == "PAPER_CONSTRUCTION_FAILED":
        missing = [str(x) for x in _list(construction.get("missing_fields")) if _text(x)]
        if missing:
            return "paper construction", "PAPER_CONSTRUCTION_FAILED: missing " + ", ".join(missing)
        return "paper construction", ";".join(_reason_codes(construction)) or "Paper construction failed."
    if auto_status == "AUTO_PROMOTION_BLOCKED":
        return "auto-promotion", ";".join(_reason_codes(lifecycle)) or "Auto-promotion blocked by paper lifecycle."
    if paper_observation_created and outcome_created:
        return "validation sample flow", "Oil Shock paper observation created through normal Aegis gates; resolved outcome evidence exists and validation sample flow is future-scoped."
    if paper_observation_created:
        return "paper observation", "Oil Shock paper observation created through normal Aegis gates; Package 017 stops before outcome generation."
    return "candidate-to-paper", "Oil Shock candidate stopped before paper observation flow."


def _oil_row(payload: Any) -> dict[str, Any]:
    payload = _dict(payload)
    row = payload.get("oil_shock")
    return dict(row) if isinstance(row, Mapping) else {}


def _oil_contract(payload: Any, proof: Mapping[str, Any]) -> dict[str, Any]:
    cid = _text(proof.get("candidate_contract_id"))
    for row in _list(_dict(payload).get("candidate_contracts")):
        if _matches_oil(row, cid):
            return dict(row)
    return {}


def _oil_rejection(payload: Any, proof: Mapping[str, Any]) -> dict[str, Any]:
    cid = _text(proof.get("candidate_contract_id"))
    for key in ("rejected_raw_signals", "candidates_rejected"):
        for row in _list(_dict(payload).get(key)):
            if _matches_oil(row, cid):
                return dict(row)
    return {}


def _find_entry(payload: Any, raw_signal_id: str, contract: Mapping[str, Any]) -> dict[str, Any]:
    cert_id = _text(contract.get("entry_reference_price_certification_id"))
    for row in _list(_dict(payload).get("rows")):
        if (raw_signal_id and _text(row.get("raw_signal_id") or row.get("intent_id")) == raw_signal_id) or (cert_id and _text(row.get("certification_id")) == cert_id):
            return dict(row)
    return {}


def _find_construction(payload: Any, candidate_id: str, raw_signal_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    candidates = _list(payload.get("constructed_paper_trades")) + _list(payload.get("skipped_candidates"))
    if payload.get("candidate_id"):
        candidates.append(payload)
    for row in candidates:
        if (candidate_id and _text(row.get("candidate_id")) == candidate_id) or (raw_signal_id and _text(row.get("raw_signal_id")) == raw_signal_id):
            return dict(row)
    return {}


def _find_lifecycle(payload: Any, candidate_id: str, raw_signal_id: str) -> dict[str, Any]:
    for row in _list(_dict(payload).get("rows")):
        if (candidate_id and _text(row.get("candidate_id")) == candidate_id) or (raw_signal_id and _text(row.get("raw_signal_id")) == raw_signal_id):
            return dict(row)
    return {}


def _find_position(payload: Any, candidate_id: str, paper_position_id: str) -> dict[str, Any]:
    payload = _dict(payload)
    for key in ("positions", "open_positions", "closed_positions", "historical_positions"):
        for row in _list(payload.get(key)):
            if (candidate_id and _text(row.get("candidate_id") or _dict(row.get("candidate_lineage")).get("candidate_id")) == candidate_id) or (paper_position_id and _text(row.get("position_id")) == paper_position_id):
                return dict(row)
    return {}



def _outcome_advancement_created(outcome: Mapping[str, Any], lifecycle: Mapping[str, Any]) -> bool:
    outcome_state = _upper(outcome.get("outcome_state"))
    if outcome_state and outcome_state not in {"OPEN", "UNKNOWN_BLOCKED"}:
        return True
    if _upper(outcome.get("auto_closure_state")):
        return True
    lifecycle_outcome = _text(lifecycle.get("outcome_id"))
    lifecycle_status = _upper(lifecycle.get("outcome_status") or lifecycle.get("validation_sample_status"))
    return bool(lifecycle_outcome and lifecycle_status and lifecycle_status not in {"OPEN", "NOT_READY", "NOT_APPLICABLE"})

def _find_outcome(payload: Any, candidate_id: str, outcome_id: str) -> dict[str, Any]:
    for row in _list(_dict(payload).get("outcomes")):
        if (outcome_id and _text(row.get("outcome_id")) == outcome_id) or (candidate_id and _text(row.get("candidate_id")) == candidate_id):
            return dict(row)
    return {}


def _find_sample(payload: Any, candidate_id: str, outcome_id: str) -> dict[str, Any]:
    for row in _list(_dict(payload).get("samples")):
        if (outcome_id and _text(row.get("outcome_id")) == outcome_id) or (candidate_id and _text(row.get("candidate_id")) == candidate_id):
            return dict(row)
    return {}


def _matches_oil(row: Any, candidate_id: str) -> bool:
    if not isinstance(row, Mapping):
        return False
    hay = " ".join(_text(row.get(key)) for key in ("hypothesis_id", "hypothesis_name", "display_name", "sleeve_id", "raw_signal_id", "intent_id", "candidate_id", "symbol")).lower()
    return bool((candidate_id and _text(row.get("candidate_id")) == candidate_id) or OIL_HYPOTHESIS_ID in hay or "oil_shock" in hay or "oil shock" in hay or _text(row.get("raw_signal_id")).lower().startswith(OIL_SIGNAL_ID_PREFIX))


def _reason_codes(row: Mapping[str, Any]) -> list[str]:
    codes: list[str] = []
    for key in ("rejection_reason", "reason_code", "detail_reason_codes", "rejection_reason_codes", "certification_reason_codes", "entry_reference_price_certification_reason_codes", "blocker_reason_codes", "auto_promotion_reason_codes", "blocker_codes"):
        value = row.get(key)
        if isinstance(value, list):
            codes.extend(str(item) for item in value if _text(item))
        elif _text(value):
            codes.append(_text(value))
    for field in _list(row.get("missing_fields")):
        if _text(field):
            codes.append("MISSING_" + _text(field).upper())
    return [code for code in codes if code]


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _without_time(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    out.pop("computed_at_utc", None)
    out.pop("content_hash", None)
    oil = dict(out.get("oil_shock") or {})
    oil.pop("computed_at_utc", None)
    out["oil_shock"] = oil
    return out
