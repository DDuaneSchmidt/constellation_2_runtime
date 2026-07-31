from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_generated_hypothesis_paper_setup_bridge_v1"
FILENAME = "generated_hypothesis_paper_setup_bridge.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_PAPER_SETUP_BRIDGE_V1"
OIL_SHOCK_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_SHOCK_NAME = "Oil shock reversals across energy ETFs"
OIL_SHOCK_SLEEVE_ID = "C2_OIL_SHOCK_REVERSAL_V1"
DEFAULT_REQUIRED_SYMBOLS = ["DBC", "SPY", "USO", "XLE"]
SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_forced_candidate_creation": True,
    "no_candidate_contract_bypass": True,
    "no_entry_price_certification_bypass": True,
    "no_paper_lifecycle_bypass": True,
    "no_outcome_validation_bypass": True,
    "raw_signal_created_by_this_artifact": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "outcome_created_by_this_artifact": False,
    "trade_created_by_this_artifact": False,
    "allocation_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "safety_gates_changed": False,
}


def generated_hypothesis_paper_setup_bridge_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, str(day_utc), FILENAME)


def build_generated_hypothesis_paper_setup_bridge_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    proposal_state = _find_oil(payloads.get("proposal_promotion", {}).get("proposal_states"))
    evidence = _find_oil(payloads.get("evidence_packets", {}).get("evidence_packets"))
    shadow = _find_oil(payloads.get("shadow_trials", {}).get("shadow_trials"))
    promotion = _find_oil(payloads.get("promotion_packets", {}).get("promotion_packets"))
    queue_item = _find_oil(payloads.get("approval_queue", {}).get("approval_queue"))
    blueprint = _find_oil(payloads.get("paper_blueprint", {}).get("paper_sleeve_blueprints"))
    readiness = _find_oil(payloads.get("paper_readiness", {}).get("paper_readiness_certifications"))
    setup = _find_oil(payloads.get("paper_setup", {}).get("paper_tracking_setups"))
    governance = _find_oil_governance(payloads.get("governance_bridge", {}))

    row = _bridge_row(
        day_utc=day,
        proposal_state=proposal_state,
        evidence=evidence,
        shadow=shadow,
        promotion=promotion,
        queue_item=queue_item,
        blueprint=blueprint,
        readiness=readiness,
        setup=setup,
        governance=governance,
        paths=paths,
    )
    rows = [row]
    summary = {
        "bridge_count": len(rows),
        "ready_count": sum(1 for item in rows if item.get("bridge_status") == "PAPER_SETUP_BRIDGE_READY"),
        "blocked_count": sum(1 for item in rows if item.get("bridge_status") != "PAPER_SETUP_BRIDGE_READY"),
        "candidate_construction_eligible_count": sum(1 for item in rows if item.get("candidate_construction_eligible") is True),
        "oil_shock_bridge_status": row.get("bridge_status"),
        "oil_shock_candidate_construction_eligible": row.get("candidate_construction_eligible"),
        "oil_shock_missing_fields": row.get("missing_fields") or [],
    }
    artifact = {
        "schema_id": "aegis_generated_hypothesis_paper_setup_bridge",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": day,
        "policy_version": POLICY_VERSION,
        "bridge_status": row.get("bridge_status"),
        "hypotheses": rows,
        "oil_shock": row,
        **row,
        "summary": summary,
        **SAFETY,
        "safety": dict(SAFETY),
        "computed_at_utc": _now(),
    }
    artifact["deterministic_rerun_id"] = stable_hash_v1({"day_utc": day, "policy_version": POLICY_VERSION, "source_hashes": row["source_artifact_hashes"], "status": row.get("bridge_status")})
    artifact["content_hash"] = stable_hash_v1(_without_generated(artifact))
    return artifact


def write_generated_hypothesis_paper_setup_bridge_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_paper_setup_bridge_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_paper_setup_bridge_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "proposal_promotion": report_path_v1(root, "aegis_hypothesis_proposal_promotion_v1", day, "promotion_pipeline.v1.json"),
        "evidence_packets": report_path_v1(root, "aegis_hypothesis_evidence_packet_v1", day, "evidence_packets.v1.json"),
        "shadow_trials": report_path_v1(root, "aegis_hypothesis_shadow_trial_v1", day, "shadow_trials.v1.json"),
        "promotion_packets": report_path_v1(root, "aegis_hypothesis_promotion_packet_v1", day, "promotion_packets.v1.json"),
        "approval_queue": report_path_v1(root, "aegis_paper_promotion_approval_queue_v1", day, "approval_queue.v1.json"),
        "paper_blueprint": report_path_v1(root, "aegis_paper_sleeve_blueprint_v1", day, "paper_sleeve_blueprint.v1.json"),
        "paper_readiness": report_path_v1(root, "aegis_paper_readiness_certification_v1", day, "paper_readiness_certification.v1.json"),
        "paper_setup": report_path_v1(root, "aegis_approved_hypothesis_paper_tracking_setup_v1", day, "approved_hypothesis_paper_tracking_setup.v1.json"),
        "governance_bridge": report_path_v1(root, "aegis_generated_hypothesis_governance_bridge_v1", day, "generated_hypothesis_governance_bridge.v1.json"),
    }


def _bridge_row(*, day_utc: str, proposal_state: Mapping[str, Any], evidence: Mapping[str, Any], shadow: Mapping[str, Any], promotion: Mapping[str, Any], queue_item: Mapping[str, Any], blueprint: Mapping[str, Any], readiness: Mapping[str, Any], setup: Mapping[str, Any], governance: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    governance_ready = governance.get("governance_bridge_status") == "GOVERNANCE_BRIDGE_READY"
    approval_event = queue_item.get("latest_approval_event") if isinstance(queue_item.get("latest_approval_event"), Mapping) else {}
    approval_event_hash = text_v1(governance.get("approval_event_hash") if governance_ready else "") or text_v1(approval_event.get("event_hash") or setup.get("approval_event_hash") or blueprint.get("approval_event_hash"))
    risk_policy = blueprint.get("risk_policy") if isinstance(blueprint.get("risk_policy"), Mapping) else {}
    candidate_policy = blueprint.get("candidate_construction_policy") if isinstance(blueprint.get("candidate_construction_policy"), Mapping) else {}
    validation_plan = blueprint.get("validation_plan") if isinstance(blueprint.get("validation_plan"), Mapping) else {}
    universe = _symbols(evidence.get("instrument_universe") or blueprint.get("instrument_universe") or DEFAULT_REQUIRED_SYMBOLS)
    generated_sleeve_id = text_v1(governance.get("generated_sleeve_id")) if governance_ready else OIL_SHOCK_SLEEVE_ID
    sleeve_id = text_v1(governance.get("sleeve_id")) if governance_ready else (generated_sleeve_id if blueprint or setup else "")
    risk_policy_id = text_v1(governance.get("risk_policy_id")) if governance_ready else text_v1(risk_policy.get("policy_id"))
    if not risk_policy_id and promotion.get("risk_policy_status") == "RESEARCH_ONLY_PASS" and blueprint:
        risk_policy_id = "aegis_research_only_risk_policy_v1"
    exit_policy_id = text_v1(governance.get("exit_policy_id")) if governance_ready else text_v1(blueprint.get("exit_policy_id") or validation_plan.get("exit_policy_id"))
    if not exit_policy_id and (text_v1(blueprint.get("exit_logic")) or text_v1(evidence.get("exit_logic"))) and promotion.get("exit_policy_status") == "RESEARCH_ONLY_DEFINED" and blueprint:
        exit_policy_id = "aegis_research_only_exit_policy_v1"
    candidate_policy_id = text_v1(governance.get("candidate_construction_policy_id")) if governance_ready else text_v1(candidate_policy.get("policy_id"))
    paper_blueprint_status = "PRESENT" if blueprint else ("GOVERNANCE_READY" if governance_ready else "MISSING")
    readiness_status = text_v1(readiness.get("certification_status")) if readiness else ("PAPER_READINESS_CERTIFIED" if governance_ready else "MISSING")
    tracking_status = text_v1(setup.get("paper_setup_status")) if setup else ("PAPER_TRACKING_READY" if governance_ready else "MISSING")
    approval_present = (queue_item.get("approval_state") == "PAPER_PROMOTION_APPROVED" and bool(approval_event_hash)) or (governance_ready and bool(approval_event_hash))
    fields = {
        "approval_event": approval_present,
        "approval_event_hash": bool(approval_event_hash),
        "generated_sleeve_id": bool(generated_sleeve_id),
        "sleeve_id": bool(sleeve_id),
        "risk_policy_id": bool(risk_policy_id),
        "exit_policy_id": bool(exit_policy_id),
        "paper_sleeve_blueprint": bool(blueprint) or governance_ready,
        "paper_readiness_certification": readiness_status == "PAPER_READINESS_CERTIFIED",
        "paper_tracking_setup": bool(setup) or governance_ready,
        "paper_setup_status": tracking_status == "PAPER_TRACKING_READY",
    }
    missing_fields = [key for key, ok in fields.items() if not ok]
    missing_policy_fields = [key for key in ("risk_policy_id", "exit_policy_id") if key in missing_fields]
    reason_codes: list[str] = []
    if not evidence:
        reason_codes.append("EVIDENCE_PACKET_MISSING")
    if not shadow:
        reason_codes.append("SHADOW_TRIAL_MISSING")
    if not promotion:
        reason_codes.append("PROMOTION_PACKET_MISSING")
    if not approval_present:
        reason_codes.append("APPROVAL_EVENT_MISSING")
    if missing_policy_fields:
        reason_codes.append("POLICY_MAPPING_MISSING")
    if readiness and readiness_status != "PAPER_READINESS_CERTIFIED":
        reason_codes.append("PAPER_READINESS_FAILED")
    reason_codes.extend(f"MISSING_{field.upper()}" for field in missing_fields)
    bridge_ready = not missing_fields and approval_present and readiness_status == "PAPER_READINESS_CERTIFIED" and tracking_status == "PAPER_TRACKING_READY" and (setup.get("candidate_generation_eligible") is True or governance_ready)
    if bridge_ready:
        bridge_status = "PAPER_SETUP_BRIDGE_READY"
        reason_codes = ["PAPER_SETUP_BRIDGE_READY"]
        next_step = "Oil Shock candidate construction can evaluate future market setups."
        ui_message = "Oil Shock paper setup bridge is ready. Candidate construction can evaluate future setups."
    else:
        bridge_status = "BLOCKED"
        next_step = "produce governed approval event and certified paper setup bridge before candidate construction eligibility"
        ui_message = "Oil Shock paper setup bridge blocked: " + ", ".join(missing_fields) + "."
    return {
        "hypothesis_id": OIL_SHOCK_HYPOTHESIS_ID,
        "hypothesis_name": OIL_SHOCK_NAME,
        "approval_state": text_v1(queue_item.get("approval_state") or proposal_state.get("state") or promotion.get("promotion_decision")),
        "approval_event_present": bool(approval_present),
        "approval_event_hash": approval_event_hash,
        "generated_sleeve_id": generated_sleeve_id,
        "sleeve_id": sleeve_id,
        "instrument_universe": universe,
        "risk_policy_id": risk_policy_id,
        "exit_policy_id": exit_policy_id,
        "candidate_construction_policy_id": candidate_policy_id,
        "paper_sleeve_blueprint_status": paper_blueprint_status,
        "paper_readiness_certification_status": readiness_status,
        "paper_tracking_setup_status": tracking_status,
        "paper_setup_status": tracking_status if tracking_status != "MISSING" else "",
        "governance_bridge_status": text_v1(governance.get("governance_bridge_status")),
        "governance_bridge_id": text_v1(governance.get("artifact_id") or "aegis_generated_hypothesis_governance_bridge_v1") if governance else "",
        "paper_sleeve_blueprint_id": text_v1(governance.get("paper_sleeve_blueprint_id")) if governance_ready else "",
        "paper_readiness_certification_id": text_v1(governance.get("paper_readiness_certification_id")) if governance_ready else "",
        "paper_tracking_setup_id": text_v1(governance.get("paper_tracking_setup_id")) if governance_ready else "",
        "bridge_status": bridge_status,
        "candidate_construction_eligible": bool(bridge_ready),
        "missing_fields": missing_fields,
        "missing_policy_fields": missing_policy_fields,
        "reason_codes": reason_codes,
        "david_action_required": False,
        "next_expected_step": next_step,
        "ui_message": ui_message,
        "raw_signal_count": 0,
        "candidate_count": 0,
        "paper_observation_count": 0,
        "outcome_count": 0,
        "trade_count": 0,
        "allocation_count": 0,
        "normal_pipeline_required": True,
        "candidate_contracts_authoritative": True,
        "paper_lifecycle_authoritative": True,
        "source_artifact_paths": [str(path) for path in paths.values()],
        "source_artifact_hashes": {str(path): file_hash_v1(path) for path in paths.values()},
        "computed_at_utc": _now(),
    }


def _find_oil_governance(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    row = payload.get("oil_shock") if isinstance(payload.get("oil_shock"), Mapping) else {}
    if row:
        return dict(row)
    return _find_oil(payload.get("hypotheses"))


def _find_oil(rows: Any) -> dict[str, Any]:
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        text = " ".join([text_v1(row.get("hypothesis_id")), text_v1(row.get("hypothesis_name")), text_v1(row.get("display_name")), text_v1(row.get("hypothesis_statement"))]).lower()
        if OIL_SHOCK_HYPOTHESIS_ID.lower() in text or "oil shock reversals" in text:
            return dict(row)
    return {}


def _symbols(values: Any) -> list[str]:
    return sorted({text_v1(item).upper() for item in values or [] if text_v1(item)})


def _without_generated(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _without_generated(item) for key, item in value.items() if key not in {"computed_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
