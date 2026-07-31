from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_oil_shock_candidate_construction_v1"
FILENAME = "oil_shock_candidate_construction.v1.json"
POLICY_VERSION = "AEGIS_OIL_SHOCK_CANDIDATE_CONSTRUCTION_V1"
HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
HYPOTHESIS_NAME = "Oil shock reversals across energy ETFs"
ENGINE_ID = "C2_OIL_SHOCK_REVERSAL_V1"
THESIS_ID = "THESIS_EVENT_DISLOCATION_V1"
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
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "raw_signal_created_by_this_artifact": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "safety_gates_changed": False,
}


def oil_shock_candidate_construction_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, str(day_utc), FILENAME)


def build_oil_shock_candidate_construction_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve()
    paths = _paths(root, repo, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items() if path.suffix == ".json"}
    proposal_path = _proposal_path(payloads)
    proposal = read_json_v1(proposal_path) if proposal_path.exists() else {}
    evidence = _find_oil(payloads.get("evidence_packets", {}).get("evidence_packets"))
    promotion = _find_oil(payloads.get("promotion_packets", {}).get("promotion_packets"))
    shadow = _find_oil(payloads.get("shadow_trials", {}).get("shadow_trials"))
    queue_item = _find_oil(payloads.get("approval_queue", {}).get("approval_queue"))
    blueprint = _find_oil(payloads.get("paper_blueprint", {}).get("paper_sleeve_blueprints"))
    readiness = _find_oil(payloads.get("paper_readiness", {}).get("paper_readiness_certifications"))
    setup = _find_oil(payloads.get("paper_setup", {}).get("paper_tracking_setups"))
    bridge = _oil_bridge(payloads.get("paper_setup_bridge", {}))
    universe = _symbols(evidence.get("instrument_universe") or blueprint.get("instrument_universe") or bridge.get("instrument_universe") or proposal.get("proposed_universe") or DEFAULT_REQUIRED_SYMBOLS)
    required = _required_fields(evidence, blueprint)
    fields = _construction_fields(
        evidence=evidence,
        promotion=promotion,
        proposal=proposal,
        blueprint=blueprint,
        readiness=readiness,
        setup=setup,
        queue_item=queue_item,
        bridge=bridge,
        universe=universe,
    )
    missing_components = _missing_components(
        evidence=evidence,
        promotion=promotion,
        shadow=shadow,
        proposal=proposal,
        queue_item=queue_item,
        blueprint=blueprint,
        readiness=readiness,
        setup=setup,
        bridge=bridge,
    )
    missing_fields = [key for key in required if not fields.get(key)]
    detail_missing = [key for key, value in fields.items() if _is_required_construction_field(key) and not value]
    missing = sorted(set(missing_components + missing_fields + detail_missing))
    governance_status = _governance_status(queue_item=queue_item, setup=setup, promotion=promotion, bridge=bridge)
    bridge_ready = bridge.get("bridge_status") == "PAPER_SETUP_BRIDGE_READY" and bridge.get("candidate_construction_eligible") is True
    construction_ready = not missing and governance_status == "PAPER_TRACKING_READY" and (setup.get("candidate_generation_eligible") is True or bridge_ready)
    if construction_ready:
        status = "READY_FOR_MARKET_EVALUATION"
        blocker = "NONE"
        reason_codes = ["CANDIDATE_CONSTRUCTION_READY"]
        next_step = "evaluate Oil Shock market setup and emit raw signal only if existing rules qualify"
        ui_message = "Oil Shock candidate construction is complete. Aegis is waiting for qualifying market setup. No David action required."
    else:
        status = "POLICY_INCOMPLETE"
        blocker = "POLICY_INCOMPLETE"
        reason_codes = ["POLICY_INCOMPLETE", *[f"MISSING_{item.upper()}" for item in missing]]
        next_step = "complete governed paper-tracking approval/setup and construction policy before candidate generation"
        ui_message = "Oil Shock candidate construction is incomplete. Aegis system repair required. No David action required."
    row = {
        "hypothesis_id": HYPOTHESIS_ID,
        "hypothesis_name": HYPOTHESIS_NAME,
        "sleeve_id": fields.get("sleeve_id") or "",
        "generated_sleeve_id": fields.get("generated_sleeve_id") or ENGINE_ID,
        "signal_id": fields.get("signal_id") or "",
        "symbol": fields.get("symbol") or "",
        "instrument_basket": universe,
        "direction": fields.get("direction") or "",
        "instrument_type": fields.get("instrument_type") or "",
        "entry_logic": fields.get("entry_logic") or "",
        "exit_logic": fields.get("exit_logic") or "",
        "risk_policy_id": fields.get("risk_policy_id") or "",
        "exit_policy_id": fields.get("exit_policy_id") or "",
        "expected_holding_period": fields.get("expected_holding_period") or "",
        "required_evidence_fields": required,
        "governance_status": governance_status,
        "candidate_construction_status": status,
        "blocker_code": blocker,
        "missing_construction_fields": missing,
        "missing_components": missing_components,
        "reason_codes": reason_codes,
        "candidate_count": 0,
        "raw_signal_count": 0,
        "candidate_flow_started": False,
        "david_action_required": False,
        "next_expected_step": next_step,
        "ui_message": ui_message,
        "recommended_repair": _recommended_repair(missing),
        "normal_pipeline_required": True,
        "candidate_contracts_authoritative": True,
        "paper_lifecycle_authoritative": True,
        "source_artifact_paths": [str(path) for path in paths.values()] + ([str(proposal_path)] if proposal_path else []),
        "source_artifact_hashes": {**{str(path): file_hash_v1(path) for path in paths.values()}, **({str(proposal_path): file_hash_v1(proposal_path)} if proposal_path else {})},
    }
    artifact = {
        "schema_id": "aegis_oil_shock_candidate_construction",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "policy_version": POLICY_VERSION,
        "oil_shock": row,
        **row,
        **SAFETY,
        "safety": dict(SAFETY),
        "computed_at_utc": _now(),
    }
    artifact["deterministic_rerun_id"] = stable_hash_v1({"day_utc": str(day_utc), "policy_version": POLICY_VERSION, "source_hashes": row["source_artifact_hashes"], "status": status})
    artifact["content_hash"] = stable_hash_v1(_without_generated(artifact))
    return artifact


def write_oil_shock_candidate_construction_v1(*, truth_root: Path | str, repo_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_oil_shock_candidate_construction_v1(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    return write_json_v1(oil_shock_candidate_construction_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, repo: Path, day: str) -> dict[str, Path]:
    return {
        "proposal_promotion": report_path_v1(root, "aegis_hypothesis_proposal_promotion_v1", day, "promotion_pipeline.v1.json"),
        "evidence_packets": report_path_v1(root, "aegis_hypothesis_evidence_packet_v1", day, "evidence_packets.v1.json"),
        "shadow_trials": report_path_v1(root, "aegis_hypothesis_shadow_trial_v1", day, "shadow_trials.v1.json"),
        "promotion_packets": report_path_v1(root, "aegis_hypothesis_promotion_packet_v1", day, "promotion_packets.v1.json"),
        "approval_queue": report_path_v1(root, "aegis_paper_promotion_approval_queue_v1", day, "approval_queue.v1.json"),
        "paper_blueprint": report_path_v1(root, "aegis_paper_sleeve_blueprint_v1", day, "paper_sleeve_blueprint.v1.json"),
        "paper_readiness": report_path_v1(root, "aegis_paper_readiness_certification_v1", day, "paper_readiness_certification.v1.json"),
        "paper_setup": report_path_v1(root, "aegis_approved_hypothesis_paper_tracking_setup_v1", day, "approved_hypothesis_paper_tracking_setup.v1.json"),
        "market_data_universe": report_path_v1(root, "aegis_market_data_universe_consistency_v1", day, "market_data_universe_consistency.v1.json"),
        "paper_setup_bridge": report_path_v1(root, "aegis_generated_hypothesis_paper_setup_bridge_v1", day, "generated_hypothesis_paper_setup_bridge.v1.json"),
        "flow_enablement": report_path_v1(root, "aegis_oil_shock_candidate_flow_enablement_v1", day, "oil_shock_candidate_flow_enablement.v1.json"),
        "exit_policy": repo / "ops/aegis/trade_lifecycle/exit_policy_registry_v1.py",
    }


def _oil_bridge(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    row = payload.get("oil_shock") if isinstance(payload.get("oil_shock"), Mapping) else {}
    if row:
        return dict(row)
    for item in payload.get("hypotheses") or []:
        if isinstance(item, Mapping) and (text_v1(item.get("hypothesis_id")) == HYPOTHESIS_ID or "oil shock" in text_v1(item.get("hypothesis_name")).lower()):
            return dict(item)
    return {}


def _find_oil(rows: Any) -> dict[str, Any]:
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        text = " ".join([text_v1(row.get("hypothesis_id")), text_v1(row.get("hypothesis_name")), text_v1(row.get("display_name")), text_v1(row.get("hypothesis_statement"))]).lower()
        if HYPOTHESIS_ID.lower() in text or "oil shock reversals" in text:
            return dict(row)
    return {}


def _proposal_path(payloads: Mapping[str, Any]) -> Path:
    for family in ("evidence_packets", "promotion_packets"):
        row = _find_oil(payloads.get(family, {}).get(family) or payloads.get(family, {}).get("promotion_packets") or payloads.get(family, {}).get("evidence_packets"))
        value = text_v1(row.get("input_proposal_file"))
        if value:
            return Path(value).expanduser()
    return Path()


def _symbols(values: Any) -> list[str]:
    return sorted({text_v1(item).upper() for item in values or [] if text_v1(item)})


def _required_fields(evidence: Mapping[str, Any], blueprint: Mapping[str, Any]) -> list[str]:
    values = evidence.get("required_evidence_fields") or blueprint.get("required_evidence_fields") or []
    return [text_v1(item) for item in values if text_v1(item)]


def _construction_fields(*, evidence: Mapping[str, Any], promotion: Mapping[str, Any], proposal: Mapping[str, Any], blueprint: Mapping[str, Any], readiness: Mapping[str, Any], setup: Mapping[str, Any], queue_item: Mapping[str, Any], bridge: Mapping[str, Any], universe: list[str]) -> dict[str, Any]:
    policy = blueprint.get("candidate_construction_policy") if isinstance(blueprint.get("candidate_construction_policy"), Mapping) else {}
    risk = blueprint.get("risk_policy") if isinstance(blueprint.get("risk_policy"), Mapping) else {}
    primary_symbol = "USO" if "USO" in universe else (universe[0] if universe else "")
    approval_event = queue_item.get("latest_approval_event") if isinstance(queue_item.get("latest_approval_event"), Mapping) else {}
    bridge_ready = bridge.get("bridge_status") == "PAPER_SETUP_BRIDGE_READY" and bridge.get("candidate_construction_eligible") is True
    return {
        "hypothesis_id": HYPOTHESIS_ID,
        "sleeve_id": text_v1(bridge.get("sleeve_id")) if bridge_ready else (ENGINE_ID if setup or blueprint else ""),
        "generated_sleeve_id": text_v1(bridge.get("generated_sleeve_id")) if bridge_ready else ENGINE_ID,
        "signal_id": f"oil_shock_reversal_{primary_symbol.lower()}" if primary_symbol else "",
        "symbol": primary_symbol,
        "direction": "LONG" if (evidence or proposal) else "",
        "instrument_type": "EQUITY_OR_ETF" if universe else "",
        "entry_logic": evidence.get("entry_logic") or blueprint.get("entry_logic") or "",
        "exit_logic": evidence.get("exit_logic") or blueprint.get("exit_logic") or "",
        "risk_policy_id": text_v1(bridge.get("risk_policy_id")) if bridge_ready else (risk.get("policy_id") or ("aegis_research_only_risk_policy_v1" if promotion.get("risk_policy_status") == "RESEARCH_ONLY_PASS" and setup else "")),
        "exit_policy_id": text_v1(bridge.get("exit_policy_id")) if bridge_ready else ("aegis_research_only_exit_policy_v1" if (promotion.get("exit_policy_status") or evidence.get("exit_logic")) and setup else ""),
        "expected_holding_period": evidence.get("expected_holding_period") or blueprint.get("expected_holding_period") or "",
        "candidate_construction_policy": policy.get("policy_id") or "",
        "approval_event_hash": text_v1(bridge.get("approval_event_hash")) if bridge_ready else (approval_event.get("event_hash") or setup.get("approval_event_hash") or blueprint.get("approval_event_hash") or ""),
        "paper_setup_status": text_v1(bridge.get("paper_setup_status")) if bridge_ready else (setup.get("paper_setup_status") or ""),
        "readiness_status": readiness.get("certification_status") or "",
        "hypothesis_statement": evidence.get("hypothesis_statement") or proposal.get("hypothesis") or "",
        "instrument_universe": universe,
        "data_availability": (evidence.get("readiness") or {}).get("data_requirement_status") if isinstance(evidence.get("readiness"), Mapping) else "",
        "sample_frequency": evidence.get("expected_sample_frequency") or blueprint.get("expected_sample_frequency") or "",
    }


def _is_required_construction_field(key: str) -> bool:
    return key in {"hypothesis_id", "sleeve_id", "signal_id", "symbol", "direction", "instrument_type", "entry_logic", "exit_logic", "risk_policy_id", "exit_policy_id", "expected_holding_period", "approval_event_hash", "paper_setup_status"}


def _missing_components(*, evidence: Mapping[str, Any], promotion: Mapping[str, Any], shadow: Mapping[str, Any], proposal: Mapping[str, Any], queue_item: Mapping[str, Any], blueprint: Mapping[str, Any], readiness: Mapping[str, Any], setup: Mapping[str, Any], bridge: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    bridge_ready = bridge.get("bridge_status") == "PAPER_SETUP_BRIDGE_READY" and bridge.get("candidate_construction_eligible") is True
    if not proposal:
        missing.append("hypothesis_proposal")
    if not evidence:
        missing.append("evidence_packet")
    if not shadow:
        missing.append("shadow_trial")
    if not promotion:
        missing.append("promotion_packet")
    approval_event = queue_item.get("latest_approval_event") if isinstance(queue_item.get("latest_approval_event"), Mapping) else {}
    if not bridge_ready and (queue_item.get("approval_state") != "PAPER_PROMOTION_APPROVED" or not approval_event):
        missing.append("approval_event")
    if not bridge_ready and not blueprint:
        missing.append("paper_sleeve_blueprint")
    if not bridge_ready and not readiness:
        missing.append("paper_readiness_certification")
    if not bridge_ready and not setup:
        missing.append("paper_tracking_setup")
    elif not bridge_ready and setup.get("candidate_generation_eligible") is not True:
        missing.append("candidate_generation_eligible_setup")
    if bridge and not bridge_ready:
        missing.append("paper_setup_bridge")
    return missing


def _governance_status(*, queue_item: Mapping[str, Any], setup: Mapping[str, Any], promotion: Mapping[str, Any], bridge: Mapping[str, Any]) -> str:
    if bridge.get("bridge_status") == "PAPER_SETUP_BRIDGE_READY" and bridge.get("paper_setup_status"):
        return text_v1(bridge.get("paper_setup_status"))
    if setup.get("paper_setup_status"):
        return text_v1(setup.get("paper_setup_status"))
    if queue_item.get("approval_state"):
        return text_v1(queue_item.get("approval_state"))
    return text_v1(promotion.get("promotion_decision") or "UNKNOWN")


def _recommended_repair(missing: list[str]) -> str:
    if "approval_event" in missing or "paper_tracking_setup" in missing:
        return "Produce governed paper-promotion approval event and rerun approved-hypothesis paper setup before Oil Shock candidate construction."
    if missing:
        return "Complete missing governed construction fields: " + ", ".join(missing) + "."
    return "No construction repair required; proceed to market setup evaluation."


def _without_generated(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _without_generated(item) for key, item in value.items() if key not in {"computed_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated(item) for item in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
