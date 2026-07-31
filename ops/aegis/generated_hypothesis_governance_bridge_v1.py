from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_generated_hypothesis_governance_bridge_v1"
FILENAME = "generated_hypothesis_governance_bridge.v1.json"
POLICY_VERSION = "AEGIS_GENERATED_HYPOTHESIS_GOVERNANCE_BRIDGE_V1"
OIL_SHOCK_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_SHOCK_NAME = "Oil shock reversals across energy ETFs"
GENERATED_SLEEVE_ID = "GENERATED_OIL_SHOCK_REVERSAL_ETF_V1"
RISK_POLICY_ID = "GENERATED_RESEARCH_PAPER_RISK_POLICY_V1"
EXIT_POLICY_ID = "GENERATED_RESEARCH_PAPER_EXIT_POLICY_V1"
CONSTRUCTION_POLICY_ID = "GENERATED_OIL_SHOCK_CANDIDATE_CONSTRUCTION_POLICY_V1"
VALIDATION_PLAN_ID = "GENERATED_OIL_SHOCK_VALIDATION_PLAN_V1"
BLUEPRINT_ID = "GENERATED_OIL_SHOCK_PAPER_SLEEVE_BLUEPRINT_V1"
READINESS_ID = "GENERATED_OIL_SHOCK_PAPER_READINESS_CERTIFICATION_V1"
TRACKING_ID = "GENERATED_OIL_SHOCK_PAPER_TRACKING_SETUP_V1"
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


def generated_hypothesis_governance_bridge_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, str(day_utc), FILENAME)


def build_generated_hypothesis_governance_bridge_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _paths(root, day)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    row = _row(
        proposal_state=_find_oil(payloads.get("proposal_promotion", {}).get("proposal_states")),
        evidence=_find_oil(payloads.get("evidence_packets", {}).get("evidence_packets")),
        shadow=_find_oil(payloads.get("shadow_trials", {}).get("shadow_trials")),
        promotion=_find_oil(payloads.get("promotion_packets", {}).get("promotion_packets")),
        queue_item=_find_oil(payloads.get("approval_queue", {}).get("approval_queue")),
        paper_bridge=_find_oil_bridge(payloads.get("paper_setup_bridge", {})),
        market_universe=payloads.get("market_data_universe", {}),
        workflow_state=_find_oil(payloads.get("workflow_state", {}).get("hypotheses")),
        paths=paths,
    )
    rows = [row]
    artifact = {
        "schema_id": "aegis_generated_hypothesis_governance_bridge",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": day,
        "policy_version": POLICY_VERSION,
        "governance_bridge_status": row.get("governance_bridge_status"),
        "hypotheses": rows,
        "oil_shock": row,
        **row,
        "summary": {
            "bridge_count": len(rows),
            "ready_count": sum(1 for item in rows if item.get("governance_bridge_status") == "GOVERNANCE_BRIDGE_READY"),
            "blocked_count": sum(1 for item in rows if item.get("governance_bridge_status") != "GOVERNANCE_BRIDGE_READY"),
            "oil_shock_governance_bridge_status": row.get("governance_bridge_status"),
            "oil_shock_missing_fields": row.get("missing_fields") or [],
        },
        **SAFETY,
        "safety": dict(SAFETY),
        "computed_at_utc": _now(),
    }
    artifact["deterministic_rerun_id"] = stable_hash_v1({"day_utc": day, "policy_version": POLICY_VERSION, "source_hashes": row["source_artifact_hashes"], "status": row.get("governance_bridge_status")})
    artifact["content_hash"] = stable_hash_v1(_without_generated(artifact))
    return artifact


def write_generated_hypothesis_governance_bridge_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_governance_bridge_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_governance_bridge_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "proposal_promotion": report_path_v1(root, "aegis_hypothesis_proposal_promotion_v1", day, "promotion_pipeline.v1.json"),
        "evidence_packets": report_path_v1(root, "aegis_hypothesis_evidence_packet_v1", day, "evidence_packets.v1.json"),
        "shadow_trials": report_path_v1(root, "aegis_hypothesis_shadow_trial_v1", day, "shadow_trials.v1.json"),
        "promotion_packets": report_path_v1(root, "aegis_hypothesis_promotion_packet_v1", day, "promotion_packets.v1.json"),
        "approval_queue": report_path_v1(root, "aegis_paper_promotion_approval_queue_v1", day, "approval_queue.v1.json"),
        "approval_lineage": report_path_v1(root, "aegis_generated_hypothesis_approval_event_lineage_v1", day, "generated_hypothesis_approval_event_lineage.v1.json"),
        "paper_setup_bridge": report_path_v1(root, "aegis_generated_hypothesis_paper_setup_bridge_v1", day, "generated_hypothesis_paper_setup_bridge.v1.json"),
        "market_data_universe": report_path_v1(root, "aegis_market_data_universe_consistency_v1", day, "market_data_universe_consistency.v1.json"),
        "workflow_state": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
    }


def _row(*, proposal_state: Mapping[str, Any], evidence: Mapping[str, Any], shadow: Mapping[str, Any], promotion: Mapping[str, Any], queue_item: Mapping[str, Any], paper_bridge: Mapping[str, Any], market_universe: Mapping[str, Any], workflow_state: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    approval_lineage = read_json_v1(paths["approval_lineage"])
    lineage_row = approval_lineage.get("oil_shock") if isinstance(approval_lineage.get("oil_shock"), Mapping) else approval_lineage
    approval_event = lineage_row.get("approval_event") if isinstance(lineage_row.get("approval_event"), Mapping) else {}
    approval_event_hash = text_v1(lineage_row.get("approval_event_hash"))
    approval_present = lineage_row.get("approval_lineage_status") == "APPROVAL_EVENT_FOUND" and bool(approval_event_hash)
    policy_authority = _policy_authority(evidence=evidence, promotion=promotion)
    universe = _symbols(evidence.get("instrument_universe") or paper_bridge.get("instrument_universe") or DEFAULT_REQUIRED_SYMBOLS)
    market_status = text_v1(market_universe.get("status") or market_universe.get("coverage_status"))
    generated_defaults = {
        "risk_policy_id": RISK_POLICY_ID,
        "exit_policy_id": EXIT_POLICY_ID,
        "candidate_construction_policy_id": CONSTRUCTION_POLICY_ID,
        "validation_plan_id": VALIDATION_PLAN_ID,
        "policy_source": "generated_research_defaults",
        "default_policy_version": POLICY_VERSION,
        "default_policy_scope": "paper research metadata only; no trading, allocation, advice, or execution authority",
    } if policy_authority else {}
    fields = {
        "approval_event": approval_present,
        "approval_event_hash": bool(approval_event_hash),
        "generated_sleeve_id": True,
        "sleeve_id": True,
        "sleeve_name": True,
        "risk_policy_id": bool(generated_defaults.get("risk_policy_id")),
        "exit_policy_id": bool(generated_defaults.get("exit_policy_id")),
        "candidate_construction_policy_id": bool(generated_defaults.get("candidate_construction_policy_id")),
        "validation_plan_id": bool(generated_defaults.get("validation_plan_id")),
        "paper_sleeve_blueprint_id": True,
        "paper_readiness_certification_id": True,
        "paper_tracking_setup_id": True,
    }
    missing_fields = [key for key, ok in fields.items() if not ok]
    reason_codes: list[str] = []
    if not evidence:
        reason_codes.append("EVIDENCE_PACKET_MISSING")
    if not shadow:
        reason_codes.append("SHADOW_TRIAL_MISSING")
    if not promotion:
        reason_codes.append("PROMOTION_PACKET_MISSING")
    if not approval_present:
        reason_codes.append(text_v1(lineage_row.get("approval_lineage_status")) or "APPROVAL_EVENT_MISSING")
    if not policy_authority:
        reason_codes.append("GOVERNANCE_POLICY_MAPPING_MISSING")
    reason_codes.extend(f"MISSING_{field.upper()}" for field in missing_fields)
    ready = not missing_fields and approval_present and policy_authority
    if ready:
        status = "GOVERNANCE_BRIDGE_READY"
        reason_codes = ["GOVERNANCE_BRIDGE_READY", "GENERATED_RESEARCH_DEFAULTS_EXPLICIT"]
        ui_message = "Oil Shock governance bridge is ready. Aegis can evaluate Oil Shock candidate construction through normal gates."
        next_step = "run generated hypothesis paper setup bridge and Oil Shock candidate construction"
    else:
        status = "BLOCKED"
        ui_message = "Oil Shock governance bridge blocked: " + ", ".join(missing_fields) + "."
        next_step = "complete approval event and governance policy mapping before paper setup bridge eligibility"
    return {
        "hypothesis_id": OIL_SHOCK_HYPOTHESIS_ID,
        "hypothesis_name": OIL_SHOCK_NAME,
        "approval_event": approval_event if approval_present else {},
        "approval_event_present": bool(approval_present),
        "approval_event_hash": approval_event_hash,
        "approval_lineage_status": text_v1(lineage_row.get("approval_lineage_status")),
        "approval_lineage_source_path": text_v1(lineage_row.get("approval_source_path")),
        "approval_lineage_source_hash": text_v1(lineage_row.get("approval_source_hash")),
        "generated_sleeve_id": GENERATED_SLEEVE_ID,
        "sleeve_id": GENERATED_SLEEVE_ID,
        "sleeve_name": "Generated Oil Shock Reversal ETF Paper Research Sleeve",
        "instrument_universe": universe,
        "risk_policy_id": generated_defaults.get("risk_policy_id", ""),
        "exit_policy_id": generated_defaults.get("exit_policy_id", ""),
        "candidate_construction_policy_id": generated_defaults.get("candidate_construction_policy_id", ""),
        "validation_plan_id": generated_defaults.get("validation_plan_id", ""),
        "paper_sleeve_blueprint_id": BLUEPRINT_ID,
        "paper_readiness_certification_id": READINESS_ID,
        "paper_tracking_setup_id": TRACKING_ID,
        "generated_research_defaults_used": bool(generated_defaults),
        "generated_research_defaults": generated_defaults,
        "policy_authority": policy_authority,
        "market_data_universe_status": market_status,
        "workflow_state": text_v1(workflow_state.get("state") or proposal_state.get("state")),
        "governance_bridge_status": status,
        "missing_fields": missing_fields,
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
        "source_artifact_paths": [str(path) for path in paths.values()],
        "source_artifact_hashes": {str(path): file_hash_v1(path) for path in paths.values()},
        "computed_at_utc": _now(),
    }


def _policy_authority(*, evidence: Mapping[str, Any], promotion: Mapping[str, Any]) -> bool:
    return all([
        bool(evidence),
        bool(text_v1(evidence.get("entry_logic"))),
        bool(text_v1(evidence.get("exit_logic"))),
        promotion.get("risk_policy_status") == "RESEARCH_ONLY_PASS",
        promotion.get("exit_policy_status") == "RESEARCH_ONLY_DEFINED",
    ])


def _find_oil(rows: Any) -> dict[str, Any]:
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        text = " ".join([text_v1(row.get("hypothesis_id")), text_v1(row.get("hypothesis_name")), text_v1(row.get("display_name")), text_v1(row.get("hypothesis_statement"))]).lower()
        if OIL_SHOCK_HYPOTHESIS_ID.lower() in text or "oil shock reversals" in text:
            return dict(row)
    return {}


def _find_oil_bridge(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    row = payload.get("oil_shock") if isinstance(payload.get("oil_shock"), Mapping) else {}
    if row:
        return dict(row)
    return _find_oil(payload.get("hypotheses"))


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
