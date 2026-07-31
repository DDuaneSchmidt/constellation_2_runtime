from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_oil_shock_candidate_flow_v1"
FILENAME = "oil_shock_candidate_flow.v1.json"
POLICY_VERSION = "AEGIS_OIL_SHOCK_CANDIDATE_FLOW_PROOF_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_NAME = "Oil shock reversals across energy ETFs"
SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_real_world_position_management": True,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
    "ai_created_or_approved_candidate": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def oil_shock_candidate_flow_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_oil_shock_candidate_flow_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root)
    paths = _paths(root, str(day_utc))
    payloads = {key: read_json_v1(path) for key, path in paths.items() if path.suffix == ".json"}
    hashes = {key: file_hash_v1(path) for key, path in paths.items()}
    oil = _oil_row(payloads)
    result = _classify(oil, payloads, paths)
    artifact = {
        "schema_id": "aegis_oil_shock_candidate_flow",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": str(day_utc),
        "policy_version": POLICY_VERSION,
        "input_artifact_hashes": hashes,
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "policy_version": POLICY_VERSION, "inputs": hashes}),
        "oil_shock": result,
        "summary": {
            "candidate_flow_status": result["candidate_flow_status"],
            "candidate_flow_started": result["candidate_flow_started"],
            "reason_codes": result["reason_codes"],
            "exact_blocker": result["exact_blocker"],
            "david_action_required": result["david_action_required"],
        },
        **SAFETY,
        "safety": dict(SAFETY),
    }
    artifact["content_hash"] = stable_hash_v1(_without_generated_time(artifact))
    return artifact


def write_oil_shock_candidate_flow_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_oil_shock_candidate_flow_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(oil_shock_candidate_flow_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "workflow": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
        "throughput": report_path_v1(root, "aegis_generated_hypothesis_throughput_v1", day, "generated_hypothesis_throughput.v1.json"),
        "follow_through": report_path_v1(root, "aegis_research_follow_through_control_v1", day, "research_follow_through_control.v1.json"),
        "ai_root_cause": report_path_v1(root, "aegis_ai_root_cause_analysis_v1", day, "ai_root_cause_analysis.v1.json"),
        "evidence_packets": report_path_v1(root, "aegis_hypothesis_evidence_packet_v1", day, "evidence_packets.v1.json"),
        "promotion_packets": report_path_v1(root, "aegis_hypothesis_promotion_packet_v1", day, "promotion_packets.v1.json"),
        "paper_setup": report_path_v1(root, "aegis_approved_hypothesis_paper_tracking_setup_v1", day, "approved_hypothesis_paper_tracking_setup.v1.json"),
        "paper_blueprint": report_path_v1(root, "aegis_paper_sleeve_blueprint_v1", day, "paper_sleeve_blueprint.v1.json"),
        "paper_readiness": report_path_v1(root, "aegis_paper_readiness_certification_v1", day, "paper_readiness_certification.v1.json"),
        "candidate_diagnostics": report_path_v1(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"),
        "candidate_producer": report_path_v1(root, "aegis_oil_shock_candidate_producer_v1", day, "oil_shock_candidate_producer.v1.json"),
        "candidate_construction": report_path_v1(root, "aegis_oil_shock_candidate_construction_v1", day, "oil_shock_candidate_construction.v1.json"),
        "generated_paper_setup_bridge": report_path_v1(root, "aegis_generated_hypothesis_paper_setup_bridge_v1", day, "generated_hypothesis_paper_setup_bridge.v1.json"),
        "candidate_state": report_path_v1(root, "aegis_candidate_state_v1", day, "candidate_state.v1.json"),
        "candidate_contracts": report_path_v1(root, "aegis_candidate_contracts_v1", day, "candidate_contracts.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day, "candidate_to_paper_lifecycle.v1.json"),
        "entry_price_certification": report_path_v1(root, "aegis_entry_reference_price_certification_v1", day, "entry_reference_price_certification.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "exit_policy": Path("/home/node/constellation/ops/aegis/trade_lifecycle/exit_policy_registry_v1.py"),
    }


def _oil_row(payloads: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "workflow": _find(payloads.get("workflow", {}).get("hypotheses")),
        "throughput": _find(payloads.get("throughput", {}).get("generated_hypotheses")),
        "follow_up": _find(payloads.get("follow_through", {}).get("follow_ups")),
        "ai": _find(payloads.get("ai_root_cause", {}).get("root_cause_rows")),
        "evidence": _find(payloads.get("evidence_packets", {}).get("evidence_packets")),
        "promotion": _find(payloads.get("promotion_packets", {}).get("promotion_packets")),
        "setup": _find(payloads.get("paper_setup", {}).get("paper_tracking_setups")),
        "blueprint": _find(payloads.get("paper_blueprint", {}).get("paper_sleeve_blueprints")),
        "readiness": _find(payloads.get("paper_readiness", {}).get("paper_readiness_certifications")),
    }



def _artifact_row(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    row = payload.get("oil_shock") if isinstance(payload.get("oil_shock"), Mapping) else payload
    return row if isinstance(row, Mapping) else {}

def _find(rows: Any) -> dict[str, Any]:
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        hay = " ".join([text_v1(row.get("hypothesis_id")), text_v1(row.get("hypothesis_name")), text_v1(row.get("display_name")), text_v1(row.get("hypothesis_statement"))]).lower()
        if OIL_HYPOTHESIS_ID in hay or "oil shock reversals" in hay:
            return dict(row)
    return {}


def _classify(oil: Mapping[str, Mapping[str, Any]], payloads: Mapping[str, Any], paths: Mapping[str, Path]) -> dict[str, Any]:
    workflow = oil.get("workflow", {})
    throughput = oil.get("throughput", {})
    setup = oil.get("setup", {})
    evidence = oil.get("evidence", {})
    blueprint = oil.get("blueprint", {})
    readiness = oil.get("readiness", {})
    ai = oil.get("ai", {})
    candidate_rows = _candidate_rows(payloads)
    lifecycle_rows = [row for row in payloads.get("candidate_lifecycle", {}).get("rows", []) or [] if _is_oil(row)]
    diagnostics_sleeves = payloads.get("candidate_diagnostics", {}).get("sleeves") or []
    oil_sleeves = [row for row in diagnostics_sleeves if _is_oil(row)]
    producer = payloads.get("candidate_producer", {}) if isinstance(payloads.get("candidate_producer"), Mapping) else {}
    construction = payloads.get("candidate_construction", {}) if isinstance(payloads.get("candidate_construction"), Mapping) else {}
    generated_setup = _artifact_row(payloads.get("generated_paper_setup_bridge", {}))
    if producer and not oil_sleeves:
        oil_sleeves = [producer]
    required_fields = list(evidence.get("required_evidence_fields") or blueprint.get("required_evidence_fields") or ["hypothesis_statement", "instrument_universe", "entry_logic", "exit_logic", "data_availability", "sample_frequency"])
    available = _available_fields(evidence, blueprint)
    missing = [field for field in required_fields if field not in available]
    universe = list(evidence.get("instrument_universe") or blueprint.get("instrument_universe") or [])
    candidate_count = len(candidate_rows)
    paper_count = len(lifecycle_rows)
    producer_status = text_v1(producer.get("producer_status") or producer.get("last_evaluation_status"))
    construction_status = text_v1(construction.get("candidate_construction_status") or producer.get("candidate_construction_status"))
    missing_construction_fields = [text_v1(item) for item in construction.get("missing_construction_fields") or producer.get("missing_construction_fields") or [] if text_v1(item)]
    policy_complete = bool((blueprint.get("candidate_construction_policy") or {}).get("requires_valid_candidate_contract")) and bool((readiness.get("readiness_checks") or {}).get("entry_exit_price_certification_path_exists", True)) and paths["exit_policy"].exists()
    generated_setup_ready = bool(generated_setup.get("bridge_status") == "PAPER_SETUP_BRIDGE_READY" and generated_setup.get("candidate_construction_eligible") is True and not generated_setup.get("missing_fields"))
    construction_ready = bool(
        (setup.get("candidate_generation_eligible") is True and not setup.get("missing_fields") and not missing and policy_complete)
        or generated_setup_ready
        or (construction_status == "READY_FOR_MARKET_EVALUATION" and not missing_construction_fields)
    )
    raw_signal_count = int(producer.get("raw_signal_count") or len(producer.get("output_intents") or []))
    if candidate_count:
        status = "CANDIDATE_FLOW_STARTED"
        reasons = ["CANDIDATE_FLOW_STARTED"]
        blocker = "NONE"
        no_reason = "Existing deterministic candidate rows are present for Oil Shock."
        next_step = "existing candidate/paper lifecycle validation"
    elif producer_status == "VALID_CANDIDATE_SIGNAL" and construction_ready and raw_signal_count:
        status = "CANDIDATE_FLOW_STARTED"
        reasons = ["CANDIDATE_FLOW_STARTED", "VALID_CANDIDATE_SIGNAL"]
        blocker = "NONE"
        no_reason = "Oil Shock producer emitted a deterministic raw signal; candidate contracts remain the downstream authority."
        next_step = "route raw signal through signal evidence graph and candidate contracts"
    elif not evidence or missing:
        status = "BLOCKED"
        reasons = ["MISSING_DATA"]
        blocker = "MISSING_DATA"
        no_reason = "Required Oil Shock evidence fields are missing."
        next_step = "provide missing deterministic evidence fields"
    elif not oil_sleeves:
        status = "BLOCKED"
        reasons = ["PRODUCER_MISSING"]
        blocker = "PRODUCER_MISSING"
        no_reason = "Oil Shock has paper-tracking readiness, but no deterministic Oil Shock candidate producer or sleeve row appears in candidate-generation diagnostics."
        next_step = "register or implement deterministic Oil Shock candidate producer, then run the existing candidate lifecycle"
    elif producer_status in {"MISSING_DATA", "SYSTEM_DATA_PIPELINE_REQUIRED"}:
        status = "SYSTEM_DATA_PIPELINE_REQUIRED"
        reasons = ["SYSTEM_DATA_PIPELINE_REQUIRED", *(["MISSING_MARKET_DATA"] if producer.get("missing_market_symbols") else [])]
        blocker = "SYSTEM_DATA_PIPELINE_REQUIRED"
        missing_symbols = ", ".join(str(item) for item in producer.get("missing_market_symbols") or [])
        no_reason = "Oil Shock producer ran, but required system market-data evidence is missing."
        next_step = "repair or populate system market-data evidence" + (f" for: {missing_symbols}" if missing_symbols else "")
    elif producer_status == "CANDIDATE_CONSTRUCTION_INCOMPLETE":
        status = "BLOCKED"
        reasons = ["CANDIDATE_CONSTRUCTION_INCOMPLETE"]
        blocker = "CANDIDATE_CONSTRUCTION_INCOMPLETE"
        no_reason = "Oil Shock producer ran, but candidate construction rules are incomplete."
        next_step = "complete deterministic Oil Shock candidate construction policy"
    elif producer_status == "POLICY_INCOMPLETE" or construction_status == "POLICY_INCOMPLETE":
        status = "POLICY_INCOMPLETE"
        reasons = ["POLICY_INCOMPLETE", *[f"MISSING_{item.upper()}" for item in missing_construction_fields]]
        blocker = "POLICY_INCOMPLETE"
        no_reason = "Oil Shock candidate construction is incomplete. Aegis system repair required. No David action required."
        next_step = "complete governed paper-tracking approval/setup and construction policy before candidate generation"
    elif producer_status == "NO_MARKET_SETUP":
        status = "WAITING_FOR_MARKET_CONDITIONS"
        reasons = ["NO_MARKET_SETUP", "WAIT_FOR_MARKET_CONDITIONS"]
        blocker = "NO_MARKET_SETUP"
        no_reason = "Oil Shock producer ran, but no qualifying market setup was present."
        next_step = "wait for qualifying Oil Shock candidate conditions"
    elif not construction_ready:
        status = "BLOCKED"
        reasons = ["CANDIDATE_CONSTRUCTION_INCOMPLETE"]
        blocker = "CANDIDATE_CONSTRUCTION_INCOMPLETE"
        no_reason = "Candidate construction policy or required evidence is incomplete."
        next_step = "complete candidate construction policy and required evidence"
    elif oil_sleeves and all(int(row.get("candidate_count") or 0) == 0 for row in oil_sleeves):
        status = "WAITING_FOR_MARKET_CONDITIONS"
        reasons = ["NO_MARKET_SETUP", "WAIT_FOR_MARKET_CONDITIONS"]
        blocker = "NO_MARKET_SETUP"
        no_reason = "Oil Shock producer ran, but no qualifying market setup was present."
        next_step = "wait for qualifying Oil Shock candidate conditions"
    else:
        status = "BLOCKED"
        reasons = ["IMPLEMENTATION_DEFECT"]
        blocker = "IMPLEMENTATION_DEFECT"
        no_reason = "Candidate artifacts are internally inconsistent for Oil Shock."
        next_step = "investigate candidate-generation implementation defect"
    return {
        "hypothesis_id": text_v1(workflow.get("hypothesis_id") or evidence.get("hypothesis_id") or OIL_HYPOTHESIS_ID),
        "hypothesis_name": text_v1(workflow.get("display_name") or evidence.get("hypothesis_statement") or OIL_NAME),
        "current_state": text_v1(workflow.get("current_state") or throughput.get("throughput_status") or setup.get("paper_setup_status")),
        "paper_setup_status": text_v1(setup.get("paper_setup_status") or readiness.get("certification_status")),
        "instrument_universe": universe,
        "required_evidence_fields": required_fields,
        "available_evidence_fields": sorted(available),
        "missing_evidence_fields": missing,
        "candidate_construction_status": construction_status,
        "missing_construction_fields": missing_construction_fields,
        "candidate_generation_rules": {
            "candidate_generation_eligible_after_readiness": bool((blueprint.get("candidate_construction_policy") or {}).get("candidate_generation_eligible_after_readiness") or setup.get("candidate_generation_eligible")),
            "requires_valid_candidate_contract": bool((blueprint.get("candidate_construction_policy") or {}).get("requires_valid_candidate_contract")),
            "requires_entry_reference_price_certification": bool((blueprint.get("candidate_construction_policy") or {}).get("requires_entry_reference_price_certification") or generated_setup_ready),
            "paper_observation_creation": text_v1((blueprint.get("candidate_construction_policy") or {}).get("paper_observation_creation") or "Only downstream lifecycle may create paper observations."),
            "do_not_force_candidates": True,
        },
        "candidate_construction_readiness": "READY" if construction_ready else "NOT_READY",
        "entry_price_certification_path": str(paths["entry_price_certification"]),
        "exit_policy_path": str(paths["exit_policy"]),
        "producer_status": producer_status or ("REGISTERED" if oil_sleeves else "MISSING"),
        "producer_registered": bool(oil_sleeves or producer),
        "producer_run_status": "RAN" if producer else "NOT_RUN",
        "candidate_producer_status": producer_status or ("REGISTERED" if oil_sleeves else "MISSING"),
        "last_evaluation_status": producer_status or "",
        "candidate_flow_status": status,
        "candidate_flow_started": bool(candidate_count or (status == "CANDIDATE_FLOW_STARTED" and raw_signal_count)),
        "candidate_count": candidate_count,
        "raw_signal_count": raw_signal_count,
        "candidate_contract_count": candidate_count,
        "paper_observation_count": paper_count,
        "existing_candidate_ids": [text_v1(row.get("candidate_id")) for row in candidate_rows],
        "reason_no_candidates_generated_yet": "" if candidate_count else no_reason,
        "reason_codes": reasons,
        "blocker_code": blocker,
        "blocker_owner": ("MARKET_CONDITIONS" if blocker == "NO_MARKET_SETUP" else ("NONE" if blocker == "NONE" else "AEGIS_SYSTEM")),
        "blocker_classification": blocker,
        "exact_blocker": blocker,
        "due_to": {
            "missing_data": "MISSING_DATA" in reasons,
            "no_market_setup": "NO_MARKET_SETUP" in reasons,
            "missing_candidate_producer": "PRODUCER_MISSING" in reasons,
            "incomplete_candidate_construction_policy": "CANDIDATE_CONSTRUCTION_INCOMPLETE" in reasons,
            "incomplete_instrument_universe": not bool(universe),
            "incomplete_exit_risk_policy": "POLICY_INCOMPLETE" in reasons,
            "stale_artifact": False,
            "implementation_defect": "IMPLEMENTATION_DEFECT" in reasons,
        },
        "next_expected_step": next_step,
        "david_action_required": False,
        "ui_message": (
            "Oil Shock candidate flow started."
            if status == "CANDIDATE_FLOW_STARTED"
            else (
                "No David action required. Oil Shock producer ran; no qualifying market setup."
                if blocker == "NO_MARKET_SETUP"
                else (
                    "Oil Shock requires system market-data evidence. No David action required unless Aegis creates a source action."
                    if blocker == "SYSTEM_DATA_PIPELINE_REQUIRED"
                    else "No David action required. Aegis is waiting for qualifying Oil Shock candidate conditions."
                )
            )
        ),
        "ai_root_cause_advisory": "; ".join(ai.get("likely_causes") or []),
        "source_candidate_lifecycle_path": str(paths["candidate_lifecycle"]),
        **SAFETY,
    }


def _candidate_rows(payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key, field in [("candidate_state", "candidates"), ("candidate_contracts", "candidate_contracts")]:
        for row in payloads.get(key, {}).get(field, []) or []:
            if isinstance(row, Mapping) and _is_oil(row):
                rows.append(dict(row))
    return rows


def _is_oil(row: Mapping[str, Any]) -> bool:
    hay = " ".join(text_v1(row.get(k)) for k in ["hypothesis_id", "hypothesis_name", "display_name", "sleeve_id", "raw_signal_id", "event_family_id", "candidate_id"]).lower()
    return OIL_HYPOTHESIS_ID in hay or "oil_shock" in hay or "oil shock" in hay


def _available_fields(evidence: Mapping[str, Any], blueprint: Mapping[str, Any]) -> set[str]:
    available = set()
    checks = {
        "hypothesis_statement": evidence.get("hypothesis_statement") or blueprint.get("hypothesis_name"),
        "instrument_universe": evidence.get("instrument_universe") or blueprint.get("instrument_universe"),
        "entry_logic": evidence.get("entry_logic") or blueprint.get("entry_logic"),
        "exit_logic": evidence.get("exit_logic") or blueprint.get("exit_logic"),
        "data_availability": (evidence.get("readiness") or {}).get("data_requirement_status") or (evidence.get("readiness") or {}).get("market_data_availability"),
        "sample_frequency": evidence.get("expected_sample_frequency") or blueprint.get("expected_sample_frequency"),
    }
    for key, value in checks.items():
        if value:
            available.add(key)
    return available


def _without_generated_time(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _without_generated_time(v) for k, v in value.items() if k not in {"computed_at_utc", "generated_at", "generated_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated_time(v) for v in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
