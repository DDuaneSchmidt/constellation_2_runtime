from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.hypothesis_proposal_promotion_v1 import (
    SAFETY,
    SAFETY_STATEMENT,
    approval_queue_path_v1,
    evidence_packets_path_v1,
    promotion_packets_path_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1

REPORT_SPECS = {
    "blueprint": ("aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"),
    "certification": ("aegis_paper_readiness_certification_v1", "paper_readiness_certification.v1.json"),
    "setup": ("aegis_approved_hypothesis_paper_tracking_setup_v1", "approved_hypothesis_paper_tracking_setup.v1.json"),
}


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def report_path_v1(truth_root: Path | str, key: str, day_utc: str) -> Path:
    family, filename = REPORT_SPECS[key]
    return Path(truth_root) / "reports" / family / str(day_utc) / filename


def paper_sleeve_blueprint_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "blueprint", day_utc)


def paper_readiness_certification_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "certification", day_utc)


def approved_hypothesis_paper_tracking_setup_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, "setup", day_utc)


def _report_exists(truth_root: Path | str, family: str, day_utc: str, filename: str) -> bool:
    return (Path(truth_root) / "reports" / family / str(day_utc) / filename).exists()


def _read_payload(path: Path) -> dict[str, Any]:
    payload = read_json_v1(path)
    return payload if isinstance(payload, dict) else {}


def _by_hypothesis(rows: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            hypothesis_id = str(row.get("hypothesis_id") or "")
            if hypothesis_id:
                out[hypothesis_id] = dict(row)
    return out


def _approved_queue_items(queue_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in queue_payload.get("approval_queue") or []
        if isinstance(row, Mapping) and row.get("approval_state") == "PAPER_PROMOTION_APPROVED"
    ]


def build_paper_sleeve_blueprints_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    generated_at = f"{day_utc}T00:00:00Z"
    queue = _read_payload(approval_queue_path_v1(truth_root=truth_root, day_utc=day_utc))
    evidence = _read_payload(evidence_packets_path_v1(truth_root=truth_root, day_utc=day_utc))
    packets = _read_payload(promotion_packets_path_v1(truth_root=truth_root, day_utc=day_utc))
    evidence_by_id = _by_hypothesis(evidence.get("evidence_packets"))
    packet_by_id = _by_hypothesis(packets.get("promotion_packets"))

    blueprints: list[dict[str, Any]] = []
    for item in _approved_queue_items(queue):
        hypothesis_id = str(item.get("hypothesis_id") or "")
        evidence_packet = evidence_by_id.get(hypothesis_id, {})
        promotion_packet = packet_by_id.get(hypothesis_id, {})
        approval_event = item.get("latest_approval_event") if isinstance(item.get("latest_approval_event"), Mapping) else {}
        blueprint = {
            "schema_id": "aegis_paper_sleeve_blueprint_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": hypothesis_id,
            "hypothesis_name": item.get("hypothesis_name") or promotion_packet.get("hypothesis_name") or hypothesis_id,
            "source_proposal_id": approval_event.get("proposal_id") or approval_event.get("hypothesis_proposal_id") or hypothesis_id,
            "promotion_packet_hash": item.get("promotion_packet_hash") or promotion_packet.get("promotion_packet_hash") or "",
            "approval_event_hash": approval_event.get("event_hash") or "",
            "instrument_universe": evidence_packet.get("instrument_universe") or [],
            "entry_logic": evidence_packet.get("entry_logic") or "",
            "exit_logic": evidence_packet.get("exit_logic") or "",
            "expected_holding_period": evidence_packet.get("expected_holding_period") or "",
            "expected_sample_frequency": item.get("expected_sample_frequency") or evidence_packet.get("expected_sample_frequency") or "",
            "required_evidence_fields": evidence_packet.get("required_evidence_fields") or [],
            "candidate_construction_policy": {
                "policy_id": "aegis_research_only_candidate_construction_v1",
                "candidate_generation_eligible_after_readiness": True,
                "requires_valid_candidate_contract": True,
                "requires_entry_reference_price_certification": True,
                "creates_paper_observations_now": False,
                "paper_observation_creation": "Only future qualifying paper candidates can create observations.",
            },
            "risk_policy": {
                "policy_id": "aegis_research_only_risk_policy_v1",
                "paper_research_only": True,
                "known_risks": evidence_packet.get("known_risks") or promotion_packet.get("risks_caveats") or [],
                "no_broker_execution": True,
                "no_trade_advice": True,
                "no_live_trading": True,
                "no_real_capital": True,
            },
            "validation_plan": {
                "expected_validation_timeline": item.get("expected_validation_timeline") or promotion_packet.get("expected_validation_timeline") or "",
                "required_evidence_fields": evidence_packet.get("required_evidence_fields") or [],
                "outcome_registry_schema": "aegis_outcome_registry_v1",
                "entry_price_certification_schema": "aegis_entry_reference_price_certification_v1",
                "next_step": "Awaiting qualifying paper candidates",
            },
            "source_artifact_paths": [
                str(approval_queue_path_v1(truth_root=truth_root, day_utc=day_utc)),
                str(evidence_packets_path_v1(truth_root=truth_root, day_utc=day_utc)),
                str(promotion_packets_path_v1(truth_root=truth_root, day_utc=day_utc)),
            ],
            "safety_statement": SAFETY_STATEMENT,
            "safety": dict(SAFETY),
        }
        blueprint["blueprint_hash"] = _stable_hash({**blueprint, "blueprint_hash": ""})
        blueprints.append(blueprint)

    payload = {
        "schema_id": "aegis_paper_sleeve_blueprint_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "paper_sleeve_blueprints": blueprints,
        "summary": {"blueprint_count": len(blueprints)},
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def build_paper_readiness_certifications_v1(blueprint_payload: Mapping[str, Any], *, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    generated_at = f"{day_utc}T00:00:00Z"
    entry_path_exists = _report_exists(truth_root, "aegis_entry_reference_price_certification_v1", day_utc, "entry_reference_price_certification.v1.json")
    outcome_path_exists = _report_exists(truth_root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json")
    certifications: list[dict[str, Any]] = []
    for blueprint in blueprint_payload.get("paper_sleeve_blueprints") or []:
        if not isinstance(blueprint, Mapping):
            continue
        checks = {
            "entry_logic_exists": bool(str(blueprint.get("entry_logic") or "").strip()),
            "exit_logic_exists": bool(str(blueprint.get("exit_logic") or "").strip()),
            "instrument_universe_exists": bool(blueprint.get("instrument_universe")),
            "required_data_exists": bool(blueprint.get("required_evidence_fields")),
            "candidate_construction_is_possible": bool((blueprint.get("candidate_construction_policy") or {}).get("requires_valid_candidate_contract")) and bool(blueprint.get("instrument_universe")),
            "entry_exit_price_certification_path_exists": entry_path_exists and outcome_path_exists,
            "outcome_validation_schema_exists": outcome_path_exists,
            "no_broker_trading_behavior_enabled": all([
                (blueprint.get("safety") or {}).get("broker_execution_allowed") is False,
                (blueprint.get("safety") or {}).get("trade_advice_allowed") is False,
                (blueprint.get("safety") or {}).get("live_trading_allowed") is False,
                (blueprint.get("safety") or {}).get("real_capital_allowed") is False,
            ]),
            "risk_policy_exists": bool((blueprint.get("risk_policy") or {}).get("policy_id")),
        }
        missing = [key for key, ok in checks.items() if not ok]
        status = "PAPER_READINESS_CERTIFIED" if not missing else "PAPER_READINESS_BLOCKED"
        certification = {
            "schema_id": "aegis_paper_readiness_certification_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": blueprint.get("hypothesis_id"),
            "hypothesis_name": blueprint.get("hypothesis_name"),
            "blueprint_hash": blueprint.get("blueprint_hash"),
            "approval_event_hash": blueprint.get("approval_event_hash"),
            "promotion_packet_hash": blueprint.get("promotion_packet_hash"),
            "certification_status": status,
            "readiness_checks": checks,
            "missing_fields": missing,
            "reason_codes": [f"MISSING_{key.upper()}" for key in missing],
            "safety_statement": SAFETY_STATEMENT,
            "safety": dict(SAFETY),
        }
        certification["certification_hash"] = _stable_hash({**certification, "certification_hash": ""})
        certifications.append(certification)
    payload = {
        "schema_id": "aegis_paper_readiness_certification_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "paper_readiness_certifications": certifications,
        "summary": {
            "certification_count": len(certifications),
            "certified_count": sum(1 for row in certifications if row["certification_status"] == "PAPER_READINESS_CERTIFIED"),
            "blocked_count": sum(1 for row in certifications if row["certification_status"] != "PAPER_READINESS_CERTIFIED"),
        },
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def build_approved_hypothesis_paper_tracking_setup_v1(
    blueprint_payload: Mapping[str, Any],
    certification_payload: Mapping[str, Any],
    *,
    truth_root: Path | str,
    day_utc: str,
) -> dict[str, Any]:
    generated_at = f"{day_utc}T00:00:00Z"
    certifications = _by_hypothesis(certification_payload.get("paper_readiness_certifications"))
    setups: list[dict[str, Any]] = []
    for blueprint in blueprint_payload.get("paper_sleeve_blueprints") or []:
        if not isinstance(blueprint, Mapping):
            continue
        hypothesis_id = str(blueprint.get("hypothesis_id") or "")
        certification = certifications.get(hypothesis_id, {})
        ready = certification.get("certification_status") == "PAPER_READINESS_CERTIFIED"
        setup = {
            "schema_id": "aegis_approved_hypothesis_paper_tracking_setup_item_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "generated_at_utc": generated_at,
            "hypothesis_id": hypothesis_id,
            "hypothesis_name": blueprint.get("hypothesis_name") or hypothesis_id,
            "approval_state": "PAPER_PROMOTION_APPROVED",
            "approved_status_text": "Approved for paper research tracking",
            "blueprint_hash": blueprint.get("blueprint_hash"),
            "certification_hash": certification.get("certification_hash"),
            "promotion_packet_hash": blueprint.get("promotion_packet_hash"),
            "approval_event_hash": blueprint.get("approval_event_hash"),
            "paper_setup_status": "PAPER_TRACKING_READY" if ready else "PAPER_TRACKING_BLOCKED",
            "candidate_generation_eligible": bool(ready),
            "next_step": "Awaiting qualifying paper candidates" if ready else "Resolve paper readiness blockers before candidate eligibility.",
            "reason_codes": certification.get("reason_codes") or [],
            "missing_fields": certification.get("missing_fields") or [],
            "paper_observation_created": False,
            "broker_order_created": False,
            "real_trade_created": False,
            "real_capital_allocation_created": False,
            "trade_advice_created": False,
            "autonomous_execution_enabled": False,
            "safety_statement": SAFETY_STATEMENT,
            "safety": dict(SAFETY),
            "source_artifact_paths": [
                str(paper_sleeve_blueprint_path_v1(truth_root=truth_root, day_utc=day_utc)),
                str(paper_readiness_certification_path_v1(truth_root=truth_root, day_utc=day_utc)),
            ],
        }
        setup["setup_hash"] = _stable_hash({**setup, "setup_hash": ""})
        setups.append(setup)
    payload = {
        "schema_id": "aegis_approved_hypothesis_paper_tracking_setup_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "generated_at_utc": generated_at,
        "paper_tracking_setups": setups,
        "summary": {
            "setup_count": len(setups),
            "paper_tracking_ready_count": sum(1 for row in setups if row["paper_setup_status"] == "PAPER_TRACKING_READY"),
            "paper_tracking_blocked_count": sum(1 for row in setups if row["paper_setup_status"] != "PAPER_TRACKING_READY"),
            "candidate_generation_eligible_count": sum(1 for row in setups if row["candidate_generation_eligible"] is True),
        },
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def build_all_approved_hypothesis_paper_setup_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    blueprints = build_paper_sleeve_blueprints_v1(truth_root=truth_root, day_utc=day_utc)
    certifications = build_paper_readiness_certifications_v1(blueprints, truth_root=truth_root, day_utc=day_utc)
    setup = build_approved_hypothesis_paper_tracking_setup_v1(blueprints, certifications, truth_root=truth_root, day_utc=day_utc)
    write_json_v1(paper_sleeve_blueprint_path_v1(truth_root=truth_root, day_utc=day_utc), blueprints)
    write_json_v1(paper_readiness_certification_path_v1(truth_root=truth_root, day_utc=day_utc), certifications)
    write_json_v1(approved_hypothesis_paper_tracking_setup_path_v1(truth_root=truth_root, day_utc=day_utc), setup)
    return {
        "ok": True,
        "day_utc": str(day_utc),
        "summary": setup.get("summary") or {},
        "oil_shock_setup_status": next((row.get("paper_setup_status") for row in setup.get("paper_tracking_setups") or [] if "oil shock" in str(row.get("hypothesis_name") or "").lower()), ""),
        "paths": {
            "paper_sleeve_blueprint": str(paper_sleeve_blueprint_path_v1(truth_root=truth_root, day_utc=day_utc)),
            "paper_readiness_certification": str(paper_readiness_certification_path_v1(truth_root=truth_root, day_utc=day_utc)),
            "approved_hypothesis_paper_tracking_setup": str(approved_hypothesis_paper_tracking_setup_path_v1(truth_root=truth_root, day_utc=day_utc)),
        },
        "paper_tracking_setups": setup.get("paper_tracking_setups") or [],
        "safety": dict(SAFETY),
    }
