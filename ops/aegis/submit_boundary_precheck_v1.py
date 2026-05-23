from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.evidence_event_store_v1 import append_evidence_event_v1
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1
from ops.aegis.trade_ticket_lineage_v1 import (
    build_trade_ticket_lineage_v1,
    canonical_construction_contract_v1,
    now_utc_v1,
    sha256_file_v1,
    stable_hash_v1,
    submit_boundary_precheck_path_v1,
    ticket_id_v1,
    write_trade_ticket_lineage_v1,
)


SCHEMA_ID = "submit_boundary_precheck"
SCHEMA_VERSION = "v1"


def build_submit_boundary_precheck_v1(
    *,
    truth_root: Path | str,
    construction: Mapping[str, Any],
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = generated_at_utc or now_utc_v1()
    day = str(construction.get("source_day") or generated[:10])
    ticket_id = ticket_id_v1(construction)
    contract = canonical_construction_contract_v1(construction)
    runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day)
    caps = runtime.get("capabilities") if isinstance(runtime.get("capabilities"), dict) else {}
    runtime_hash = str(runtime.get("deterministic_output_hash") or "")
    manual_cap = caps.get("MANUAL_TRADE_CAPTURE_ALLOWED") if isinstance(caps.get("MANUAL_TRADE_CAPTURE_ALLOWED"), dict) else {}
    trade_advice_cap = caps.get("TRADE_ADVICE_ALLOWED") if isinstance(caps.get("TRADE_ADVICE_ALLOWED"), dict) else {}
    autonomous_cap = caps.get("AUTONOMOUS_EXECUTION_ALLOWED") if isinstance(caps.get("AUTONOMOUS_EXECUTION_ALLOWED"), dict) else {}
    broker_cap = caps.get("BROKER_SUBMIT_TRANSMIT") if isinstance(caps.get("BROKER_SUBMIT_TRANSMIT"), dict) else {}

    lineage = build_trade_ticket_lineage_v1(
        truth_root=root,
        construction={**dict(construction), **contract, "runtime_evaluation_hash": runtime_hash or construction.get("runtime_evaluation_hash", "")},
        generated_at_utc=generated,
        require_submit_boundary=False,
    )
    lineage_status = str(lineage.get("lineage_status") or "")
    if lineage_status == "CAPTURED_HISTORICAL":
        payload = {
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "artifact_id": "",
            "submit_boundary_id": "",
            "ticket_id": ticket_id,
            "day_utc": day,
            "runtime_evaluation_hash": str(lineage.get("runtime_evaluation_hash") or runtime_hash),
            "current_runtime_evaluation_hash": runtime_hash,
            "manual_trade_capture_allowed": bool(manual_cap.get("allowed", False)),
            "trade_advice_allowed": bool(trade_advice_cap.get("allowed", False)),
            "trade_advice_allowed_does_not_authorize_broker_submit": True,
            "autonomous_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "construction_contract_id": contract["construction_contract_id"],
            "construction_contract_hash": str(lineage.get("construction_contract_hash") or contract["construction_contract_hash"]),
            "paper_trade_construction_id": str(lineage.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
            "lineage_status": "CAPTURED_HISTORICAL",
            "ticket_lineage_status": "CAPTURED_HISTORICAL",
            "validation_status": "NOT_APPLICABLE",
            "blocker_codes": [],
            "allowed_boundary": "NONE_POST_CAPTURE_HISTORICAL",
            "post_capture_authority": "IMMUTABLE_CAPTURE_EVIDENCE",
            "capture_record_id": str(lineage.get("capture_record_id") or ""),
            "captured_at_utc": str(lineage.get("captured_at_utc") or ""),
            "submit_boundary_revalidation_required": False,
            "market_freshness_status": "CAPTURE_TIME_FROZEN",
            "paper_intent_status": "CAPTURE_TIME_FROZEN",
            "conversion_status": "CAPTURE_TIME_FROZEN",
            "broker_submission_allowed": False,
            "broker_execution_allowed": False,
            "order_routing_allowed": False,
            "paper_submit_created": False,
            "source_artifact_paths": [str(path) for path in lineage.get("source_artifact_paths") or []],
            "generated_at_utc": str(lineage.get("generated_at_utc") or generated),
        }
        pre_hash = stable_hash_v1({**payload, "submit_boundary_id": "", "submit_boundary_hash": "", "artifact_id": ""})
        payload["submit_boundary_id"] = "submit-boundary-precheck:historical:" + pre_hash[:13]
        payload["artifact_id"] = f"submit_boundary_precheck_v1:{day}:historical:{pre_hash[:10]}"
        payload["submit_boundary_hash"] = pre_hash
        payload["evidence_hash"] = stable_hash_v1({**payload, "evidence_hash": ""})
        return payload
    blockers: list[str] = []
    if not runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if str(construction.get("runtime_evaluation_hash") or runtime_hash) != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if not bool(manual_cap.get("allowed", False)):
        blockers.append("MANUAL_CAPTURE_POLICY_BLOCKED")
    if bool(autonomous_cap.get("allowed", False)):
        blockers.append("AUTONOMOUS_EXECUTION_NOT_DISABLED")
    if bool(broker_cap.get("allowed", False)):
        blockers.append("BROKER_SUBMIT_TRANSMIT_NOT_DISABLED")
    if contract["construction_contract_hash"] != str(construction.get("construction_contract_hash") or contract["construction_contract_hash"]):
        blockers.append("STALE_CONTRACT")
    if lineage_status not in {"ACTIVE_CURRENT", "MISSING_SUBMIT_BOUNDARY"}:
        blockers.extend([str(code) for code in lineage.get("blocker_codes") or [lineage_status] if str(code)])
    evidence_status = lineage.get("evidence_status") if isinstance(lineage.get("evidence_status"), dict) else {}
    if evidence_status.get("market_freshness") != "VALID":
        blockers.append("MARKET_FRESHNESS_MISSING")
    if evidence_status.get("paper_intent") != "VALID":
        blockers.append("PAPER_INTENT_MISSING")
    if evidence_status.get("conversion") != "VALID":
        blockers.append("CONVERSION_MISSING")

    blockers = sorted(set(blockers))
    valid = not blockers
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "submit_boundary_id": "",
        "ticket_id": ticket_id,
        "day_utc": day,
        "runtime_evaluation_hash": runtime_hash,
        "manual_trade_capture_allowed": bool(manual_cap.get("allowed", False)),
        "trade_advice_allowed": bool(trade_advice_cap.get("allowed", False)),
        "trade_advice_allowed_does_not_authorize_broker_submit": True,
        "autonomous_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "construction_contract_id": contract["construction_contract_id"],
        "construction_contract_hash": contract["construction_contract_hash"],
        "paper_trade_construction_id": str(construction.get("construction_id") or ""),
        "market_freshness_status": evidence_status.get("market_freshness") or "MISSING",
        "paper_intent_status": evidence_status.get("paper_intent") or "MISSING",
        "conversion_status": evidence_status.get("conversion") or "MISSING",
        "allocation_status": evidence_status.get("allocation") or "MISSING",
        "allocation_artifact_path": str(lineage.get("allocation_artifact_path") or ""),
        "allocation_artifact_hash": str(lineage.get("allocation_artifact_hash") or ""),
        "allocation_limit_used": lineage.get("allocation_limit_used") if isinstance(lineage.get("allocation_limit_used"), dict) else {},
        "risk_contract_status": evidence_status.get("risk_contract") or "MISSING",
        "risk_contract_path": str(lineage.get("risk_contract_path") or ""),
        "risk_contract_hash": str(lineage.get("risk_contract_hash") or ""),
        "risk_measure": str(lineage.get("risk_measure") or ""),
        "risk_measure_definition": lineage.get("risk_measure_definition") if isinstance(lineage.get("risk_measure_definition"), dict) else {},
        "risk_limit_used": lineage.get("risk_limit_used") if isinstance(lineage.get("risk_limit_used"), dict) else {},
        "candidate_identity_status": evidence_status.get("candidate_identity") or "MISSING",
        "economic_state_status": evidence_status.get("economic_state") or "MISSING",
        "economic_state_path": str(lineage.get("economic_state_path") or ""),
        "economic_state_hash": str(lineage.get("economic_state_hash") or ""),
        "economic_state_build_path": str(lineage.get("economic_state_build_path") or ""),
        "cash_ledger_path": str(lineage.get("cash_ledger_path") or ""),
        "cash_ledger_hash": str(lineage.get("cash_ledger_hash") or ""),
        "cash_ledger_source_type": str(lineage.get("cash_ledger_source_type") or ""),
        "positions_snapshot_path": str(lineage.get("positions_snapshot_path") or ""),
        "positions_snapshot_hash": str(lineage.get("positions_snapshot_hash") or ""),
        "positions_source_type": str(lineage.get("positions_source_type") or ""),
        "position_lifecycle_path": str(lineage.get("position_lifecycle_path") or ""),
        "position_lifecycle_hash": str(lineage.get("position_lifecycle_hash") or ""),
        "accounting_nav_path": str(lineage.get("accounting_nav_path") or ""),
        "accounting_nav_hash": str(lineage.get("accounting_nav_hash") or ""),
        "accounting_nav_source_type": str(lineage.get("accounting_nav_source_type") or ""),
        "candidate_identity_set_path": str(lineage.get("candidate_identity_set_path") or ""),
        "candidate_identity_set_hash": str(lineage.get("candidate_identity_set_hash") or ""),
        "expected_candidate_id": str(lineage.get("expected_candidate_id") or ""),
        "actual_phasec_candidate_id": str(lineage.get("actual_phasec_candidate_id") or ""),
        "actual_phasec_order_plan_path": str(lineage.get("actual_phasec_order_plan_path") or ""),
        "stale_order_plan_reason": str(lineage.get("stale_order_plan_reason") or ""),
        "lineage_status": "ACTIVE_CURRENT" if valid else lineage_status,
        "ticket_lineage_status": "ACTIVE_CURRENT" if valid else lineage_status,
        "validation_status": "VALIDATED" if valid else "REJECTED",
        "blocker_codes": blockers,
        "allowed_boundary": "MANUAL_CAPTURE_RECORD_APPEND_ONLY" if valid else "NONE",
        "broker_submission_allowed": False,
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "paper_submit_created": False,
        "source_artifact_paths": [str(path) for path in lineage.get("source_artifact_paths") or []],
        "generated_at_utc": generated,
    }
    pre_hash = stable_hash_v1({**payload, "submit_boundary_id": "", "submit_boundary_hash": "", "artifact_id": ""})
    payload["submit_boundary_id"] = "submit-boundary-precheck:" + pre_hash[:24]
    payload["artifact_id"] = f"submit_boundary_precheck_v1:{day}:{pre_hash[:20]}"
    payload["submit_boundary_hash"] = pre_hash
    payload["evidence_hash"] = stable_hash_v1({**payload, "evidence_hash": ""})
    return payload


def write_submit_boundary_precheck_v1(*, truth_root: Path | str, payload: Mapping[str, Any], emit_events: bool = True) -> Path:
    root = Path(truth_root).expanduser().resolve()
    day = str(payload.get("day_utc") or "")
    ticket_id = str(payload.get("ticket_id") or "")
    path = submit_boundary_precheck_path_v1(truth_root=root, day_utc=day, ticket_id=ticket_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    if emit_events:
        _emit_precheck_events(root=root, day=day, path=path, payload=dict(payload))
    return path


def build_and_write_submit_boundary_precheck_v1(
    *,
    truth_root: Path | str,
    construction: Mapping[str, Any],
    generated_at_utc: str | None = None,
    emit_events: bool = True,
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    precheck = build_submit_boundary_precheck_v1(truth_root=truth_root, construction=construction, generated_at_utc=generated_at_utc)
    path = write_submit_boundary_precheck_v1(truth_root=truth_root, payload=precheck, emit_events=emit_events)
    lineage = build_trade_ticket_lineage_v1(truth_root=truth_root, construction=construction, generated_at_utc=generated_at_utc, require_submit_boundary=True)
    lineage_path = write_trade_ticket_lineage_v1(truth_root=truth_root, payload=lineage, emit_events=emit_events)
    return {**precheck, "artifact_path": str(path)}, path, {**lineage, "artifact_path": str(lineage_path)}


def _emit_precheck_events(*, root: Path, day: str, path: Path, payload: dict[str, Any]) -> None:
    base = {
        "run_id": f"ops.aegis.submit_boundary_precheck_v1:{day}",
        "parent_run_id": str(payload.get("ticket_id") or ""),
        "day_utc": day,
        "created_at_utc": str(payload.get("generated_at_utc") or now_utc_v1()),
        "producer": "ops.aegis.submit_boundary_precheck_v1",
        "producer_version": "v1",
        "git_sha": "UNKNOWN",
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "input_hashes": {
            "runtime_evaluation_hash": str(payload.get("runtime_evaluation_hash") or ""),
            "construction_contract_hash": str(payload.get("construction_contract_hash") or ""),
        },
        "output_hashes": {str(path): sha256_file_v1(path)},
        "artifact_paths": [str(path)],
    }
    for event_type, status in (
        ("EvidenceProduced", "PRODUCED"),
        ("EvidenceValidated" if payload.get("validation_status") == "VALIDATED" else "EvidenceRejected", str(payload.get("validation_status") or "UNKNOWN")),
    ):
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
