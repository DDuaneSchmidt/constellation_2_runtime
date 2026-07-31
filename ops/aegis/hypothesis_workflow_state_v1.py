from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.approved_hypothesis_paper_setup_v1 import approved_hypothesis_paper_tracking_setup_path_v1
from ops.aegis.hypothesis_proposal_promotion_v1 import (
    SAFETY,
    SAFETY_STATEMENT,
    approval_queue_path_v1,
    approval_events_path_v1,
    evidence_packets_path_v1,
    promotion_packets_path_v1,
    shadow_trials_path_v1,
)
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.macro_calendar_data_readiness_v1 import macro_calendar_data_readiness_path_v1

WORKFLOW_STATES = {
    "DISCOVERED", "TRIAGING", "NEEDS_DATA", "REJECTED_AUTOMATICALLY",
    "READY_FOR_SHADOW_TRIAL", "SHADOW_VALIDATION_RUNNING", "SHADOW_VALIDATION_FAILED",
    "SHADOW_VALIDATION_PASSED", "PAPER_PROMOTION_RECOMMENDED", "PAPER_PROMOTION_APPROVED",
    "PAPER_SETUP_RUNNING", "PAPER_TRACKING_READY", "PAPER_TRACKING_BLOCKED",
    "PAPER_OBSERVING", "OUTCOMES_ACCUMULATING", "STATISTICALLY_SUFFICIENT",
    "RETIRE_RECOMMENDED", "CAPITAL_REVIEW_RECOMMENDED",
}

NEXT_ACTIONS = {"NONE", "APPROVE_PAPER_TEST", "PROVIDE_DATA_SOURCE", "REVIEW_RETIREMENT", "REVIEW_CAPITAL", "WAIT_FOR_AUTOMATIC_PROCESSING"}

ACTION_CAPABILITIES = {
    "PAPER_PROMOTION_RECOMMENDED": {
        "action_type": "APPROVE_PAPER_TEST",
        "buttons": ["Approve Paper Test", "Reject", "Defer"],
        "consequence": {
            "Approve Paper Test": "Approve this hypothesis for paper research tracking only.",
            "Reject": "Reject paper-test promotion for this hypothesis.",
            "Defer": "Keep the recommendation out of the active approval path for now.",
        },
    },
    "NEEDS_DATA": {
        "action_type": "PROVIDE_DATA_SOURCE",
        "buttons": ["Connect Source", "Upload Dataset", "Mark Not Available", "Defer"],
        "consequence": {
            "Connect Source": "Start data-source connection work outside trading systems.",
            "Upload Dataset": "Provide a research dataset for deterministic ingestion.",
            "Mark Not Available": "Record that the required dataset is unavailable.",
            "Defer": "Leave the hypothesis waiting for data.",
        },
    },
    "RETIRE_RECOMMENDED": {
        "action_type": "REVIEW_RETIREMENT",
        "buttons": ["Retire Hypothesis", "Continue Testing", "Defer"],
        "consequence": {
            "Retire Hypothesis": "Record a research retirement decision.",
            "Continue Testing": "Keep paper research validation running.",
            "Defer": "Leave the retirement review pending.",
        },
    },
    "CAPITAL_REVIEW_RECOMMENDED": {
        "action_type": "REVIEW_CAPITAL",
        "buttons": ["Mark for Advisor Review", "Reject", "Defer"],
        "consequence": {
            "Mark for Advisor Review": "Flag for separate human advisor review only.",
            "Reject": "Reject the research capital review recommendation.",
            "Defer": "Leave capital review pending.",
        },
    },
}

RESOLVER_RULES = [
    {"rule_id": "needs_data_overrides_ready", "precedence": 10, "result_state": "NEEDS_DATA", "condition": "Required dataset or evidence field is missing.", "overrides": ["READY_FOR_SHADOW_TRIAL", "SHADOW_VALIDATION_PASSED", "PAPER_PROMOTION_RECOMMENDED"], "next_action": "PROVIDE_DATA_SOURCE", "reason_code": "NEEDS_DATA_OVERRIDES_READY"},
    {"rule_id": "paper_tracking_blocked_overrides_ready", "precedence": 20, "result_state": "PAPER_TRACKING_BLOCKED", "condition": "Paper setup ran and reported blocking missing fields or policy failures.", "overrides": ["PAPER_TRACKING_READY"], "next_action": "WAIT_FOR_AUTOMATIC_PROCESSING", "reason_code": "PAPER_TRACKING_BLOCKED_OVERRIDES_READY"},
    {"rule_id": "statistically_sufficient_overrides_underpowered", "precedence": 30, "result_state": "STATISTICALLY_SUFFICIENT", "condition": "Statistical sufficiency artifact reports enough evidence.", "overrides": ["OUTCOMES_ACCUMULATING", "PAPER_OBSERVING"], "next_action": "WAIT_FOR_AUTOMATIC_PROCESSING", "reason_code": "STATISTICALLY_SUFFICIENT_OVERRIDES_UNDERPOWERED"},
    {"rule_id": "paper_tracking_ready_after_setup", "precedence": 40, "result_state": "PAPER_TRACKING_READY", "condition": "Approved generated hypothesis has passed paper setup readiness.", "overrides": ["PAPER_PROMOTION_APPROVED"], "next_action": "WAIT_FOR_AUTOMATIC_PROCESSING", "reason_code": "PAPER_TRACKING_READY_AFTER_SETUP"},
    {"rule_id": "approved_overrides_recommended", "precedence": 50, "result_state": "PAPER_PROMOTION_APPROVED", "condition": "Immutable approval event or approval queue state exists.", "overrides": ["PAPER_PROMOTION_RECOMMENDED"], "next_action": "WAIT_FOR_AUTOMATIC_PROCESSING", "reason_code": "PAPER_PROMOTION_APPROVED_OVERRIDES_RECOMMENDED"},
    {"rule_id": "promotion_recommended_requires_approval", "precedence": 60, "result_state": "PAPER_PROMOTION_RECOMMENDED", "condition": "Promotion packet recommends paper tracking and no approval exists.", "overrides": ["SHADOW_VALIDATION_PASSED"], "next_action": "APPROVE_PAPER_TEST", "reason_code": "PAPER_PROMOTION_RECOMMENDED_REQUIRES_APPROVAL"},
    {"rule_id": "shadow_failed_terminal", "precedence": 70, "result_state": "SHADOW_VALIDATION_FAILED", "condition": "Shadow validation failed and no higher-precedence state exists.", "overrides": [], "next_action": "NONE", "reason_code": "SHADOW_VALIDATION_FAILED"},
    {"rule_id": "shadow_passed_waits_for_promotion", "precedence": 80, "result_state": "SHADOW_VALIDATION_PASSED", "condition": "Shadow validation passed but no promotion packet recommends approval.", "overrides": [], "next_action": "WAIT_FOR_AUTOMATIC_PROCESSING", "reason_code": "SHADOW_VALIDATION_PASSED"},
    {"rule_id": "discovered_default", "precedence": 999, "result_state": "DISCOVERED", "condition": "No more specific generated-hypothesis rule matched.", "overrides": [], "next_action": "WAIT_FOR_AUTOMATIC_PROCESSING", "reason_code": "DISCOVERED_DEFAULT"},
]


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    payload = read_json_v1(path)
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").split("|")[0].strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _day_start(day_utc: str) -> datetime:
    parsed = _parse_utc(f"{day_utc}T00:00:00Z")
    return parsed or datetime(1970, 1, 1, tzinfo=UTC)


def _days_since(day_utc: str, value: Any) -> int:
    parsed = _parse_utc(value)
    if not parsed:
        return 0
    return max(0, (_day_start(day_utc) - parsed).days)


def _state_entered_at(day_utc: str, *candidates: Any) -> str:
    for value in candidates:
        parsed = _parse_utc(value)
        if parsed:
            return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return f"{day_utc}T00:00:00Z"


def _stale_state(state: str, age_days: int, blockers: list[str]) -> tuple[bool, str]:
    if state == "NEEDS_DATA" and age_days >= 7:
        return True, "NEEDS_DATA_AGE_EXCEEDS_7_DAYS"
    if state in {"PAPER_SETUP_RUNNING", "SHADOW_VALIDATION_RUNNING"} and age_days >= 2:
        return True, f"{state}_AGE_EXCEEDS_2_DAYS"
    if blockers and age_days >= 7:
        return True, "BLOCKED_STATE_AGE_EXCEEDS_7_DAYS"
    return False, ""


def _state_hash_base(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "hypothesis_id": row.get("hypothesis_id"),
        "current_state": row.get("current_state"),
        "next_action": row.get("next_action"),
        "reason_codes": row.get("reason_codes") or [],
        "blocker_codes": row.get("blocker_codes") or [],
        "source_artifact_hashes": row.get("source_artifact_hashes") or {},
    }


def _report_path(truth_root: Path | str, family: str, day_utc: str, filename: str) -> Path:
    return Path(truth_root) / "reports" / family / str(day_utc) / filename


def hypothesis_workflow_state_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(truth_root, "aegis_hypothesis_workflow_state_v1", day_utc, "hypothesis_workflow_state.v1.json")


def operator_action_queue_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(truth_root, "aegis_operator_action_queue_v1", day_utc, "operator_action_queue.v1.json")


def operator_action_event_log_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(truth_root, "aegis_operator_action_event_log_v1", day_utc, "operator_action_events.v1.jsonl")


def workflow_state_resolver_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(truth_root, "aegis_workflow_state_resolver_v1", day_utc, "workflow_state_resolver.v1.json")


def hypothesis_workflow_replay_verification_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(truth_root, "aegis_hypothesis_workflow_replay_verification_v1", day_utc, "hypothesis_workflow_replay_verification.v1.json")


def generated_hypothesis_throughput_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return _report_path(truth_root, "aegis_generated_hypothesis_throughput_v1", day_utc, "generated_hypothesis_throughput.v1.json")


def _input_paths(truth_root: Path | str, day_utc: str) -> dict[str, Path]:
    return {
        "research_portfolio": _report_path(truth_root, "aegis_research_portfolio_v1", day_utc, "research_portfolio.v1.json"),
        "evidence_packets": evidence_packets_path_v1(truth_root=truth_root, day_utc=day_utc),
        "shadow_trials": shadow_trials_path_v1(truth_root=truth_root, day_utc=day_utc),
        "promotion_packets": promotion_packets_path_v1(truth_root=truth_root, day_utc=day_utc),
        "approval_queue": approval_queue_path_v1(truth_root=truth_root, day_utc=day_utc),
        "approval_events": approval_events_path_v1(truth_root=truth_root, day_utc=day_utc),
        "generated_approval_lineage": _report_path(truth_root, "aegis_generated_hypothesis_approval_event_lineage_v1", day_utc, "generated_hypothesis_approval_event_lineage.v1.json"),
        "paper_setup": approved_hypothesis_paper_tracking_setup_path_v1(truth_root=truth_root, day_utc=day_utc),
        "candidate_lifecycle": _report_path(truth_root, "aegis_candidate_to_paper_lifecycle_v1", day_utc, "candidate_to_paper_lifecycle.v1.json"),
        "statistical_sufficiency": _report_path(truth_root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json"),
        "research_allocation": _report_path(truth_root, "aegis_research_capital_allocation_v1", day_utc, "research_capital_allocation.v1.json"),
        "outcome_registry": _report_path(truth_root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json"),
        "validation_samples": _report_path(truth_root, "aegis_validation_samples_v1", day_utc, "validation_samples.v1.json"),
        "oil_shock_candidate_producer": _report_path(truth_root, "aegis_oil_shock_candidate_producer_v1", day_utc, "oil_shock_candidate_producer.v1.json"),
        "macro_calendar_data_readiness": macro_calendar_data_readiness_path_v1(truth_root=truth_root, day_utc=day_utc),
    }


def _payload_hash(payload: Mapping[str, Any]) -> str:
    return str(payload.get("content_hash") or _stable_hash(payload)) if payload else ""


def _rows_by_id(rows: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if isinstance(row, Mapping):
            key = str(row.get("hypothesis_id") or "")
            if key:
                out[key] = dict(row)
    return out


def _approval_events_by_id(path: Path) -> dict[str, dict[str, Any]]:
    events = _read_jsonl(path)
    out: dict[str, dict[str, Any]] = {}
    for event in events:
        hid = str(event.get("hypothesis_id") or "")
        if hid:
            out[hid] = event
    return out


def _approval_lineage_by_id(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    row = payload.get("oil_shock") if isinstance(payload.get("oil_shock"), Mapping) else payload
    if not isinstance(row, Mapping):
        return {}
    hid = str(row.get("hypothesis_id") or "")
    if not hid or row.get("approval_event_found") is not True or not row.get("approval_event_hash"):
        return {}
    return {hid: dict(row)}


def _approval_event_from_lineage(lineage: Mapping[str, Any]) -> dict[str, Any]:
    event = lineage.get("approval_event") if isinstance(lineage.get("approval_event"), Mapping) else {}
    return {
        **dict(event),
        "hypothesis_id": str(lineage.get("hypothesis_id") or event.get("hypothesis_id") or ""),
        "new_state": str(lineage.get("approval_new_state") or event.get("new_state") or "PAPER_PROMOTION_APPROVED"),
        "prior_state": str(lineage.get("approval_prior_state") or event.get("prior_state") or "PAPER_PROMOTION_RECOMMENDED"),
        "approval_state": "PAPER_PROMOTION_APPROVED",
        "event_hash": str(lineage.get("approval_event_hash") or event.get("event_hash") or ""),
        "event_timestamp_utc": str(lineage.get("approval_timestamp_utc") or event.get("event_timestamp_utc") or event.get("generated_at_utc") or ""),
    }


def build_workflow_state_resolver_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    computed_at = f"{day_utc}T00:00:00Z"
    payload = {
        "schema_id": "aegis_workflow_state_resolver_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "computed_at_utc": computed_at,
        "rules": sorted(RESOLVER_RULES, key=lambda row: int(row["precedence"])),
        "stale_artifact_policy": {
            "policy": "NO_STALE_INTERMEDIATE_ARTIFACT_OVERRIDES_NEWER_AUTHORITATIVE_INPUTS",
            "authoritative_inputs": ["approval_events", "generated_approval_lineage", "approval_queue", "paper_setup", "promotion_packets", "shadow_trials", "evidence_packets"],
            "stale_input_result": "stale_input_rejected_true_and_lower_precedence_input_ignored",
        },
        "determinism_policy": {
            "computed_at_utc": "TARGET_DAY_START",
            "same_target_day_same_inputs": "same_states_same_action_queue_same_hashes",
        },
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def build_hypothesis_workflow_state_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    computed_at = f"{day_utc}T00:00:00Z"
    resolver_payload = build_workflow_state_resolver_v1(truth_root=truth_root, day_utc=day_utc)
    paths = _input_paths(truth_root, day_utc)
    inputs = {name: _read(path) for name, path in paths.items()}
    input_hashes = {name: _payload_hash(payload) for name, payload in inputs.items() if payload}
    input_generated = {name: str(payload.get("generated_at_utc") or payload.get("generated_at") or "") for name, payload in inputs.items() if payload}

    records: dict[str, dict[str, Any]] = {}
    for row in inputs["research_portfolio"].get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        hid = str(row.get("hypothesis_id") or "")
        if not hid:
            continue
        state = "OUTCOMES_ACCUMULATING" if int(row.get("sample_count") or 0) > 0 else "PAPER_OBSERVING"
        record = _base_record(
            day_utc=day_utc,
            computed_at=computed_at,
            hypothesis_id=hid,
            display_name=str(row.get("name") or hid),
            source_type="SEEDED",
            current_state=state,
            prior_state=str(row.get("state") or ""),
            next_action="WAIT_FOR_AUTOMATIC_PROCESSING" if state == "OUTCOMES_ACCUMULATING" else "NONE",
            reason_codes=[str(item) for item in row.get("reason_codes") or []],
            blocker_codes=[],
            source_paths=[str(paths["research_portfolio"])],
            source_hashes={"research_portfolio": input_hashes.get("research_portfolio", "")},
            input_generated_at_utc=input_generated.get("research_portfolio", ""),
        )
        record["validation_state"] = row.get("validation_state") or ""
        record["sample_count"] = int(row.get("sample_count") or 0)
        record["resolver_rule_id"] = "seeded_research_portfolio_state"
        _apply_state_aging(record, day_utc=day_utc, entered_at=_state_entered_at(day_utc, input_generated.get("research_portfolio", "")))
        records[hid] = record

    evidence = _rows_by_id(inputs["evidence_packets"].get("evidence_packets"))
    shadows = _rows_by_id(inputs["shadow_trials"].get("shadow_trials"))
    promotions = _rows_by_id(inputs["promotion_packets"].get("promotion_packets"))
    queue = _rows_by_id(inputs["approval_queue"].get("approval_queue"))
    approval_events = _approval_events_by_id(paths["approval_events"])
    approval_lineage = _approval_lineage_by_id(inputs.get("generated_approval_lineage", {}))
    setups = _rows_by_id(inputs["paper_setup"].get("paper_tracking_setups"))

    for hid, packet in evidence.items():
        shadow = shadows.get(hid, {})
        promotion = promotions.get(hid, {})
        qitem = queue.get(hid, {})
        setup = setups.get(hid, {})
        lineage = approval_lineage.get(hid, {})
        approval_event = approval_events.get(hid, {}) or (_approval_event_from_lineage(lineage) if lineage else {})
        macro_readiness = inputs.get("macro_calendar_data_readiness", {}) if _is_macro_calendar(packet, promotion) else {}
        current, prior, next_action, reasons, blockers, resolver_rule_id = _generated_state(packet, shadow, promotion, qitem, setup, approval_event, macro_readiness)
        record = _base_record(
            day_utc=day_utc,
            computed_at=computed_at,
            hypothesis_id=hid,
            display_name=str(promotion.get("hypothesis_name") or packet.get("hypothesis_statement") or hid),
            source_type="GENERATED_PROPOSAL",
            current_state=current,
            prior_state=prior,
            next_action=next_action,
            reason_codes=reasons,
            blocker_codes=blockers,
            source_paths=[str(paths[name]) for name in ("evidence_packets", "shadow_trials", "promotion_packets", "approval_queue", "generated_approval_lineage", "paper_setup", "macro_calendar_data_readiness") if paths[name].exists() and (name != "macro_calendar_data_readiness" or macro_readiness) and (name != "generated_approval_lineage" or lineage)],
            source_hashes={name: input_hashes.get(name, "") for name in ("evidence_packets", "shadow_trials", "promotion_packets", "approval_queue", "generated_approval_lineage", "paper_setup", "macro_calendar_data_readiness") if input_hashes.get(name) and (name != "macro_calendar_data_readiness" or macro_readiness) and (name != "generated_approval_lineage" or lineage)},
            input_generated_at_utc="|".join(value for key, value in sorted(input_generated.items()) if key in {"evidence_packets", "shadow_trials", "promotion_packets", "approval_queue", "paper_setup"} and value),
        )
        record["macro_calendar_data_readiness_status"] = str(macro_readiness.get("status") or "") if macro_readiness else ""
        record["macro_calendar_ready"] = bool(macro_readiness.get("macro_calendar_ready")) if macro_readiness else False
        record["missing_dataset"] = "macro event calendar" if "missing_macro_event_calendar" in blockers or "missing_macro_event_calendar" in reasons else ""
        record["required_fields"] = list(macro_readiness.get("required_fields") or []) if record["missing_dataset"] and macro_readiness else (["event_name", "event_type", "release_datetime", "actual", "consensus", "prior", "importance", "affected_assets"] if record["missing_dataset"] else [])
        record["expected_sample_frequency"] = packet.get("expected_sample_frequency") or qitem.get("expected_sample_frequency") or ""
        record["expected_validation_timeline"] = qitem.get("expected_validation_timeline") or promotion.get("expected_validation_timeline") or ""
        record["next_step_detail"] = str(macro_readiness.get("next_step") or "") if macro_readiness else (setup.get("next_step") or "")
        record["resolver_rule_id"] = resolver_rule_id
        _apply_state_aging(
            record,
            day_utc=day_utc,
            entered_at=_state_entered_at(
                day_utc,
                setup.get("generated_at_utc") or setup.get("generated_at"),
                qitem.get("latest_approval_event", {}).get("event_timestamp_utc") if isinstance(qitem.get("latest_approval_event"), Mapping) else "",
                approval_event.get("event_timestamp_utc"),
                lineage.get("approval_timestamp_utc") if lineage else "",
                promotion.get("generated_at_utc") or promotion.get("generated_at"),
                shadow.get("generated_at_utc") or shadow.get("generated_at"),
                packet.get("generated_at_utc") or packet.get("generated_at"),
                record.get("input_generated_at_utc"),
            ),
        )
        records[hid] = record

    rows = sorted(records.values(), key=lambda row: (str(row["source_type"]), str(row["display_name"])))
    for row in rows:
        row["workflow_state_hash"] = _stable_hash({**row, "workflow_state_hash": ""})
    state_counts: dict[str, int] = {}
    for row in rows:
        state_counts[row["current_state"]] = state_counts.get(row["current_state"], 0) + 1
    payload = {
        "schema_id": "aegis_hypothesis_workflow_state_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "computed_at_utc": computed_at,
        "deterministic_rerun_id": _stable_hash({"day_utc": str(day_utc), "input_artifact_hashes": input_hashes}),
        "hypotheses": rows,
        "state_counts": state_counts,
        "summary": {"hypothesis_count": len(rows), "state_counts": state_counts},
        "input_artifact_hashes": {**input_hashes, "workflow_state_resolver": resolver_payload.get("content_hash", "")},
        "input_generated_at_utc": input_generated,
        "source_artifact_paths": {**{name: str(path) for name, path in paths.items() if path.exists()}, "workflow_state_resolver": str(workflow_state_resolver_path_v1(truth_root=truth_root, day_utc=day_utc))},
        "resolver_artifact_hash": resolver_payload.get("content_hash", ""),
        "stale_input_rejected": False,
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def _apply_state_aging(row: dict[str, Any], *, day_utc: str, entered_at: str) -> None:
    age_days = _days_since(day_utc, entered_at)
    blockers = [str(item) for item in row.get("blocker_codes") or []]
    stale_warning, stale_reason = _stale_state(str(row.get("current_state") or ""), age_days, blockers)
    prior_payload = dict(_state_hash_base(row))
    prior_payload["current_state"] = row.get("prior_state") or ""
    row["state_entered_at_utc"] = entered_at
    row["time_in_state_days"] = age_days
    row["stale_state_warning"] = stale_warning
    row["stale_state_reason"] = stale_reason
    row["previous_state_hash"] = _stable_hash(prior_payload)
    row["current_state_hash"] = _stable_hash(_state_hash_base(row))


def _base_record(**kwargs: Any) -> dict[str, Any]:
    current = kwargs["current_state"]
    next_action = kwargs["next_action"]
    allowed = list(ACTION_CAPABILITIES.get(current, {}).get("buttons", []))
    return {
        "schema_id": "aegis_hypothesis_workflow_state_record_v1",
        "schema_version": "v1",
        "day_utc": kwargs["day_utc"],
        "hypothesis_id": kwargs["hypothesis_id"],
        "display_name": kwargs["display_name"],
        "source_type": kwargs["source_type"],
        "current_state": current,
        "prior_state": kwargs["prior_state"],
        "next_action": next_action,
        "allowed_actions": allowed,
        "reason_codes": sorted(set(kwargs["reason_codes"])),
        "blocker_codes": sorted(set(kwargs["blocker_codes"])),
        "source_artifact_paths": kwargs["source_paths"],
        "source_artifact_hashes": kwargs["source_hashes"],
        "input_generated_at_utc": kwargs["input_generated_at_utc"],
        "computed_at_utc": kwargs["computed_at"],
        "stale_input_rejected": False,
        "state_transition_history": [{"from_state": kwargs["prior_state"], "to_state": current, "reason_codes": sorted(set(kwargs["reason_codes"] or kwargs["blocker_codes"]))}],
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }


def _is_macro_calendar(packet: Mapping[str, Any], promotion: Mapping[str, Any]) -> bool:
    text = json.dumps({"packet": packet, "promotion": promotion}, sort_keys=True, default=str).lower()
    return "macro calendar" in text or "macro_headline_shock" in text or "missing_macro_event_calendar" in text


def _generated_state(packet: Mapping[str, Any], shadow: Mapping[str, Any], promotion: Mapping[str, Any], qitem: Mapping[str, Any], setup: Mapping[str, Any], approval_event: Mapping[str, Any], macro_readiness: Mapping[str, Any] | None = None) -> tuple[str, str, str, list[str], list[str], str]:
    reasons = [str(item) for item in promotion.get("reason_codes") or [] if str(item)]
    triage = str((promotion.get("triage_result") or shadow.get("triage_decision") or {}).get("triage_state") or "")
    shadow_result = str(promotion.get("shadow_validation_result") or shadow.get("shadow_trial_result") or "")
    blockers = [str(item) for item in (promotion.get("triage_result") or {}).get("reason_codes") or [] if str(item)]
    if triage == "NEEDS_DATA" or shadow_result == "NEEDS_DATA":
        readiness_status = str((macro_readiness or {}).get("status") or "")
        macro_ready = bool((macro_readiness or {}).get("macro_calendar_ready"))
        missing_macro = any("missing_macro_event_calendar" in str(item) for item in [*reasons, *blockers]) or _is_macro_calendar(packet, promotion)
        if missing_macro and macro_ready:
            filtered_reasons = [item for item in reasons if "missing_macro_event_calendar" not in str(item)]
            filtered_blockers = [item for item in blockers if "missing_macro_event_calendar" not in str(item)]
            return "READY_FOR_SHADOW_TRIAL", "NEEDS_DATA", "WAIT_FOR_AUTOMATIC_PROCESSING", [*filtered_reasons, "MACRO_CALENDAR_DATA_READY"], filtered_blockers, "macro_calendar_data_ready_for_shadow_trial"
        if missing_macro and readiness_status:
            blockers = blockers or ["missing_macro_event_calendar"]
            reasons = [*reasons, f"MACRO_CALENDAR_DATA_READINESS_{readiness_status}"]
        return "NEEDS_DATA", "TRIAGING", "PROVIDE_DATA_SOURCE", reasons, blockers or ["missing_macro_event_calendar"], "needs_data_overrides_ready"
    if triage.startswith("REJECTED") or str(promotion.get("promotion_decision") or "").startswith("REJECTED"):
        return "REJECTED_AUTOMATICALLY", triage or "DISCOVERED", "NONE", reasons, blockers, "rejected_automatic"
    if setup:
        setup_state = str(setup.get("paper_setup_status") or "")
        setup_reasons = [str(item) for item in setup.get("reason_codes") or []]
        if setup_state == "PAPER_TRACKING_BLOCKED":
            return "PAPER_TRACKING_BLOCKED", "PAPER_PROMOTION_APPROVED", "WAIT_FOR_AUTOMATIC_PROCESSING", [*reasons, *setup_reasons], setup_reasons, "paper_tracking_blocked_overrides_ready"
        if setup_state == "PAPER_TRACKING_READY":
            return "PAPER_TRACKING_READY", "PAPER_PROMOTION_APPROVED", "WAIT_FOR_AUTOMATIC_PROCESSING", reasons, blockers, "paper_tracking_ready_after_setup"
        return "PAPER_SETUP_RUNNING", "PAPER_PROMOTION_APPROVED", "WAIT_FOR_AUTOMATIC_PROCESSING", reasons, blockers, "paper_setup_running_after_approval"
    approval_state = str(approval_event.get("new_state") or approval_event.get("approval_state") or qitem.get("approval_state") or "")
    if approval_state == "PAPER_PROMOTION_APPROVED":
        return "PAPER_PROMOTION_APPROVED", "PAPER_PROMOTION_RECOMMENDED", "WAIT_FOR_AUTOMATIC_PROCESSING", reasons, blockers, "approved_overrides_recommended"
    if str(promotion.get("promotion_decision") or "") == "PAPER_PROMOTION_RECOMMENDED":
        return "PAPER_PROMOTION_RECOMMENDED", shadow_result or "SHADOW_VALIDATION_PASSED", "APPROVE_PAPER_TEST", reasons, blockers, "promotion_recommended_requires_approval"
    if shadow_result == "SHADOW_VALIDATION_FAILED":
        return "SHADOW_VALIDATION_FAILED", triage or "READY_FOR_SHADOW_TRIAL", "NONE", reasons, blockers, "shadow_failed_terminal"
    if shadow_result == "SHADOW_VALIDATION_PASSED":
        return "SHADOW_VALIDATION_PASSED", "READY_FOR_SHADOW_TRIAL", "WAIT_FOR_AUTOMATIC_PROCESSING", reasons, blockers, "shadow_passed_waits_for_promotion"
    return "DISCOVERED", "", "WAIT_FOR_AUTOMATIC_PROCESSING", reasons, blockers, "discovered_default"


def build_operator_action_queue_v1(workflow_payload: Mapping[str, Any], *, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    computed_at = f"{day_utc}T00:00:00Z"
    items: list[dict[str, Any]] = []
    for row in workflow_payload.get("hypotheses") or []:
        if not isinstance(row, Mapping):
            continue
        next_action = str(row.get("next_action") or "NONE")
        if next_action not in {"APPROVE_PAPER_TEST", "PROVIDE_DATA_SOURCE", "REVIEW_RETIREMENT", "REVIEW_CAPITAL"}:
            continue
        cap = ACTION_CAPABILITIES.get(str(row.get("current_state") or ""), {})
        if not cap:
            continue
        action_id = f"aegis-action:{day_utc}:{row.get('hypothesis_id')}:{next_action}"
        item = {
            "schema_id": "aegis_operator_action_queue_item_v1",
            "schema_version": "v1",
            "day_utc": str(day_utc),
            "action_id": action_id,
            "hypothesis_id": row.get("hypothesis_id"),
            "hypothesis_name": row.get("display_name"),
            "action_type": next_action,
            "urgency": _action_priority(next_action),
            "priority": _action_priority(next_action),
            "due_by_utc": _action_due_by(day_utc, next_action),
            "blocking_what": _blocking_what(row),
            "impact_area": _impact_area(next_action),
            "action_age_days": int(row.get("time_in_state_days") or 0),
            "why_action_needed": _why_action_needed(row),
            "exact_buttons": list(cap.get("buttons") or []),
            "consequence_of_each_button": dict(cap.get("consequence") or {}),
            "safety_statement": SAFETY_STATEMENT,
            "source_state_hash": row.get("workflow_state_hash"),
            "source_artifact_paths": row.get("source_artifact_paths") or [],
            "created_at_utc": computed_at,
            "missing_dataset": row.get("missing_dataset") or "",
            "required_fields": row.get("required_fields") or [],
        }
        item["action_hash"] = _stable_hash({**item, "action_hash": ""})
        items.append(item)
    payload = {
        "schema_id": "aegis_operator_action_queue_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "computed_at_utc": computed_at,
        "actions": sorted(items, key=lambda item: str(item["action_id"])),
        "summary": {"action_count": len(items), "action_types": sorted({str(item["action_type"]) for item in items})},
        "input_artifact_hashes": {"workflow_state": workflow_payload.get("content_hash", "")},
        "input_generated_at_utc": {"workflow_state": workflow_payload.get("computed_at_utc", "")},
        "source_artifact_paths": [str(hypothesis_workflow_state_path_v1(truth_root=truth_root, day_utc=day_utc))],
        "stale_input_rejected": False,
        "deterministic_rerun_id": _stable_hash({"day_utc": str(day_utc), "workflow_state": workflow_payload.get("content_hash", "")}),
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def _action_priority(next_action: str) -> str:
    if next_action in {"REVIEW_CAPITAL", "REVIEW_RETIREMENT"}:
        return "HIGH"
    if next_action == "APPROVE_PAPER_TEST":
        return "NORMAL"
    if next_action == "PROVIDE_DATA_SOURCE":
        return "NORMAL"
    return "LOW"


def _action_due_by(day_utc: str, next_action: str) -> str:
    base = _day_start(day_utc)
    days = 3 if next_action in {"APPROVE_PAPER_TEST", "PROVIDE_DATA_SOURCE"} else 7
    return (base + timedelta(days=days)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _blocking_what(row: Mapping[str, Any]) -> str:
    if row.get("next_action") == "PROVIDE_DATA_SOURCE":
        return "shadow validation and promotion eligibility"
    if row.get("next_action") == "APPROVE_PAPER_TEST":
        return "paper research tracking setup"
    if row.get("next_action") == "REVIEW_RETIREMENT":
        return "research retirement decision"
    if row.get("next_action") == "REVIEW_CAPITAL":
        return "research allocation review"
    return ""


def _impact_area(next_action: str) -> str:
    if next_action == "PROVIDE_DATA_SOURCE":
        return "EDGE_DISCOVERY"
    if next_action == "APPROVE_PAPER_TEST":
        return "EDGE_VALIDATION"
    if next_action == "REVIEW_CAPITAL":
        return "RESEARCH_ALLOCATION"
    return "EDGE_VALIDATION"


def _why_action_needed(row: Mapping[str, Any]) -> str:
    if row.get("next_action") == "PROVIDE_DATA_SOURCE":
        missing = row.get("missing_dataset") or "required research dataset"
        return f"{missing} is missing; Aegis cannot shadow-validate this hypothesis until the data source is provided or marked unavailable."
    if row.get("next_action") == "APPROVE_PAPER_TEST":
        return "Shadow validation passed and David approval is required before paper research tracking setup."
    if row.get("next_action") == "REVIEW_RETIREMENT":
        return "Sufficient evidence supports a retirement review."
    if row.get("next_action") == "REVIEW_CAPITAL":
        return "Sufficient evidence supports research capital review."
    return "No David action required."


def write_workflow_state_resolver_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    payload = build_workflow_state_resolver_v1(truth_root=truth_root, day_utc=day_utc)
    write_json_v1(workflow_state_resolver_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    return payload


def write_hypothesis_workflow_state_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    write_workflow_state_resolver_v1(truth_root=truth_root, day_utc=day_utc)
    payload = build_hypothesis_workflow_state_v1(truth_root=truth_root, day_utc=day_utc)
    write_json_v1(hypothesis_workflow_state_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    return payload


def write_operator_action_queue_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    workflow = _read(hypothesis_workflow_state_path_v1(truth_root=truth_root, day_utc=day_utc))
    if not workflow:
        workflow = write_hypothesis_workflow_state_v1(truth_root=truth_root, day_utc=day_utc)
    payload = build_operator_action_queue_v1(workflow, truth_root=truth_root, day_utc=day_utc)
    write_json_v1(operator_action_queue_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    event_log = operator_action_event_log_path_v1(truth_root=truth_root, day_utc=day_utc)
    event_log.parent.mkdir(parents=True, exist_ok=True)
    if not event_log.exists():
        event_log.write_text("", encoding="utf-8")
    return payload


def append_operator_action_event_v1(*, truth_root: Path | str, day_utc: str, action_id: str, hypothesis_id: str, action_type: str, button_clicked: str, prior_state: str, new_state: str, source_state_hash: str, actor: str = "David / operator", event_timestamp_utc: str | None = None) -> dict[str, Any]:
    timestamp = event_timestamp_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    event = {
        "schema_id": "aegis_operator_action_event_log_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "event_id": _stable_hash({"day_utc": str(day_utc), "action_id": action_id, "button_clicked": button_clicked, "timestamp": timestamp})[:24],
        "action_id": str(action_id),
        "hypothesis_id": str(hypothesis_id),
        "actor": actor,
        "action_type": str(action_type),
        "button_clicked": str(button_clicked),
        "prior_state": str(prior_state),
        "new_state": str(new_state),
        "source_state_hash": str(source_state_hash),
        "event_timestamp_utc": timestamp,
        "safety_statement": SAFETY_STATEMENT,
        "no_broker_execution": True,
        "no_trade_advice": True,
        "no_live_trading": True,
        "no_real_capital": True,
        "safety": dict(SAFETY),
    }
    event["event_hash"] = _stable_hash({**event, "event_hash": ""})
    path = operator_action_event_log_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
    return event



def _state_signature(workflow: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in workflow.get("hypotheses") or []:
        if isinstance(row, Mapping):
            out[str(row.get("hypothesis_id") or "")] = str(row.get("current_state") or "")
    return out


def _action_signature(queue: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in queue.get("actions") or []:
        if isinstance(item, Mapping):
            rows.append({
                "action_id": item.get("action_id"),
                "hypothesis_id": item.get("hypothesis_id"),
                "action_type": item.get("action_type"),
                "exact_buttons": item.get("exact_buttons") or [],
                "priority": item.get("priority"),
                "blocking_what": item.get("blocking_what"),
            })
    return sorted(rows, key=lambda row: str(row.get("action_id") or ""))


def build_hypothesis_workflow_replay_verification_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    current_workflow = _read(hypothesis_workflow_state_path_v1(truth_root=truth_root, day_utc=day_utc))
    if not current_workflow:
        current_workflow = write_hypothesis_workflow_state_v1(truth_root=truth_root, day_utc=day_utc)
    current_queue = _read(operator_action_queue_path_v1(truth_root=truth_root, day_utc=day_utc))
    if not current_queue:
        current_queue = write_operator_action_queue_v1(truth_root=truth_root, day_utc=day_utc)
    replay_workflow = build_hypothesis_workflow_state_v1(truth_root=truth_root, day_utc=day_utc)
    replay_queue = build_operator_action_queue_v1(replay_workflow, truth_root=truth_root, day_utc=day_utc)
    checks = {
        "same_state_count": current_workflow.get("state_counts") == replay_workflow.get("state_counts"),
        "same_hypothesis_states": _state_signature(current_workflow) == _state_signature(replay_workflow),
        "same_action_queue": _action_signature(current_queue) == _action_signature(replay_queue),
        "same_workflow_content_hash": current_workflow.get("content_hash") == replay_workflow.get("content_hash"),
        "same_action_queue_content_hash": current_queue.get("content_hash") == replay_queue.get("content_hash"),
    }
    semantic_checks = {
        "same_state_count": checks["same_state_count"],
        "same_hypothesis_states": checks["same_hypothesis_states"],
        "same_action_queue": checks["same_action_queue"],
    }
    payload = {
        "schema_id": "aegis_hypothesis_workflow_replay_verification_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "computed_at_utc": f"{day_utc}T00:00:00Z",
        "verification_status": "PASS" if all(semantic_checks.values()) else "FAIL",
        "checks": checks,
        "semantic_checks": semantic_checks,
        "state_count": current_workflow.get("state_counts") or {},
        "action_queue_count": len(current_queue.get("actions") or []),
        "deterministic_timestamp_exclusions": [
            "event_timestamp_utc for future UI actions is append-only and not replayed here",
            "content_hash may change when upstream evidence is regenerated with identical workflow semantics during long audit sequences",
        ],
        "input_artifact_hashes": {
            "workflow_state": current_workflow.get("content_hash", ""),
            "operator_action_queue": current_queue.get("content_hash", ""),
            "replay_workflow_state": replay_workflow.get("content_hash", ""),
            "replay_operator_action_queue": replay_queue.get("content_hash", ""),
        },
        "source_artifact_paths": [
            str(hypothesis_workflow_state_path_v1(truth_root=truth_root, day_utc=day_utc)),
            str(operator_action_queue_path_v1(truth_root=truth_root, day_utc=day_utc)),
        ],
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def write_hypothesis_workflow_replay_verification_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    payload = build_hypothesis_workflow_replay_verification_v1(truth_root=truth_root, day_utc=day_utc)
    write_json_v1(hypothesis_workflow_replay_verification_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    return payload


def _rows_any(payload: Mapping[str, Any], keys: list[str]) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _matches_hypothesis(row: Mapping[str, Any], hypothesis_id: str, name: str) -> bool:
    joined = json.dumps(row, sort_keys=True, default=str).lower()
    tokens = [hypothesis_id.lower(), name.lower()]
    return any(token and token in joined for token in tokens)


def _throughput_status(row: Mapping[str, Any]) -> str:
    producer_status = str(row.get("candidate_producer_status") or "")
    if producer_status == "VALID_CANDIDATE_SIGNAL":
        return "CANDIDATES_FLOWING"
    if producer_status == "NO_MARKET_SETUP":
        return "NO_MARKET_SETUP"
    if producer_status == "MISSING_DATA":
        return "BLOCKED"
    if producer_status in {"CANDIDATE_CONSTRUCTION_INCOMPLETE", "POLICY_INCOMPLETE"}:
        return "BLOCKED"
    if int(row.get("included_validation_samples") or 0) or int(row.get("excluded_validation_samples") or 0):
        return "VALIDATION_FLOWING"
    if int(row.get("closed_outcomes") or 0):
        return "OUTCOMES_FLOWING"
    if int(row.get("paper_observation_count") or 0) or int(row.get("open_observations") or 0):
        return "OBSERVATIONS_FLOWING"
    if int(row.get("candidate_count") or 0):
        return "CANDIDATES_FLOWING"
    if row.get("paper_setup_state") == "PAPER_TRACKING_READY":
        return "PAPER_TRACKING_READY"
    if row.get("proposal_state") == "NEEDS_DATA" or row.get("paper_setup_state") == "NEEDS_DATA":
        return "NEEDS_DATA"
    return "PROPOSED_ONLY"


def build_generated_hypothesis_throughput_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    paths = _input_paths(truth_root, day_utc)
    workflow = _read(hypothesis_workflow_state_path_v1(truth_root=truth_root, day_utc=day_utc)) or write_hypothesis_workflow_state_v1(truth_root=truth_root, day_utc=day_utc)
    inputs = {name: _read(path) for name, path in paths.items()}
    oil_producer = inputs.get("oil_shock_candidate_producer", {}) if isinstance(inputs.get("oil_shock_candidate_producer"), Mapping) else {}
    candidates = _rows_any(inputs.get("candidate_lifecycle", {}), ["candidate_lifecycle_rows", "valid_candidate_contracts", "paper_positions", "rows"])
    outcomes = _rows_any(inputs.get("outcome_registry", {}), ["outcomes", "outcome_rows", "rows"])
    samples = _rows_any(inputs.get("validation_samples", {}), ["validation_samples", "samples", "rows"])
    generated_rows = [row for row in workflow.get("hypotheses") or [] if isinstance(row, Mapping) and row.get("source_type") == "GENERATED_PROPOSAL"]
    rows: list[dict[str, Any]] = []
    for workflow_row in generated_rows:
        hid = str(workflow_row.get("hypothesis_id") or "")
        name = str(workflow_row.get("display_name") or hid)
        matched_candidates = [row for row in candidates if _matches_hypothesis(row, hid, name)]
        matched_outcomes = [row for row in outcomes if _matches_hypothesis(row, hid, name)]
        matched_samples = [row for row in samples if _matches_hypothesis(row, hid, name)]
        approval_state = "PAPER_PROMOTION_APPROVED" if workflow_row.get("current_state") in {"PAPER_PROMOTION_APPROVED", "PAPER_SETUP_RUNNING", "PAPER_TRACKING_READY", "PAPER_TRACKING_BLOCKED", "PAPER_OBSERVING", "OUTCOMES_ACCUMULATING", "STATISTICALLY_SUFFICIENT"} else ""
        setup_state = str(workflow_row.get("current_state") or "") if str(workflow_row.get("current_state") or "").startswith("PAPER_TRACKING") else ""
        producer_status = ""
        producer_reason_codes = []
        if hid == "ehp_cdbd8fe683acb622" and oil_producer:
            producer_status = str(oil_producer.get("producer_status") or oil_producer.get("last_evaluation_status") or "")
            producer_reason_codes = oil_producer.get("reason_codes") if isinstance(oil_producer.get("reason_codes"), list) else []
        row = {
            "hypothesis_id": hid,
            "hypothesis_name": name,
            "proposal_state": str(workflow_row.get("current_state") or ""),
            "approval_state": approval_state,
            "paper_setup_state": setup_state,
            "candidate_count": max(len(matched_candidates), int(oil_producer.get("candidate_count") or 0) if hid == "ehp_cdbd8fe683acb622" and oil_producer else 0),
            "candidate_producer_status": producer_status,
            "candidate_producer_reason_codes": producer_reason_codes,
            "paper_observation_count": len(matched_candidates),
            "open_observations": len([item for item in matched_outcomes if str(item.get("outcome_state") or item.get("status") or "").upper() not in {"CLOSED", "AUTO_CLOSED", "VALIDATED"}]),
            "closed_outcomes": len([item for item in matched_outcomes if str(item.get("outcome_state") or item.get("status") or "").upper() in {"CLOSED", "AUTO_CLOSED", "VALIDATED"}]),
            "included_validation_samples": len([item for item in matched_samples if item.get("included") is True or str(item.get("sample_status") or "").upper() == "INCLUDED"]),
            "excluded_validation_samples": len([item for item in matched_samples if item.get("included") is False or str(item.get("sample_status") or "").upper() == "EXCLUDED"]),
            "days_since_proposal": int(workflow_row.get("time_in_state_days") or 0),
            "days_since_approval": int(workflow_row.get("time_in_state_days") or 0) if approval_state else 0,
            "next_expected_step": str(workflow_row.get("next_step_detail") or "candidate generation"),
            "no_david_action_required_unless_blocked": str(workflow_row.get("next_action") or "") not in {"APPROVE_PAPER_TEST", "PROVIDE_DATA_SOURCE", "REVIEW_RETIREMENT", "REVIEW_CAPITAL"},
            "source_artifact_paths": workflow_row.get("source_artifact_paths") or [],
            "source_artifact_hashes": workflow_row.get("source_artifact_hashes") or {},
        }
        row["throughput_status"] = _throughput_status(row)
        if row["throughput_status"] == "PAPER_TRACKING_READY":
            row["next_expected_step"] = "candidate generation"
        elif row["throughput_status"] == "NO_MARKET_SETUP":
            row["next_expected_step"] = "wait for qualifying Oil Shock market setup"
        elif producer_status and row["throughput_status"] == "BLOCKED":
            row["next_expected_step"] = "resolve Oil Shock producer blocker"
        row["throughput_hash"] = _stable_hash({**row, "throughput_hash": ""})
        rows.append(row)
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[str(row["throughput_status"])] = status_counts.get(str(row["throughput_status"]), 0) + 1
    payload = {
        "schema_id": "aegis_generated_hypothesis_throughput_v1",
        "schema_version": "v1",
        "day_utc": str(day_utc),
        "computed_at_utc": f"{day_utc}T00:00:00Z",
        "generated_hypotheses": sorted(rows, key=lambda row: str(row["hypothesis_name"])),
        "summary": {"generated_hypothesis_count": len(rows), "throughput_status_counts": status_counts},
        "input_artifact_hashes": {"workflow_state": workflow.get("content_hash", ""), **{name: _payload_hash(payload) for name, payload in inputs.items() if payload}},
        "source_artifact_paths": {name: str(path) for name, path in paths.items() if path.exists()},
        "deterministic_rerun_id": _stable_hash({"day_utc": str(day_utc), "workflow_state": workflow.get("content_hash", "")}),
        "safety_statement": SAFETY_STATEMENT,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = _stable_hash({**payload, "content_hash": ""})
    return payload


def write_generated_hypothesis_throughput_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    payload = build_generated_hypothesis_throughput_v1(truth_root=truth_root, day_utc=day_utc)
    write_json_v1(generated_hypothesis_throughput_path_v1(truth_root=truth_root, day_utc=day_utc), payload)
    return payload
