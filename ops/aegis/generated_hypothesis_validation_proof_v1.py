from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1

FAMILY = "aegis_generated_hypothesis_validation_proof_v1"
FILENAME = "generated_hypothesis_validation_proof.v1.json"
SCHEMA_ID = "aegis_generated_hypothesis_validation_proof"
SCHEMA_VERSION = "v1"
POLICY_VERSION = "aegis_generated_hypothesis_validation_proof_policy_v1"

STAGE_ORDER = [
    ("proposal_state", "PROPOSAL"),
    ("shadow_validation_state", "SHADOW_VALIDATION"),
    ("approval_state", "APPROVAL"),
    ("paper_tracking_setup_state", "PAPER_TRACKING_READY"),
    ("candidate_flow_state", "CANDIDATE_FLOW"),
    ("paper_observation_flow_state", "PAPER_OBSERVATION_FLOW"),
    ("outcome_flow_state", "OUTCOME_FLOW"),
    ("validation_sample_flow_state", "VALIDATION_SAMPLE_FLOW"),
]

ACTIVE_STATES = {"READY", "FLOWING", "COMPLETE"}

SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_forced_candidates": True,
    "no_forced_paper_observations": True,
    "no_candidate_mutation": True,
    "no_paper_lifecycle_mutation": True,
    "no_outcome_mutation": True,
    "no_validation_mutation": True,
    "no_allocation_mutation": True,
    "no_workflow_state_mutation": True,
    "safety_gates_changed": False,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def generated_hypothesis_validation_proof_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / FAMILY / day_utc / FILENAME


def build_generated_hypothesis_validation_proof_v1(*, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    computed_at = computed_at_utc or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    paths = _input_paths(root, day_utc)
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    rows = _proof_rows(payloads, paths, computed_at)
    summary = _summary(rows)
    body: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day_utc,
        "target_day": day_utc,
        "computed_at_utc": computed_at,
        "hypothesis_rows": rows,
        "summary": summary,
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "source_artifact_hashes": {key: _sha(path) for key, path in sorted(paths.items())},
        "safety_statement": "Generated hypothesis validation proof is read-only. It does not force candidates, paper observations, outcomes, validation samples, trading, broker actions, allocation, or safety gates.",
        **SAFETY,
    }
    body["content_hash"] = _stable_hash({k: v for k, v in body.items() if k not in {"computed_at_utc", "content_hash"}})
    return body


def write_generated_hypothesis_validation_proof_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_generated_hypothesis_validation_proof_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(generated_hypothesis_validation_proof_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    specs = {
        "generated_throughput": ("aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"),
        "data_action_routing": ("aegis_data_action_routing_v1", "data_action_routing.v1.json"),
        "oil_shock_candidate_flow": ("aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"),
        "candidate_to_paper_proof": ("aegis_generated_hypothesis_candidate_to_paper_v1", "generated_hypothesis_candidate_to_paper.v1.json"),
        "paper_observation_to_outcome_proof": ("aegis_generated_hypothesis_paper_observation_to_outcome_v1", "generated_hypothesis_paper_observation_to_outcome.v1.json"),
        "paper_construction_repair_proof": ("aegis_generated_hypothesis_paper_construction_repair_v1", "generated_hypothesis_paper_construction_repair.v1.json"),
        "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        "candidate_lifecycle": ("aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"),
        "outcome_registry": ("aegis_outcome_registry_v1", "outcome_registry.v1.json"),
        "validation_samples": ("aegis_validation_samples_v1", "validation_samples.v1.json"),
        "macro_calendar_data_readiness": ("aegis_macro_calendar_data_readiness_v1", "macro_calendar_data_readiness.v1.json"),
    }
    out: dict[str, Path] = {}
    for key, (family, filename) in specs.items():
        path, _payload = latest_json_v1(root, family, day, filename)
        out[key] = path or root / "reports" / family / day / filename
    return out


def _proof_rows(payloads: Mapping[str, Mapping[str, Any]], paths: Mapping[str, Path], computed_at: str) -> list[dict[str, Any]]:
    throughput = payloads.get("generated_throughput", {})
    routing_by_id = {str(row.get("hypothesis_id") or "").lower(): row for row in _list(payloads.get("data_action_routing", {}).get("routing_rows")) if isinstance(row, Mapping)}
    oil = _oil_row(payloads.get("oil_shock_candidate_flow", {}))
    candidate_to_paper = _oil_row(payloads.get("candidate_to_paper_proof", {}))
    paper_to_outcome = _oil_row(payloads.get("paper_observation_to_outcome_proof", {}))
    paper_repair = _oil_row(payloads.get("paper_construction_repair_proof", {}))
    rows = []
    for item in _list(throughput.get("generated_hypotheses")):
        if not isinstance(item, Mapping):
            continue
        hid = str(item.get("hypothesis_id") or "")
        route = routing_by_id.get(hid.lower(), {})
        is_oil = _is_oil(item, oil)
        if is_oil and oil:
            route = route or routing_by_id.get(str(oil.get("hypothesis_id") or "").lower(), {})
        row = _row_for_hypothesis(item, route if isinstance(route, Mapping) else {}, oil if is_oil else {}, candidate_to_paper if is_oil else {}, paper_to_outcome if is_oil else {}, paper_repair if is_oil else {}, paths, computed_at)
        rows.append(row)
    return rows


def _row_for_hypothesis(item: Mapping[str, Any], route: Mapping[str, Any], oil: Mapping[str, Any], candidate_to_paper: Mapping[str, Any], paper_to_outcome: Mapping[str, Any], paper_repair: Mapping[str, Any], paths: Mapping[str, Path], computed_at: str) -> dict[str, Any]:
    hid = str(oil.get("hypothesis_id") or item.get("hypothesis_id") or "")
    name = str(oil.get("hypothesis_name") or item.get("hypothesis_name") or hid)
    owner = _owner(route, oil)
    david_action = bool(route.get("david_action_required") if route else oil.get("david_action_required"))
    next_step = _next_step(route, oil, item)
    candidate_count = _int(oil.get("candidate_count") if oil else item.get("candidate_count"))
    observation_count = _int(oil.get("paper_observation_count") if oil else item.get("paper_observation_count") or item.get("open_observations"))
    if candidate_to_paper:
        observation_count = 1 if candidate_to_paper.get("paper_observation_created") is True else 0
    closed_count = _int(item.get("closed_outcomes"))
    if paper_to_outcome and paper_to_outcome.get("outcome_row_created") is True:
        closed_count = max(closed_count, 1)
    sample_count = _int(item.get("included_validation_samples"))
    if paper_repair and paper_repair.get("paper_observation_found") is True:
        observation_count = max(observation_count, 1)
    if paper_to_outcome:
        if paper_to_outcome.get("paper_observation_found") is True:
            observation_count = max(observation_count, 1)
        if paper_to_outcome.get("outcome_row_created") is True or str(paper_to_outcome.get("outcome_status") or "") == "OUTCOME_CREATED":
            closed_count = max(closed_count, 1)
        if str(paper_to_outcome.get("validation_sample_status") or "") in {"INCLUDED", "EXCLUDED"}:
            sample_count = max(sample_count, 1)

    proposal_state = "NEEDS_DATA" if _upper(item.get("throughput_status")) == "NEEDS_DATA" else "COMPLETE"
    shadow_state = "NEEDS_DATA" if owner == "DAVID" and _upper(item.get("throughput_status")) == "NEEDS_DATA" else ("COMPLETE" if _upper(item.get("approval_state")) else "NOT_STARTED")
    approval_state = "COMPLETE" if "APPROVED" in _upper(item.get("approval_state")) else ("NOT_STARTED" if proposal_state != "NEEDS_DATA" else "NEEDS_DATA")
    paper_setup_state = "READY" if _upper(oil.get("paper_setup_status") or item.get("paper_setup_state")) == "PAPER_TRACKING_READY" else ("NOT_STARTED" if approval_state != "NEEDS_DATA" else "NEEDS_DATA")

    candidate_state = _candidate_flow_state(candidate_count, route, oil)
    paper_observation_state = "FLOWING" if observation_count > 0 else "NOT_STARTED"
    outcome_state = "FLOWING" if closed_count > 0 else "NOT_STARTED"
    if paper_to_outcome and paper_to_outcome.get("paper_observation_found") is True and paper_to_outcome.get("outcome_row_created") is not True:
        outcome_state = "BLOCKED" if paper_to_outcome.get("remaining_blocker") not in {"", "NONE", None} else "NOT_STARTED"
    validation_state = "FLOWING" if sample_count > 0 else "NOT_STARTED"

    states = {
        "proposal_state": proposal_state,
        "shadow_validation_state": shadow_state,
        "approval_state": approval_state,
        "paper_tracking_setup_state": paper_setup_state,
        "candidate_flow_state": candidate_state,
        "paper_observation_flow_state": paper_observation_state,
        "outcome_flow_state": outcome_state,
        "validation_sample_flow_state": validation_state,
    }
    stop_stage, stop_reason = _stop(states, owner, route, oil, item)
    if candidate_to_paper and states.get("paper_observation_flow_state") != "FLOWING" and candidate_to_paper.get("current_stop_stage"):
        stop_stage = str(candidate_to_paper.get("current_stop_stage") or stop_stage)
        stop_reason = str(candidate_to_paper.get("current_stop_reason") or stop_reason)
    if paper_to_outcome and paper_to_outcome.get("remaining_blocker") not in {"", "NONE", None}:
        stop_stage = "paper observation to outcome"
        stop_reason = str(paper_to_outcome.get("blocker_reason") or paper_to_outcome.get("remaining_blocker") or stop_reason)
    source_paths = _source_paths(item, route, oil, paths)
    if paper_repair:
        repair_path = str(paths.get("paper_construction_repair_proof") or "")
        if repair_path and repair_path not in source_paths:
            source_paths.append(repair_path)
    if candidate_to_paper:
        proof_path = str(paths.get("candidate_to_paper_proof") or "")
        if proof_path and proof_path not in source_paths:
            source_paths.append(proof_path)
        nested_paths = candidate_to_paper.get("source_artifact_paths")
        if isinstance(nested_paths, Mapping):
            for path in nested_paths.values():
                if str(path) and str(path) not in source_paths:
                    source_paths.append(str(path))
    return {
        "hypothesis_id": hid,
        "hypothesis_name": name,
        **states,
        "current_stop_stage": stop_stage,
        "current_stop_reason": stop_reason,
        "blocker_owner": owner,
        "david_action_required": david_action,
        "next_expected_step": next_step,
        "candidate_to_paper_status": str(candidate_to_paper.get("candidate_to_paper_status") or ""),
        "candidate_contract_status": str(candidate_to_paper.get("candidate_contract_status") or ""),
        "entry_reference_price_status": str(candidate_to_paper.get("entry_reference_price_status") or ""),
        "paper_construction_status": str(candidate_to_paper.get("paper_construction_status") or ""),
        "auto_promotion_status": str(candidate_to_paper.get("auto_promotion_status") or ""),
        "paper_observation_created": bool(candidate_to_paper.get("paper_observation_created") or paper_repair.get("paper_observation_found")) if candidate_to_paper or paper_repair else observation_count > 0,
        "outcome_row_created": bool(paper_to_outcome.get("outcome_row_created")) if paper_to_outcome else closed_count > 0,
        "paper_observation_to_outcome_status": str(paper_to_outcome.get("outcome_status") or ""),
        "paper_observation_to_outcome_readiness_status": str(paper_to_outcome.get("outcome_readiness_status") or ""),
        "paper_observation_to_outcome_blocker_code": str(paper_to_outcome.get("blocker_code") or ""),
        "validation_sample_status": str(paper_to_outcome.get("validation_sample_status") or ""),
        "research_quality_consumed": bool(paper_to_outcome.get("research_quality_consumed")) if paper_to_outcome else False,
        "paper_observation_to_outcome_remaining_blocker": str(paper_to_outcome.get("remaining_blocker") or ""),
        "source_artifact_paths": source_paths,
        "source_artifact_hashes": {path: _sha(path) for path in source_paths},
        "computed_at_utc": computed_at,
    }


def _candidate_flow_state(candidate_count: int, route: Mapping[str, Any], oil: Mapping[str, Any]) -> str:
    if candidate_count > 0:
        return "FLOWING"
    oil_status = _upper(oil.get("candidate_flow_status"))
    if oil_status == "CANDIDATE_FLOW_STARTED" or oil.get("candidate_flow_started") is True:
        return "FLOWING"
    owner = _owner(route, oil)
    if owner == "MARKET_CONDITIONS":
        return "WAITING_FOR_MARKET_CONDITIONS"
    if oil_status == "BLOCKED":
        return "BLOCKED"
    if owner in {"DAVID", "AEGIS_SYSTEM"}:
        return "NEEDS_DATA" if owner == "DAVID" else "BLOCKED"
    return "NOT_STARTED"


def _stop(states: Mapping[str, str], owner: str, route: Mapping[str, Any], oil: Mapping[str, Any], item: Mapping[str, Any]) -> tuple[str, str]:
    if owner == "DAVID":
        return "data readiness / shadow validation", str(route.get("missing_data_description") or "Operator-provided data is required.")
    if states.get("candidate_flow_state") in {"WAITING_FOR_MARKET_CONDITIONS", "BLOCKED", "NEEDS_DATA"}:
        reason = str(route.get("missing_data_description") or oil.get("ui_message") or oil.get("reason_no_candidates_generated_yet") or item.get("candidate_producer_status") or "Candidate flow has not started.")
        return "candidate flow", reason
    for key, label in STAGE_ORDER:
        if states.get(key) not in ACTIVE_STATES:
            return label.lower().replace("_", " "), f"{label.lower().replace('_', ' ')} is {states.get(key, 'NOT_STARTED')}."
    return "validation sample flow", "Generated hypothesis has reached validation sample flow."


def _summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    furthest = "NOT_STARTED"
    order = {label: idx for idx, (_key, label) in enumerate(STAGE_ORDER)}
    for row in rows:
        for key, label in STAGE_ORDER:
            if str(row.get(key) or "") in ACTIVE_STATES and order[label] > order.get(furthest, -1):
                furthest = label
    blocked = sum(1 for row in rows if str(row.get("current_stop_stage") or "") and row.get("validation_sample_flow_state") != "FLOWING")
    bottleneck = ""
    if rows:
        bottleneck = str(rows[0].get("current_stop_stage") or "")
        if any(row.get("blocker_owner") == "DAVID" for row in rows):
            bottleneck = "data readiness / shadow validation"
        elif any(row.get("blocker_owner") == "MARKET_CONDITIONS" for row in rows):
            bottleneck = "candidate flow"
    return {
        "generated_hypotheses_total": len(rows),
        "reached_candidate_flow_count": sum(1 for row in rows if row.get("candidate_flow_state") == "FLOWING"),
        "reached_paper_observation_count": sum(1 for row in rows if row.get("paper_observation_flow_state") == "FLOWING"),
        "reached_outcome_count": sum(1 for row in rows if row.get("outcome_flow_state") == "FLOWING"),
        "reached_validation_sample_count": sum(1 for row in rows if row.get("validation_sample_flow_state") == "FLOWING"),
        "blocked_count": blocked,
        "david_action_required_count": sum(1 for row in rows if row.get("david_action_required") is True),
        "furthest_stage_reached": furthest,
        "primary_generated_hypothesis_bottleneck": bottleneck,
    }


def _owner(route: Mapping[str, Any], oil: Mapping[str, Any]) -> str:
    owner = str(route.get("owner") or "").upper()
    if owner in {"DAVID", "AEGIS_SYSTEM", "MARKET_CONDITIONS"}:
        return owner
    if oil:
        return "MARKET_CONDITIONS" if oil.get("due_to", {}).get("missing_data") else "AEGIS_SYSTEM"
    return "NONE"


def _next_step(route: Mapping[str, Any], oil: Mapping[str, Any], item: Mapping[str, Any]) -> str:
    text = str(route.get("next_step") or oil.get("next_expected_step") or item.get("next_expected_step") or "")
    if "oil shock" in str(oil.get("hypothesis_name") or "").lower():
        return "wait for qualifying market data/setup or run producer when evidence becomes available"
    return text


def _source_paths(item: Mapping[str, Any], route: Mapping[str, Any], oil: Mapping[str, Any], paths: Mapping[str, Path]) -> list[str]:
    out = [str(path) for path in _list(item.get("source_artifact_paths")) if str(path)]
    out.extend(str(path) for path in _list(route.get("source_artifact_paths")) if str(path))
    if oil:
        out.append(str(paths["oil_shock_candidate_flow"]))
    out.extend(str(paths[key]) for key in ("generated_throughput", "data_action_routing", "validation_samples", "outcome_registry", "candidate_contracts"))
    seen = set()
    deduped = []
    for path in out:
        if path and path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def _oil_row(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("oil_shock") if isinstance(payload, Mapping) else {}
    return value if isinstance(value, Mapping) else {}


def _is_oil(row: Mapping[str, Any], oil: Mapping[str, Any]) -> bool:
    hid = str(row.get("hypothesis_id") or "").lower()
    oil_id = str(oil.get("hypothesis_id") or "").lower()
    name = str(row.get("hypothesis_name") or "").lower()
    return bool((hid and oil_id and hid == oil_id) or "oil shock" in name)


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _upper(value: Any) -> str:
    return str(value or "").upper()


def _sha(path: Path | str | None) -> str:
    try:
        p = Path(path) if path else None
        return hashlib.sha256(p.read_bytes()).hexdigest() if p and p.exists() else ""
    except OSError:
        return ""


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()
