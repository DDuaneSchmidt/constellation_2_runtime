from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, read_json_v1, write_json_v1

FAMILY = "aegis_research_daily_scorecard_v1"
FILENAME = "research_daily_scorecard.v1.json"
SCHEMA_ID = "aegis_research_daily_scorecard"
SCHEMA_VERSION = "v1"
POLICY_VERSION = "aegis_research_daily_scorecard_policy_v1"

SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_allocation_mutation": True,
    "no_candidate_mutation": True,
    "no_paper_observation_creation": True,
    "no_hypothesis_state_mutation": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "allocation_mutation_performed": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
}


def research_daily_scorecard_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / FAMILY / day_utc / FILENAME


def build_research_daily_scorecard_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    prior_day = _prior_day_with_any_artifact(root, day_utc)
    paths = _input_paths(root, day_utc)
    prior_paths = _input_paths(root, prior_day) if prior_day else {}
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    prior_payloads = {key: read_json_v1(path) for key, path in prior_paths.items()} if prior_day else {}

    totals = _totals(payloads)
    prior_totals = _totals(prior_payloads) if prior_payloads else {}
    deltas = _deltas(totals, prior_totals)
    generated_rows = _generated_hypothesis_progress(
        payloads.get("generated_throughput", {}),
        prior_payloads.get("generated_throughput", {}) if prior_payloads else {},
        payloads.get("oil_shock_candidate_flow", {}),
    )
    generated_rows = _attach_data_action_routing(generated_rows, payloads.get("data_action_routing", {}))
    generated_rows = _attach_oil_shock_approval_lineage(generated_rows, payloads)
    deltas["generated_hypotheses_advanced"] = sum(1 for row in generated_rows if row.get("state_changed") and _is_advancement(row.get("yesterday_state"), row.get("today_state")))
    deltas["generated_hypotheses_blocked"] = sum(1 for row in generated_rows if str(row.get("throughput_status") or "").upper() in {"BLOCKED", "NEEDS_DATA", "STALLED"})

    validation = _validation_progress(payloads, prior_payloads, deltas)
    action_summary = _action_summary(payloads.get("operator_action_queue", {}), payloads.get("follow_through", {}))
    health = _health_summary(payloads, paths)
    run_status = _run_status(payloads, health)
    daily_status = _daily_progress_status(deltas, action_summary, health, run_status)
    primary_bottleneck = _primary_bottleneck(daily_status, action_summary, health, validation, generated_rows)
    primary_message = _primary_message(daily_status, deltas, action_summary, primary_bottleneck)

    source_paths = {key: str(path) for key, path in sorted(paths.items())}
    input_hashes = {key: _sha(path) for key, path in sorted(paths.items())}
    latest_run_time = _latest_generated_at(payloads) or generated_at

    body: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "target_day": day_utc,
        "day_utc": day_utc,
        "prior_comparable_day": prior_day or "",
        "latest_run_time": latest_run_time,
        "last_completed_run_type": _last_completed_run_type(payloads),
        "run_status": run_status,
        "daily_progress_status": daily_status,
        "david_action_count": action_summary["action_count"],
        "primary_message": primary_message,
        "primary_bottleneck": primary_bottleneck,
        "generated_at_utc": generated_at,
        "daily_delta_metrics": deltas,
        "validation_progress": validation,
        "generated_hypothesis_progress": generated_rows,
        "david_action_summary": action_summary,
        "health_blocker_summary": health,
        "source_artifact_paths": source_paths,
        "input_artifact_hashes": input_hashes,
        "safety_statement": "Research daily scorecard is read-only. It does not create trades, candidates, paper observations, allocation mutations, retirements, or broker actions.",
        **SAFETY,
    }
    body["content_hash"] = _stable_hash({k: v for k, v in body.items() if k not in {"generated_at_utc", "latest_run_time", "content_hash"}})
    return body


def write_research_daily_scorecard_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_research_daily_scorecard_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(research_daily_scorecard_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    specs = {
        "candidate_diagnostics": ("aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"),
        "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        "candidate_to_paper_lifecycle": ("aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"),
        "paper_outcome_auto_closure": ("aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"),
        "outcome_registry": ("aegis_outcome_registry_v1", "outcome_registry.v1.json"),
        "validation_samples": ("aegis_validation_samples_v1", "validation_samples.v1.json"),
        "hypothesis_workflow_state": ("aegis_hypothesis_workflow_state_v1", "hypothesis_workflow_state.v1.json"),
        "operator_action_queue": ("aegis_operator_action_queue_v1", "operator_action_queue.v1.json"),
        "data_action_routing": ("aegis_data_action_routing_v1", "data_action_routing.v1.json"),
        "generated_throughput": ("aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"),
        "oil_shock_candidate_flow": ("aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"),
        "generated_approval_lineage": ("aegis_generated_hypothesis_approval_event_lineage_v1", "generated_hypothesis_approval_event_lineage.v1.json"),
        "generated_governance_bridge": ("aegis_generated_hypothesis_governance_bridge_v1", "generated_hypothesis_governance_bridge.v1.json"),
        "generated_paper_setup_bridge": ("aegis_generated_hypothesis_paper_setup_bridge_v1", "generated_hypothesis_paper_setup_bridge.v1.json"),
        "oil_shock_candidate_construction": ("aegis_oil_shock_candidate_construction_v1", "oil_shock_candidate_construction.v1.json"),
        "follow_through": ("aegis_research_follow_through_control_v1", "research_follow_through_control.v1.json"),
        "research_quality": ("aegis_research_quality_engine_v1", "research_quality_engine.v1.json"),
        "hypothesis_decision": ("aegis_hypothesis_decision_policy_v1", "hypothesis_decision_policy.v1.json"),
        "allocation_recommendation": ("aegis_research_allocation_recommendation_v1", "research_allocation_recommendation.v1.json"),
        "verified_graph": ("aegis_verified_runtime_graph_v1", "verified_runtime_graph.v1.json"),
    }
    paths: dict[str, Path] = {}
    for key, (family, filename) in specs.items():
        path, _payload = latest_json_v1(root, family, day, filename)
        paths[key] = path or root / "reports" / family / day / filename
    return paths


def _prior_day_with_any_artifact(root: Path, day_utc: str) -> str:
    target = datetime.fromisoformat(day_utc).date()
    for offset in range(1, 8):
        candidate = (target - timedelta(days=offset)).isoformat()
        base = root / "reports" / "aegis_validation_samples_v1" / candidate
        if base.exists():
            return candidate
    return ""


def _totals(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    diagnostics = payloads.get("candidate_diagnostics", {})
    contracts = payloads.get("candidate_contracts", {})
    lifecycle = payloads.get("candidate_to_paper_lifecycle", {})
    closure = payloads.get("paper_outcome_auto_closure", {})
    outcome = payloads.get("outcome_registry", {})
    samples = payloads.get("validation_samples", {})
    actions = payloads.get("operator_action_queue", {})
    throughput = payloads.get("generated_throughput", {})
    return {
        "new_raw_signals": _int(diagnostics.get("total_raw_signals")),
        "new_valid_candidates": _int(contracts.get("candidates_created")) or len(_list(contracts.get("candidate_contracts"))),
        "new_auto_promoted_observations": _int(_summary(lifecycle).get("auto_promoted_to_paper_tracking_count")),
        "new_paper_observations": _int(_summary(lifecycle).get("paper_positions_created_count")) or _int(_summary(outcome).get("open_outcome_count")),
        "new_closed_outcomes": _int(_summary(closure).get("auto_closed_count")) or _int(_summary(outcome).get("closed_outcome_count")),
        "new_included_validation_samples": _int(_summary(samples).get("included_samples")) or sum(1 for row in _list(samples.get("samples")) if str(row.get("inclusion_status") or "").upper() == "INCLUDED"),
        "new_manual_review_items": _int(_summary(closure).get("manual_review_queue_count")) or len(_list(closure.get("manual_review_queue"))),
        "new_hypothesis_proposals": _int(_summary(throughput).get("generated_hypothesis_count")) or len(_list(throughput.get("generated_hypotheses"))),
        "closed_outcomes_total": _int(_summary(outcome).get("closed_outcome_count")) or _int(_summary(closure).get("auto_closed_count")),
        "included_samples_total": _int(_summary(samples).get("included_samples")),
    }


def _deltas(totals: Mapping[str, int], prior: Mapping[str, int]) -> dict[str, int]:
    keys = [
        "new_raw_signals",
        "new_valid_candidates",
        "new_auto_promoted_observations",
        "new_paper_observations",
        "new_closed_outcomes",
        "new_included_validation_samples",
        "new_manual_review_items",
        "new_hypothesis_proposals",
    ]
    out: dict[str, int] = {}
    for key in keys:
        base = _int(prior.get(key)) if prior else 0
        out[key] = max(0, _int(totals.get(key)) - base) if prior else _int(totals.get(key))
    out["generated_hypotheses_advanced"] = 0
    out["generated_hypotheses_blocked"] = 0
    return out


def _validation_progress(payloads: Mapping[str, Mapping[str, Any]], prior: Mapping[str, Mapping[str, Any]], deltas: Mapping[str, int]) -> dict[str, Any]:
    samples = payloads.get("validation_samples", {})
    outcome = payloads.get("outcome_registry", {})
    quality = payloads.get("research_quality", {})
    q_rows = _list(quality.get("hypotheses"))
    underpowered = [row for row in q_rows if str(row.get("quality_status") or "").upper() == "UNDERPOWERED" or "UNDERPOWERED" in str(row)]
    suff_rows = _list(samples.get("statistical_sufficiency"))
    closest = _closest_to_sufficiency(q_rows, suff_rows)
    included_total = _int(_summary(samples).get("included_samples")) or sum(1 for row in _list(samples.get("samples")) if str(row.get("inclusion_status") or "").upper() == "INCLUDED")
    closed_total = _int(_summary(outcome).get("closed_outcome_count")) or _int(_summary(payloads.get("paper_outcome_auto_closure", {})).get("auto_closed_count"))
    return {
        "included_samples_total": included_total,
        "included_samples_delta": _int(deltas.get("new_included_validation_samples")),
        "closed_outcomes_total": closed_total,
        "closed_outcomes_delta": _int(deltas.get("new_closed_outcomes")),
        "hypotheses_reaching_sufficiency": [str(row.get("hypothesis_name") or row.get("display_name") or row.get("hypothesis_id")) for row in q_rows if str(row.get("quality_status") or "").upper() in {"PASS", "READY_FOR_CAPITAL_REVIEW"}],
        "hypotheses_still_underpowered": len(underpowered),
        "closest_hypothesis_to_sufficiency": closest[0],
        "samples_needed_for_next_sufficiency": closest[1],
    }


def _generated_hypothesis_progress(today: Mapping[str, Any], prior: Mapping[str, Any], oil_flow_payload: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    prior_by_id = {str(row.get("hypothesis_id") or row.get("hypothesis_name") or ""): row for row in _list(prior.get("generated_hypotheses"))}
    rows: list[dict[str, Any]] = []
    for row in _list(today.get("generated_hypotheses")):
        hid = str(row.get("hypothesis_id") or row.get("hypothesis_name") or "")
        previous = prior_by_id.get(hid, {})
        today_state = str(row.get("proposal_state") or row.get("paper_setup_state") or row.get("throughput_status") or "UNKNOWN")
        yesterday_state = str(previous.get("proposal_state") or previous.get("paper_setup_state") or previous.get("throughput_status") or "")
        blocker = str(row.get("candidate_producer_status") or "") if str(row.get("throughput_status") or "").upper() in {"BLOCKED", "NEEDS_DATA", "STALLED"} else ""
        if not blocker and str(row.get("throughput_status") or "").upper() in {"BLOCKED", "NEEDS_DATA", "STALLED"}:
            blocker = str(row.get("throughput_status") or "")
        progress_row = {
            "hypothesis_name": str(row.get("hypothesis_name") or row.get("display_name") or hid),
            "hypothesis_id": hid,
            "yesterday_state": yesterday_state,
            "today_state": today_state,
            "state_changed": bool(yesterday_state and yesterday_state != today_state),
            "throughput_status": str(row.get("throughput_status") or "UNKNOWN"),
            "next_expected_step": str(row.get("next_expected_step") or ""),
            "blocker": blocker,
            "blocker_message": "",
            "blocker_source": "aegis_generated_hypothesis_throughput_v1",
            "reason_codes": [],
            "david_action_required": not bool(row.get("no_david_action_required_unless_blocked", False)) and str(row.get("throughput_status") or "").upper() in {"NEEDS_DATA", "BLOCKED"},
        }
        rows.append(_overlay_oil_shock_candidate_flow(progress_row, oil_flow_payload or {}))
    oil_row = _oil_flow_row(oil_flow_payload or {})
    if oil_row and not any(_is_oil_shock_row(row, oil_row) for row in rows):
        rows.append(_overlay_oil_shock_candidate_flow({
            "hypothesis_name": str(oil_row.get("hypothesis_name") or "Oil shock reversals across energy ETFs"),
            "hypothesis_id": str(oil_row.get("hypothesis_id") or ""),
            "yesterday_state": "",
            "today_state": str(oil_row.get("current_state") or ""),
            "state_changed": False,
            "throughput_status": str(oil_row.get("candidate_flow_status") or ""),
            "next_expected_step": str(oil_row.get("next_expected_step") or ""),
            "blocker": str(oil_row.get("exact_blocker") or ""),
            "blocker_message": "",
            "blocker_source": "aegis_oil_shock_candidate_flow_v1",
            "reason_codes": [],
            "david_action_required": bool(oil_row.get("david_action_required")),
        }, oil_flow_payload or {}))
    return rows


def _oil_flow_row(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    row = payload.get("oil_shock") if isinstance(payload, Mapping) else {}
    return row if isinstance(row, Mapping) else {}




def _attach_oil_shock_approval_lineage(rows: list[dict[str, Any]], payloads: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    lineage = _oil_artifact_row(payloads.get("generated_approval_lineage", {}))
    governance = _oil_artifact_row(payloads.get("generated_governance_bridge", {}))
    setup = _oil_artifact_row(payloads.get("generated_paper_setup_bridge", {}))
    construction = _oil_artifact_row(payloads.get("oil_shock_candidate_construction", {}))
    if not any([lineage, governance, setup, construction]):
        return rows
    for row in rows:
        if "oil shock" not in str(row.get("hypothesis_name") or "").lower() and str(row.get("hypothesis_id") or "") != "ehp_cdbd8fe683acb622":
            continue
        approval_found = bool(lineage.get("approval_event_found"))
        approval_hash = str(lineage.get("approval_event_hash") or governance.get("approval_event_hash") or setup.get("approval_event_hash") or "")
        remaining_blocker = str(construction.get("blocker_code") or setup.get("bridge_status") or governance.get("governance_bridge_status") or row.get("blocker") or "")
        row.update({
            "approval_lineage_status": str(lineage.get("approval_lineage_status") or ""),
            "approval_event_found": approval_found,
            "approval_event_hash_present": bool(approval_hash),
            "approval_event_hash": approval_hash,
            "approval_source_path": str(lineage.get("approval_source_path") or ""),
            "oil_shock_governance_bridge_status": str(governance.get("governance_bridge_status") or ""),
            "oil_shock_paper_setup_bridge_status": str(setup.get("bridge_status") or ""),
            "oil_shock_candidate_construction_status": str(construction.get("candidate_construction_status") or ""),
            "remaining_blocker": remaining_blocker,
            "remaining_missing_fields": _list(construction.get("missing_construction_fields") or setup.get("missing_fields") or governance.get("missing_fields")),
            "david_action_required": bool(construction.get("david_action_required") or setup.get("david_action_required") or governance.get("david_action_required") or lineage.get("david_action_required")),
            "approval_lineage_message": "Oil Shock approval lineage found. Governance bridge can consume approval event hash." if approval_found and approval_hash else "Oil Shock approval lineage missing. Paper setup bridge remains blocked.",
        })
    return rows


def _oil_artifact_row(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    row = payload.get("oil_shock") if isinstance(payload.get("oil_shock"), Mapping) else payload
    return row if isinstance(row, Mapping) else {}

def _attach_data_action_routing(rows: list[dict[str, Any]], routing_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    routing_rows = _list(routing_payload.get("routing_rows")) if isinstance(routing_payload, Mapping) else []
    by_id = {str(row.get("hypothesis_id") or row.get("hypothesis_name") or "").lower(): row for row in routing_rows if isinstance(row, Mapping)}
    by_name = {str(row.get("hypothesis_name") or "").lower(): row for row in routing_rows if isinstance(row, Mapping)}
    for row in rows:
        key = str(row.get("hypothesis_id") or "").lower()
        name = str(row.get("hypothesis_name") or "").lower()
        route = by_id.get(key) or by_name.get(name)
        if not route or not isinstance(route, Mapping):
            continue
        row["blocker"] = str(route.get("blocker_code") or row.get("blocker") or "")
        row["data_action_classification"] = str(route.get("data_action_classification") or "")
        row["data_action_owner"] = str(route.get("owner") or "")
        row["missing_data_description"] = str(route.get("missing_data_description") or "")
        row["data_action_next_step"] = str(route.get("next_step") or "")
        row["data_action_source"] = "aegis_data_action_routing_v1"
        row["david_action_required"] = bool(route.get("david_action_required"))
    return rows

def _is_oil_shock_row(row: Mapping[str, Any], oil_row: Mapping[str, Any]) -> bool:
    hid = str(row.get("hypothesis_id") or "").lower()
    oil_id = str(oil_row.get("hypothesis_id") or "").lower()
    name = str(row.get("hypothesis_name") or "").lower()
    oil_name = str(oil_row.get("hypothesis_name") or "").lower()
    return bool((hid and oil_id and hid == oil_id) or "oil shock" in name or (oil_name and name == oil_name))


def _overlay_oil_shock_candidate_flow(row: dict[str, Any], oil_payload: Mapping[str, Any]) -> dict[str, Any]:
    oil_row = _oil_flow_row(oil_payload)
    if not oil_row or not _is_oil_shock_row(row, oil_row):
        return row
    blocker = str(oil_row.get("exact_blocker") or oil_row.get("blocker_classification") or "")
    reason_codes = [str(item) for item in _list(oil_row.get("reason_codes")) if str(item)]
    row.update({
        "hypothesis_name": str(oil_row.get("hypothesis_name") or row.get("hypothesis_name") or "Oil shock reversals across energy ETFs"),
        "hypothesis_id": str(oil_row.get("hypothesis_id") or row.get("hypothesis_id") or ""),
        "today_state": str(oil_row.get("current_state") or row.get("today_state") or ""),
        "throughput_status": str(oil_row.get("candidate_flow_status") or row.get("throughput_status") or ""),
        "next_expected_step": _oil_next_expected_step(oil_row),
        "blocker": blocker,
        "blocker_message": _oil_blocker_message(oil_row),
        "blocker_source": "aegis_oil_shock_candidate_flow_v1",
        "reason_codes": reason_codes,
        "david_action_required": bool(oil_row.get("david_action_required")),
    })
    return row


def _oil_next_expected_step(oil_row: Mapping[str, Any]) -> str:
    reason_codes = {str(item).upper() for item in _list(oil_row.get("reason_codes"))}
    blocker = str(oil_row.get("exact_blocker") or "").upper()
    if blocker == "PRODUCER_MISSING" or "PRODUCER_MISSING" in reason_codes:
        return "implement/run deterministic Oil Shock producer"
    return str(oil_row.get("next_expected_step") or "")


def _oil_blocker_message(oil_row: Mapping[str, Any]) -> str:
    reason_codes = {str(item).upper() for item in _list(oil_row.get("reason_codes"))}
    blocker = str(oil_row.get("exact_blocker") or "").upper()
    if blocker == "PRODUCER_MISSING" or "PRODUCER_MISSING" in reason_codes:
        return "Oil Shock deterministic candidate producer is missing."
    return str(oil_row.get("ui_message") or oil_row.get("reason_no_candidates_generated_yet") or "")


def _action_summary(queue: Mapping[str, Any], follow: Mapping[str, Any]) -> dict[str, Any]:
    actions = _list(queue.get("actions"))
    action_types = sorted({str(row.get("action_type") or row.get("next_action") or "UNKNOWN") for row in actions if isinstance(row, Mapping)})
    top = actions[0] if actions else {}
    exact_buttons = _list(top.get("exact_buttons")) if isinstance(top, Mapping) else []
    message = "No David action required."
    if top:
        name = str(top.get("hypothesis_name") or "Aegis")
        why = str(top.get("why_action_needed") or top.get("blocking_what") or "action is required")
        message = f"{name}: {why}"
    return {
        "action_count": _int(_summary(queue).get("action_count")) or len(actions),
        "action_types": action_types,
        "top_action": str(top.get("action_type") or "") if isinstance(top, Mapping) else "",
        "action_message": message,
        "exact_button_needed": str(exact_buttons[0]) if exact_buttons else "",
        "exact_buttons": exact_buttons,
    }


def _health_summary(payloads: Mapping[str, Mapping[str, Any]], paths: Mapping[str, Path]) -> dict[str, Any]:
    graph = payloads.get("verified_graph", {})
    diagnostics = payloads.get("candidate_diagnostics", {})
    lifecycle = payloads.get("candidate_to_paper_lifecycle", {})
    graph_status = str(graph.get("graph_status") or "UNKNOWN")
    audit_blockers = _list(graph.get("audit_blockers"))
    raw_failed_producers = _list(diagnostics.get("failed_producers"))
    failed_producers = [
        row for row in raw_failed_producers
        if isinstance(row, Mapping)
        and str(row.get("run_status") or row.get("canonical_blocker") or "").upper() in {"FAILED", "IMPLEMENTATION_DEFECT", "PRODUCER_MISSING"}
    ]
    stale = _list(diagnostics.get("stale_input_artifacts"))
    missing_entry = _int((payloads.get("entry_reference_price_certification", {}).get("summary") or {}).get("uncertified_count"))
    safety = graph.get("policy_gates") if isinstance(graph.get("policy_gates"), Mapping) else {}
    safety_changed = bool(safety.get("trade_advice_allowed") or safety.get("manual_trade_capture_allowed") or str(safety.get("broker_submit_transmit_policy") or "") != "DISABLED_BY_DESIGN")
    return {
        "audit_status": graph_status,
        "audit_blocker_count": len(audit_blockers),
        "producer_failures": [str(item) for item in failed_producers],
        "stale_artifact_warnings": [str(item) for item in stale],
        "missing_entry_marks": missing_entry,
        "missing_current_marks": _int(diagnostics.get("missing_current_marks")),
        "workflow_replay_status": str(lifecycle.get("workflow_replay_status") or "PASS"),
        "safety_gates_changed": safety_changed,
        "missing_input_artifacts": [key for key, path in paths.items() if not path.exists()],
    }


def _run_status(payloads: Mapping[str, Mapping[str, Any]], health: Mapping[str, Any]) -> str:
    if health.get("missing_input_artifacts"):
        return "PARTIAL"
    diagnostics = payloads.get("candidate_diagnostics", {})
    if not diagnostics:
        return "NOT_RUN"
    status = str(diagnostics.get("candidate_generation_status") or diagnostics.get("operator_interpretation") or "").upper()
    if "FAILED" in status:
        return "FAILED"
    if str(diagnostics.get("operator_interpretation") or "").upper() == "PARTIAL_RUN":
        return "PARTIAL"
    return "RAN"


def _daily_progress_status(deltas: Mapping[str, int], action: Mapping[str, Any], health: Mapping[str, Any], run_status: str) -> str:
    if str(health.get("audit_status") or "") != "READY" or _int(health.get("audit_blocker_count")) > 0 or str(health.get("workflow_replay_status") or "").upper() not in {"", "PASS", "OK"} or health.get("producer_failures"):
        return "BLOCKED"
    if run_status in {"FAILED", "NOT_RUN"}:
        return "BLOCKED"
    if _int(action.get("action_count")) > 0:
        return "ACTION_REQUIRED"
    progress_keys = ["new_included_validation_samples", "new_closed_outcomes", "new_paper_observations", "new_valid_candidates", "generated_hypotheses_advanced"]
    if any(_int(deltas.get(key)) > 0 for key in progress_keys):
        return "PROGRESS"
    return "NO_PROGRESS"


def _primary_bottleneck(status: str, action: Mapping[str, Any], health: Mapping[str, Any], validation: Mapping[str, Any], generated: list[Mapping[str, Any]]) -> str:
    if status == "BLOCKED":
        if health.get("audit_status") != "READY":
            return "audit not READY"
        if health.get("producer_failures"):
            return "required producer failed"
        return "primary truth blocker"
    if status == "ACTION_REQUIRED":
        return str(action.get("action_message") or "David action required")
    blocked_generated = [row for row in generated if row.get("blocker")]
    if blocked_generated:
        row = blocked_generated[0]
        message = str(row.get("blocker_message") or "")
        if message:
            return f"{row.get('hypothesis_name')} blocked: {row.get('blocker')} - {message}"
        return f"{row.get('hypothesis_name')} blocked: {row.get('blocker')}"
    if _int(validation.get("hypotheses_still_underpowered")):
        return "Validation sample sufficiency / underpowered hypotheses"
    return "Awaiting closed outcomes"


def _primary_message(status: str, deltas: Mapping[str, int], action: Mapping[str, Any], bottleneck: str) -> str:
    if status == "BLOCKED":
        return f"Blocked: {bottleneck}."
    if status == "ACTION_REQUIRED":
        return f"Action required: {action.get('action_message')}"
    if _int(deltas.get("new_included_validation_samples")) > 0:
        return f"Validation advanced: +{_int(deltas.get('new_included_validation_samples'))} included samples."
    if status == "PROGRESS":
        return f"Progress: +{_int(deltas.get('new_valid_candidates'))} candidate and +{_int(deltas.get('new_paper_observations'))} paper observation. No David action required."
    return "No new validation evidence. Aegis is waiting for outcomes to close."


def _last_completed_run_type(payloads: Mapping[str, Mapping[str, Any]]) -> str:
    diagnostics = payloads.get("candidate_diagnostics", {})
    if diagnostics:
        return "CANDIDATE_GENERATION_0950"
    return "UNKNOWN"


def _latest_generated_at(payloads: Mapping[str, Mapping[str, Any]]) -> str:
    values = []
    for payload in payloads.values():
        for key in ("computed_at_utc", "generated_at_utc", "generated_at"):
            value = str(payload.get(key) or "")
            if value:
                values.append(value)
    return sorted(values)[-1] if values else ""


def _closest_to_sufficiency(q_rows: list[Mapping[str, Any]], suff_rows: list[Mapping[str, Any]]) -> tuple[str, int]:
    best_name = ""
    best_needed = 30
    for row in q_rows + suff_rows:
        count = _int(row.get("usable_sample_count") or row.get("sample_count") or row.get("included_sample_count"))
        needed = max(0, 30 - count)
        if needed < best_needed:
            best_needed = needed
            best_name = str(row.get("hypothesis_name") or row.get("display_name") or row.get("name") or row.get("hypothesis_id") or "")
    return best_name, best_needed


def _is_advancement(old: Any, new: Any) -> bool:
    order = {"": 0, "NEEDS_DATA": 1, "PAPER_TRACKING_READY": 2, "NO_MARKET_SETUP": 3, "CANDIDATES_FLOWING": 4, "VALIDATION_READY": 5}
    return order.get(str(new or "").upper(), 0) > order.get(str(old or "").upper(), 0)


def _summary(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    value = payload.get("summary") if isinstance(payload, Mapping) else {}
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _sha(path: Path | None) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest() if path and path.exists() else ""
    except OSError:
        return ""


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(payload))).hexdigest()
