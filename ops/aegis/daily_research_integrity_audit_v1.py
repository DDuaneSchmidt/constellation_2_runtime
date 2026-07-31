from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1

FAMILY = "aegis_daily_research_integrity_audit_v1"
FILENAME = "daily_research_integrity_audit.v1.json"
SCHEMA_ID = "aegis_daily_research_integrity_audit"
SCHEMA_VERSION = "v1"
POLICY_VERSION = "AEGIS_DAILY_RESEARCH_INTEGRITY_AUDIT_POLICY_V1"

SAFETY = {
    "research_only": True,
    "read_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_allocation_mutation": True,
    "no_candidate_mutation": True,
    "no_paper_observation_creation": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "allocation_mutation_performed": False,
    "candidate_created_by_this_artifact": False,
    "paper_observation_created_by_this_artifact": False,
}


def daily_research_integrity_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / FAMILY / day_utc / FILENAME


def build_daily_research_integrity_audit_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    paths = _input_paths(root, day_utc)
    payloads = {key: _read(path) for key, path in paths.items()}
    issue_rows: list[dict[str, Any]] = []

    candidate = _candidate_integrity(payloads, paths, issue_rows)
    paper = _paper_observation_integrity(payloads, paths, issue_rows)
    exits = _exit_integrity(payloads, paths, issue_rows)
    sleeve = _sleeve_health(payloads, paths, issue_rows)
    generated = _generated_hypothesis_integrity(payloads, paths, issue_rows)
    entry = _entry_price_lineage(payloads, paths, issue_rows)
    allocation = _allocation_visibility(payloads, paths, issue_rows)
    audit = _audit_safety(payloads, paths, issue_rows)

    counts = Counter(str(row.get("severity") or "INFO") for row in issue_rows)
    blocker_count = counts["BLOCKER"]
    warning_count = counts["WARNING"]
    status = "FAIL" if blocker_count else ("WARN" if warning_count else "PASS")
    primary_issue = "Daily integrity passed. No blocking research defects."
    if blocker_count:
        primary_issue = str(next(row["recommended_next_step"] for row in issue_rows if row["severity"] == "BLOCKER"))
    elif warning_count:
        primary_issue = "Daily integrity passed with warnings."

    artifact = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "integrity_status": status,
        "primary_issue": primary_issue,
        "issue_count": len(issue_rows),
        "blocker_count": blocker_count,
        "warning_count": warning_count,
        "david_action_count": int(generated.get("david_action_count") or 0) + int(allocation.get("david_action_count") or 0),
        "issue_rows": issue_rows,
        "category_summary": dict(Counter(str(row.get("category") or "unknown") for row in issue_rows)),
        "candidate_integrity": candidate,
        "paper_observation_integrity": paper,
        "exit_integrity": exits,
        "sleeve_health": sleeve,
        "generated_hypothesis_integrity": generated,
        "entry_price_lineage": entry,
        "allocation_visibility": allocation,
        "audit_safety": audit,
        "source_artifact_paths": {key: str(path) for key, path in sorted(paths.items())},
        "input_artifact_hashes": {key: _sha(path) for key, path in sorted(paths.items())},
        "ui_summary": {
            "integrity_status": status,
            "primary_issue": primary_issue,
            "blockers": blocker_count,
            "warnings": warning_count,
            "david_actions": int(generated.get("david_action_count") or 0) + int(allocation.get("david_action_count") or 0),
            "top_issues": issue_rows[:3],
            "pass_message": "Daily integrity passed. No blocking research defects." if blocker_count == 0 and warning_count == 0 else "Daily integrity passed with warnings.",
        },
        "safety_statement": "Read-only integrity audit. It does not change trading, broker behavior, safety gates, candidate logic, paper lifecycle, exit logic, validation rules, allocations, or UI decisions.",
        **SAFETY,
    }
    artifact["content_hash"] = _stable_hash({k: v for k, v in artifact.items() if k not in {"generated_at_utc", "content_hash"}})
    return artifact


def write_daily_research_integrity_audit_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_daily_research_integrity_audit_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(daily_research_integrity_audit_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    specs = {
        "candidate_diagnostics": ("aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"),
        "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        "candidate_lifecycle": ("aegis_candidate_to_paper_lifecycle_v1", "candidate_to_paper_lifecycle.v1.json"),
        "candidate_state": ("aegis_candidate_state_v1", "candidate_state.v1.json"),
        "paper_positions": ("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"),
        "outcome_registry": ("aegis_outcome_registry_v1", "outcome_registry.v1.json"),
        "auto_closure": ("aegis_paper_outcome_auto_closure_v1", "paper_outcome_auto_closure.v1.json"),
        "exit_recommendations": ("aegis_exit_recommendations_v1", "exit_recommendations.v1.json"),
        "entry_certification": ("aegis_entry_reference_price_certification_v1", "entry_reference_price_certification.v1.json"),
        "validation_samples": ("aegis_validation_samples_v1", "validation_samples.v1.json"),
        "scorecard": ("aegis_research_daily_scorecard_v1", "research_daily_scorecard.v1.json"),
        "oil_shock_flow": ("aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"),
        "generated_throughput": ("aegis_generated_hypothesis_throughput_v1", "generated_hypothesis_throughput.v1.json"),
        "operator_action_queue": ("aegis_operator_action_queue_v1", "operator_action_queue.v1.json"),
        "allocation_recommendation": ("aegis_research_allocation_recommendation_v1", "research_allocation_recommendation.v1.json"),
        "research_capital_allocation": ("aegis_research_capital_allocation_v1", "research_capital_allocation.v1.json"),
        "verified_graph": ("aegis_verified_runtime_graph_v1", "verified_runtime_graph.v1.json"),
    }
    out: dict[str, Path] = {}
    for key, (family, filename) in specs.items():
        path, _payload = latest_json_v1(root, family, day, filename)
        out[key] = path or root / "reports" / family / day / filename
    return out


def _candidate_integrity(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    diagnostics = _dict(payloads.get("candidate_diagnostics"))
    contracts = _dict(payloads.get("candidate_contracts"))
    lifecycle = _dict(payloads.get("candidate_lifecycle"))
    summary = _dict(lifecycle.get("summary"))
    stale = _list(diagnostics.get("stale_input_artifacts"))
    failed = _list(diagnostics.get("failed_producers"))
    valid_rows = _list(contracts.get("candidate_contracts"))
    rejected_rows = _list(contracts.get("rejected_candidates")) or _list(contracts.get("rejections"))
    if stale:
        _issue(issues, "WARNING", "candidate_integrity", "aegis_candidate_generation_diagnostics_v1", "stale_input_artifacts", "0", str(len(stale)), paths["candidate_diagnostics"], "Refresh stale candidate inputs and rerun diagnostics.")
    return {
        "raw_signals": _int(diagnostics.get("total_raw_signals")),
        "valid_candidates": _int(contracts.get("candidates_created")) or len(valid_rows),
        "rejected_candidates": _int(contracts.get("candidates_rejected")) or _int(diagnostics.get("total_candidates_rejected")) or len(rejected_rows),
        "auto_promoted_candidates": _int(summary.get("auto_promoted_to_paper_tracking_count")),
        "candidates_blocked_from_paper": _int(summary.get("blocked_from_paper_count")),
        "stale_candidate_artifacts": len(stale),
        "producer_failures": len(failed),
    }


def _paper_observation_integrity(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    lifecycle_rows = _list(_dict(payloads.get("candidate_lifecycle")).get("rows"))
    outcomes = _list(_dict(payloads.get("outcome_registry")).get("outcomes"))
    positions = _open_positions(payloads)
    today_rows = [row for row in lifecycle_rows if _text(row.get("paper_position_id"))]
    outcome_by_candidate = {_text(row.get("candidate_id")): row for row in outcomes if isinstance(row, Mapping)}
    without_position = [row for row in lifecycle_rows if _text(row.get("promotion_status")) == "AUTO_PROMOTED_TO_PAPER_TRACKING" and not _text(row.get("paper_position_id"))]
    without_outcome = [row for row in lifecycle_rows if _text(row.get("paper_position_id")) and _text(row.get("candidate_id")) not in outcome_by_candidate and not _text(row.get("outcome_id"))]
    for row in without_position:
        _issue(issues, "WARNING", "paper_observation_integrity", "aegis_candidate_to_paper_lifecycle_v1", _id(row), "paper_position_id present", "missing", paths["candidate_lifecycle"], "Investigate why lifecycle row lacks paper_position_id.")
    for row in without_outcome:
        _issue(issues, "WARNING", "paper_observation_integrity", "aegis_outcome_registry_v1", _id(row), "outcome row present", "missing", paths["outcome_registry"], "Investigate outcome registry linkage for the paper observation.")
    return {
        "open_observations": sum(1 for row in outcomes if _upper(row.get("outcome_state")) == "OPEN") or len(positions),
        "new_observations_today": len(today_rows),
        "missing_entry_marks": 0,
        "observations_without_paper_position_id": len(without_position),
        "observations_without_outcome_row": len(without_outcome),
    }


def _exit_integrity(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    recommendations = _list(_dict(payloads.get("exit_recommendations")).get("recommendations")) or _list(_dict(payloads.get("exit_recommendations")).get("rows"))
    positions = _open_positions(payloads)
    rec_positions = {_text(row.get("position_id")) for row in recommendations if isinstance(row, Mapping)}
    unevaluated = [row for row in positions if _text(row.get("position_id")) not in rec_positions]
    closure_eligible = [row for row in recommendations if _upper(row.get("exit_recommendation")) in {"EXIT", "CLOSE", "TAKE_PROFIT", "STOP_OUT"} or _text(row.get("record_exit_command"))]
    missing_policy = [row for row in recommendations if not _dict(row.get("policy"))]
    overdue = [row for row in recommendations if _holding_days(row) > _max_hold_days(row)]
    auto_summary = _dict(_dict(payloads.get("auto_closure")).get("summary"))
    closed_today = _int(auto_summary.get("auto_closed_count"))
    for row in unevaluated:
        _issue(issues, "WARNING", "exit_integrity", "aegis_exit_recommendations_v1", _id(row), "exit evaluation present", "missing", paths["exit_recommendations"], "Run exit recommendation evaluation for every open paper position.")
    for row in missing_policy:
        _issue(issues, "WARNING", "exit_integrity", "aegis_exit_recommendations_v1", _id(row), "exit policy present", "missing", paths["exit_recommendations"], "Attach governed exit policy to the paper position evaluation.")
    if closed_today == 0 and (closure_eligible or overdue):
        _issue(issues, "BLOCKER", "exit_integrity", "aegis_paper_outcome_auto_closure_v1", "closed_outcomes_today", "closed when closure-eligible/overdue positions exist", "0", paths["auto_closure"], "Review closure-eligible or overdue paper positions before treating the daily run as complete.")
    return {
        "exit_evaluations_performed": len(recommendations),
        "open_positions_evaluated": max(0, len(positions) - len(unevaluated)),
        "open_positions": len(positions),
        "closure_eligible_positions": len(closure_eligible),
        "closed_outcomes_today": closed_today,
        "positions_overdue_by_exit_policy": len(overdue),
        "positions_missing_exit_policy": len(missing_policy),
        "review_only_exit_recommendations": sum(1 for row in recommendations if bool(row.get("human_review_required")) or bool(row.get("operator_action_required"))),
        "unevaluated_open_position_ids": [_text(row.get("position_id")) for row in unevaluated],
    }


def _sleeve_health(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    diagnostics = _dict(payloads.get("candidate_diagnostics"))
    lifecycle_rows = _list(_dict(payloads.get("candidate_lifecycle")).get("rows"))
    outcomes = _list(_dict(payloads.get("outcome_registry")).get("outcomes"))
    samples = _list(_dict(payloads.get("validation_samples")).get("samples"))
    recommendations = _list(_dict(payloads.get("allocation_recommendation")).get("recommendations"))
    candidate_by_sleeve = {str(row.get("sleeve_id") or "UNKNOWN"): _int(row.get("candidate_count")) for row in _list(diagnostics.get("sleeves")) if isinstance(row, Mapping)}
    observed_by_sleeve = Counter(_text(row.get("sleeve_id") or "UNKNOWN") for row in lifecycle_rows if isinstance(row, Mapping))
    outcome_by_sleeve = Counter(_text(row.get("sleeve_id") or "UNKNOWN") for row in outcomes if isinstance(row, Mapping))
    sample_by_sleeve = Counter(_text(row.get("sleeve_id") or "UNKNOWN") for row in samples if isinstance(row, Mapping))
    no_flow = sorted(sleeve for sleeve, count in candidate_by_sleeve.items() if count == 0)
    redesign_pause = [row for row in recommendations if _upper(row.get("decision_recommendation")) in {"REDESIGN", "PAUSE"} or _upper(row.get("recommended_allocation_action")) == "PAUSE"]
    for row in redesign_pause:
        _issue(issues, "WARNING", "sleeve_health", "aegis_research_allocation_recommendation_v1", _text(row.get("hypothesis_id") or row.get("name")), "follow-through item listed", _upper(row.get("decision_recommendation") or row.get("recommended_allocation_action")), paths["allocation_recommendation"], "Treat REDESIGN/PAUSE as repair investigation, not an audit blocker.")
    return {
        "candidates_by_sleeve": candidate_by_sleeve,
        "observations_by_sleeve": dict(observed_by_sleeve),
        "outcomes_by_sleeve": dict(outcome_by_sleeve),
        "included_samples_by_sleeve": dict(sample_by_sleeve),
        "sleeves_with_no_candidate_flow": no_flow,
        "sleeves_with_redesign_or_pause": [_text(row.get("hypothesis_id") or row.get("name")) for row in redesign_pause],
        "producer_failures": _list(diagnostics.get("failed_producers")),
    }


def _generated_hypothesis_integrity(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    scorecard = _dict(payloads.get("scorecard"))
    oil = _dict(_dict(payloads.get("oil_shock_flow")).get("oil_shock"))
    generated_rows = _list(scorecard.get("generated_hypothesis_progress"))
    oil_scorecard = next((row for row in generated_rows if "oil shock" in _text(row.get("hypothesis_name")).lower()), {})
    scorecard_blocker = _text(oil_scorecard.get("blocker"))
    oil_blocker = _text(oil.get("exact_blocker"))
    if scorecard_blocker and oil_blocker and scorecard_blocker != oil_blocker:
        _issue(issues, "WARNING", "generated_hypothesis_integrity", "aegis_oil_shock_candidate_flow_v1", _text(oil.get("hypothesis_id")) or "oil_shock", oil_blocker, scorecard_blocker, paths["oil_shock_flow"], "Use authoritative Oil Shock candidate flow blocker in scorecard/UI.")
    macro_rows = [row for row in generated_rows if "macro calendar" in _text(row.get("hypothesis_name")).lower()]
    return {
        "oil_shock_blocker": oil_blocker,
        "scorecard_oil_shock_blocker": scorecard_blocker,
        "oil_shock_blocker_matches_authority": not (scorecard_blocker and oil_blocker and scorecard_blocker != oil_blocker),
        "macro_calendar_data_blocker": _text(macro_rows[0].get("blocker")) if macro_rows else "",
        "generated_hypothesis_throughput_status": _text(_dict(payloads.get("generated_throughput")).get("summary", {}).get("throughput_status")) or _text(scorecard.get("daily_progress_status")),
        "generated_hypothesis_next_expected_step": _text(oil.get("next_expected_step")) or (generated_rows[0].get("next_expected_step") if generated_rows else ""),
        "david_action_required": bool(scorecard.get("david_action_count")),
        "david_action_count": _int(scorecard.get("david_action_count")),
    }


def _entry_price_lineage(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    lifecycle_rows = _list(_dict(payloads.get("candidate_lifecycle")).get("rows"))
    outcomes = _list(_dict(payloads.get("outcome_registry")).get("outcomes"))
    cert_rows = _list(_dict(payloads.get("entry_certification")).get("rows"))
    cert_by_id = {_text(row.get("certification_id")): row for row in cert_rows if isinstance(row, Mapping)}
    outcome_by_candidate = {_text(row.get("candidate_id")): row for row in outcomes if isinstance(row, Mapping)}
    affected = []
    for row in lifecycle_rows:
        if _text(row.get("entry_reference_price")):
            continue
        cert_id = _text(row.get("entry_reference_price_certification_id"))
        outcome = outcome_by_candidate.get(_text(row.get("candidate_id")), {})
        item = {
            "candidate_id": _text(row.get("candidate_id")),
            "paper_position_id": _text(row.get("paper_position_id")),
            "certified_entry_price_exists_upstream": bool(cert_id and cert_id in cert_by_id),
            "ledger_dropped_entry_mark": bool(_text(row.get("paper_position_id")) and not _text(row.get("entry_reference_price"))),
            "outcome_registry_blocked_observation": bool(_list(outcome.get("blocker_reasons"))),
            "source_artifact_path": str(paths["candidate_lifecycle"]),
        }
        affected.append(item)
        _issue(issues, "WARNING", "entry_price_lineage", "aegis_candidate_to_paper_lifecycle_v1", item["candidate_id"], "entry_reference_price present", "missing", paths["candidate_lifecycle"], "Trace certified entry price to paper ledger and outcome registry.")
    return {"missing_entry_marks_count": len(affected), "affected_rows": affected}


def _allocation_visibility(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    recommendation = _dict(payloads.get("allocation_recommendation"))
    recs = _list(recommendation.get("recommendations"))
    summary = _dict(recommendation.get("summary"))
    action_counts = {key: _int(summary.get(key)) for key in ["HOLD", "PAUSE", "INCREASE", "DECREASE"]}
    recommendation_count = _int(summary.get("recommendation_count")) or len(recs)
    capital_summary = _dict(_dict(payloads.get("research_capital_allocation")).get("summary"))
    displayed_count = _int(capital_summary.get("decision_count"))
    ui_mismatch = recommendation_count > 0 and displayed_count == 0
    if ui_mismatch:
        _issue(issues, "WARNING", "allocation_visibility", "aegis_research_allocation_recommendation_v1", "research_allocation_decisions", str(recommendation_count), "0", paths["allocation_recommendation"], "Display recommendation artifact counts; do not show 0 decisions while recommendations exist.")
    return {
        "research_allocation_recommendations_count": recommendation_count,
        "action_counts": action_counts,
        "ui_displayed_decision_count": displayed_count,
        "ui_displays_recommendations_correctly": not ui_mismatch,
        "ui_zero_decisions_while_recommendations_exist": ui_mismatch,
        "david_action_count": sum(1 for row in recs if isinstance(row, Mapping) and bool(row.get("requires_david_review"))),
    }


def _audit_safety(payloads: Mapping[str, Any], paths: Mapping[str, Path], issues: list[dict[str, Any]]) -> dict[str, Any]:
    graph = _dict(payloads.get("verified_graph"))
    graph_status = _text(graph.get("graph_status") or "UNKNOWN")
    audit_blockers = _list(graph.get("audit_blockers"))
    audit_blocker_count = _int(graph.get("audit_blocker_count")) or len(audit_blockers)
    policy = _dict(graph.get("policy_gates"))
    safety_gates_changed = any([
        bool(policy.get("trade_advice_allowed")),
        bool(policy.get("broker_execution_allowed")),
        _upper(policy.get("broker_submit_transmit_policy")) not in {"", "DISABLED_BY_DESIGN", "DISABLED"},
    ])
    if graph_status != "READY":
        _issue(issues, "BLOCKER", "audit_safety", "aegis_verified_runtime_graph_v1", "graph_status", "READY", graph_status, paths["verified_graph"], "Resolve verified runtime graph readiness before trusting daily research integrity.")
    if audit_blocker_count:
        _issue(issues, "BLOCKER", "audit_safety", "aegis_verified_runtime_graph_v1", "audit_blocker_count", "0", str(audit_blocker_count), paths["verified_graph"], "Clear verified graph audit blockers.")
    if safety_gates_changed:
        _issue(issues, "BLOCKER", "audit_safety", "aegis_verified_runtime_graph_v1", "safety_gates", "disabled", "changed", paths["verified_graph"], "Restore disabled broker/trading/advice safety gates.")
    return {
        "verified_graph_status": graph_status,
        "audit_blocker_count": audit_blocker_count,
        "safety_gates_changed": safety_gates_changed,
        "broker_execution_disabled": not bool(policy.get("broker_execution_allowed")),
        "live_trading_disabled": True,
        "trade_advice_disabled": not bool(policy.get("trade_advice_allowed")),
        "real_capital_disabled": True,
    }


def _open_positions(payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    ledger = _dict(payloads.get("paper_positions"))
    rows = _list(ledger.get("open_positions"))
    if rows:
        return [row for row in rows if isinstance(row, dict)]
    return [row for row in _list(ledger.get("positions")) if isinstance(row, dict) and _upper(row.get("current_status") or row.get("status")) == "OPEN"]


def _issue(issues: list[dict[str, Any]], severity: str, category: str, artifact: str, affected_id: str, expected: str, actual: str, source_path: Path, next_step: str) -> None:
    issues.append({
        "issue_id": f"{category}:{len(issues) + 1:03d}",
        "severity": severity,
        "category": category,
        "affected_artifact": artifact,
        "affected_id": affected_id,
        "expected_value": expected,
        "actual_value": actual,
        "source_artifact_path": str(source_path),
        "recommended_next_step": next_step,
    })


def _holding_days(row: Mapping[str, Any]) -> int:
    hp = _dict(row.get("holding_period"))
    return _int(hp.get("days") or row.get("holding_period_days"))


def _max_hold_days(row: Mapping[str, Any]) -> int:
    policy = _dict(row.get("policy"))
    return _int(policy.get("max_holding_days") or policy.get("max_hold_days")) or 999999


def _id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidate_id") or row.get("position_id") or row.get("outcome_id") or "unknown")


def _read(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
    return {}


def _sha(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ""
    except Exception:
        return ""


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0
