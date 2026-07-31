from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1
from ops.aegis.outcome_performance_metrics_v1 import build_outcome_performance_metrics_v1, outcome_performance_metrics_path_v1

SCORING_VERSION = "AEGIS_RESEARCH_QUALITY_SCORING_V2_OUTCOME_PERFORMANCE"
DECISION_POLICY_VERSION = "AEGIS_HYPOTHESIS_DECISION_POLICY_V2_OUTCOME_PERFORMANCE"
ALLOCATION_POLICY_VERSION = "AEGIS_RESEARCH_ALLOCATION_RECOMMENDATION_POLICY_V2_OUTCOME_PERFORMANCE"
FOLLOW_THROUGH_POLICY_VERSION = "AEGIS_RESEARCH_FOLLOW_THROUGH_CONTROL_POLICY_V1"
QUALITY_FAMILY = "aegis_research_quality_engine_v1"
DECISION_FAMILY = "aegis_hypothesis_decision_policy_v1"
ALLOCATION_FAMILY = "aegis_research_allocation_recommendation_v1"
FOLLOW_THROUGH_FAMILY = "aegis_research_follow_through_control_v1"
QUALITY_FILENAME = "research_quality_engine.v1.json"
DECISION_FILENAME = "hypothesis_decision_policy.v1.json"
ALLOCATION_FILENAME = "research_allocation_recommendation.v1.json"
FOLLOW_THROUGH_FILENAME = "research_follow_through_control.v1.json"
SAFETY = {
    "read_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "autonomous_execution_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "allocation_mutation_performed": False,
    "automatic_repair_performed": False,
}
MIN_INCLUDED_SAMPLES = 30


def research_quality_engine_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, QUALITY_FAMILY, day_utc, QUALITY_FILENAME)


def hypothesis_decision_policy_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, DECISION_FAMILY, day_utc, DECISION_FILENAME)


def research_allocation_recommendation_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, ALLOCATION_FAMILY, day_utc, ALLOCATION_FILENAME)


def research_follow_through_control_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FOLLOW_THROUGH_FAMILY, day_utc, FOLLOW_THROUGH_FILENAME)


def build_research_quality_engine_v1(*, truth_root: Path | str, day_utc: str, portfolio: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root)
    paths = _quality_input_paths(root, day_utc)
    inputs = {key: read_json_v1(path) for key, path in paths.items()}
    if not inputs.get("outcome_performance_metrics"):
        inputs["outcome_performance_metrics"] = build_outcome_performance_metrics_v1(truth_root=root, day_utc=day_utc)
    portfolio_payload = dict(portfolio or inputs.get("research_portfolio") or build_research_portfolio_v1(truth_root=root, day_utc=day_utc))
    rows = _combined_hypothesis_rows(portfolio_payload, inputs.get("hypothesis_workflow_state") or {})
    name_counts = Counter(_norm(row.get("name") or row.get("display_name")) for row in rows if _norm(row.get("name") or row.get("display_name")))
    suff_by_h = {text_v1(row.get("hypothesis_id")): row for row in (inputs.get("statistical_sufficiency") or {}).get("hypotheses") or [] if isinstance(row, Mapping)}
    perf_by_h = {text_v1(row.get("hypothesis_id")): row for row in (inputs.get("outcome_performance_metrics") or {}).get("hypotheses") or [] if isinstance(row, Mapping)}
    quality_rows = [_quality_row(row, suff_by_h.get(text_v1(row.get("hypothesis_id")), {}), perf_by_h.get(text_v1(row.get("hypothesis_id")), {}), paths, name_counts) for row in rows]
    artifact_hashes = {key: file_hash_v1(path) for key, path in paths.items()}
    generated = _input_generated_at(inputs)
    payload = {
        "schema_id": "aegis_research_quality_engine",
        "schema_version": "v1",
        "artifact_id": QUALITY_FAMILY,
        "day_utc": str(day_utc),
        "scoring_version": SCORING_VERSION,
        "input_artifact_hashes": artifact_hashes,
        "input_generated_at_utc": generated,
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "version": SCORING_VERSION, "inputs": artifact_hashes}),
        "source_artifact_paths": {key: str(path) for key, path in paths.items()},
        "hypotheses": quality_rows,
        "summary": {
            "hypothesis_count": len(quality_rows),
            "blocked_count": sum(1 for row in quality_rows if row.get("quality_status") == "BLOCKED"),
            "underpowered_count": sum(1 for row in quality_rows if row.get("quality_status") == "UNDERPOWERED"),
            "watch_count": sum(1 for row in quality_rows if row.get("quality_status") == "WATCH"),
            "ready_for_capital_review_count": 0,
        },
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def write_research_quality_engine_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_research_quality_engine_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(research_quality_engine_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def build_hypothesis_decision_policy_v1(*, truth_root: Path | str, day_utc: str, quality: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root)
    q = dict(quality or _load_or_build_quality(root, day_utc))
    q_path = research_quality_engine_path_v1(truth_root=root, day_utc=day_utc)
    decisions = [_decision_row(row, q_path) for row in q.get("hypotheses") or [] if isinstance(row, Mapping)]
    input_hashes = {"aegis_research_quality_engine_v1": file_hash_v1(q_path) or stable_hash_v1(_without_generated_time(q))}
    payload = {
        "schema_id": "aegis_hypothesis_decision_policy",
        "schema_version": "v1",
        "artifact_id": DECISION_FAMILY,
        "day_utc": str(day_utc),
        "scoring_version": q.get("scoring_version") or SCORING_VERSION,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "input_artifact_hashes": input_hashes,
        "input_generated_at_utc": q.get("computed_at_utc") or q.get("input_generated_at_utc") or "",
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "version": DECISION_POLICY_VERSION, "inputs": input_hashes}),
        "source_artifact_paths": {"aegis_research_quality_engine_v1": str(q_path)},
        "decisions": decisions,
        "summary": {value: sum(1 for row in decisions if row.get("recommendation") == value) for value in _decision_values()},
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["summary"]["decision_count"] = len(decisions)
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def write_hypothesis_decision_policy_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_hypothesis_decision_policy_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(hypothesis_decision_policy_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def build_research_allocation_recommendation_v1(*, truth_root: Path | str, day_utc: str, decisions: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root)
    decision_payload = dict(decisions or _load_or_build_decisions(root, day_utc))
    d_path = hypothesis_decision_policy_path_v1(truth_root=root, day_utc=day_utc)
    prior_path = report_path_v1(root, "aegis_research_allocation_v1", day_utc, "research_allocation.v1.json")
    prior = read_json_v1(prior_path)
    weight_by_h = {text_v1(row.get("hypothesis_id")): float(row.get("allocation_score") or 0.0) / 100.0 for row in prior.get("recommendations") or [] if isinstance(row, Mapping)}
    recommendations = [_allocation_row(row, weight_by_h.get(text_v1(row.get("hypothesis_id")), 0.0), d_path) for row in decision_payload.get("decisions") or [] if isinstance(row, Mapping)]
    input_hashes = {
        "aegis_hypothesis_decision_policy_v1": file_hash_v1(d_path) or stable_hash_v1(_without_generated_time(decision_payload)),
        "aegis_research_allocation_v1": file_hash_v1(prior_path),
    }
    payload = {
        "schema_id": "aegis_research_allocation_recommendation",
        "schema_version": "v1",
        "artifact_id": ALLOCATION_FAMILY,
        "day_utc": str(day_utc),
        "scoring_version": decision_payload.get("scoring_version") or SCORING_VERSION,
        "decision_policy_version": decision_payload.get("decision_policy_version") or DECISION_POLICY_VERSION,
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "input_artifact_hashes": input_hashes,
        "input_generated_at_utc": decision_payload.get("computed_at_utc") or "",
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "version": ALLOCATION_POLICY_VERSION, "inputs": input_hashes}),
        "source_artifact_paths": {"aegis_hypothesis_decision_policy_v1": str(d_path), "aegis_research_allocation_v1": str(prior_path)},
        "recommendations": recommendations,
        "summary": {value: sum(1 for row in recommendations if row.get("recommended_allocation_action") == value) for value in _allocation_values()},
        "allocation_mutation_performed": False,
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["summary"]["recommendation_count"] = len(recommendations)
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def write_research_allocation_recommendation_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_research_allocation_recommendation_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(research_allocation_recommendation_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def build_research_follow_through_control_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    quality: Mapping[str, Any] | None = None,
    decisions: Mapping[str, Any] | None = None,
    allocation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(truth_root)
    quality_payload = dict(quality or _load_or_build_quality(root, day_utc))
    decision_payload = dict(decisions or _load_or_build_decisions(root, day_utc))
    allocation_payload = dict(allocation or _load_or_build_allocation(root, day_utc))
    paths = _follow_through_input_paths(root, day_utc)
    workflow = read_json_v1(paths["aegis_hypothesis_workflow_state_v1"])
    quality_by_h = {text_v1(row.get("hypothesis_id")): row for row in quality_payload.get("hypotheses") or [] if isinstance(row, Mapping)}
    allocation_by_h = {text_v1(row.get("hypothesis_id")): row for row in allocation_payload.get("recommendations") or [] if isinstance(row, Mapping)}
    workflow_by_h = {text_v1(row.get("hypothesis_id")): row for row in workflow.get("hypotheses") or [] if isinstance(row, Mapping)}
    source_hashes = {
        "aegis_research_quality_engine_v1": file_hash_v1(paths["aegis_research_quality_engine_v1"]) or stable_hash_v1(_without_generated_time(quality_payload)),
        "aegis_hypothesis_decision_policy_v1": file_hash_v1(paths["aegis_hypothesis_decision_policy_v1"]) or stable_hash_v1(_without_generated_time(decision_payload)),
        "aegis_research_allocation_recommendation_v1": file_hash_v1(paths["aegis_research_allocation_recommendation_v1"]) or stable_hash_v1(_without_generated_time(allocation_payload)),
        "aegis_hypothesis_workflow_state_v1": file_hash_v1(paths["aegis_hypothesis_workflow_state_v1"]),
    }
    source_paths = {key: str(value) for key, value in paths.items()}
    follow_ups = [
        _follow_up_row(
            decision=row,
            quality=quality_by_h.get(text_v1(row.get("hypothesis_id")), {}),
            allocation=allocation_by_h.get(text_v1(row.get("hypothesis_id")), {}),
            workflow=workflow_by_h.get(text_v1(row.get("hypothesis_id")), {}),
            source_paths=source_paths,
            source_hashes=source_hashes,
            day_utc=str(day_utc),
        )
        for row in decision_payload.get("decisions") or []
        if isinstance(row, Mapping)
    ]
    status_counts = Counter(text_v1(row.get("current_status")) for row in follow_ups)
    type_counts = Counter(text_v1(row.get("follow_up_type")) for row in follow_ups)
    stalled = {"STALLED", "ESCALATE_TO_REDESIGN"}
    payload = {
        "schema_id": "aegis_research_follow_through_control",
        "schema_version": "v1",
        "artifact_id": FOLLOW_THROUGH_FAMILY,
        "day_utc": str(day_utc),
        "scoring_version": quality_payload.get("scoring_version") or SCORING_VERSION,
        "decision_policy_version": decision_payload.get("decision_policy_version") or DECISION_POLICY_VERSION,
        "allocation_policy_version": allocation_payload.get("allocation_policy_version") or ALLOCATION_POLICY_VERSION,
        "follow_through_policy_version": FOLLOW_THROUGH_POLICY_VERSION,
        "input_artifact_hashes": dict(source_hashes),
        "input_generated_at_utc": "|".join(sorted(text_v1(value) for value in [quality_payload.get("computed_at_utc"), decision_payload.get("computed_at_utc"), allocation_payload.get("computed_at_utc"), workflow.get("computed_at_utc") or workflow.get("generated_at")] if text_v1(value))),
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"day_utc": str(day_utc), "version": FOLLOW_THROUGH_POLICY_VERSION, "inputs": source_hashes}),
        "source_artifact_paths": source_paths,
        "follow_ups": follow_ups,
        "summary": {
            "follow_up_count": len(follow_ups),
            "by_type": dict(sorted(type_counts.items())),
            "by_status": dict(sorted(status_counts.items())),
            "overdue_or_stalled_count": sum(1 for row in follow_ups if text_v1(row.get("current_status")) in stalled or bool(row.get("is_overdue"))),
            "david_action_count": sum(1 for row in follow_ups if bool(row.get("requires_david_action"))),
            "automatic_watch_count": sum(1 for row in follow_ups if row.get("next_action") == "MONITOR_AUTOMATICALLY"),
            "repair_investigation_count": type_counts.get("REPAIR_INVESTIGATION", 0),
            "sample_accumulation_watch_count": type_counts.get("SAMPLE_ACCUMULATION_WATCH", 0),
        },
        "allocation_mutation_performed": False,
        "automatic_repair_performed": False,
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def write_research_follow_through_control_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_research_follow_through_control_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(research_follow_through_control_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _quality_input_paths(root: Path, day_utc: str) -> dict[str, Path]:
    return {
        "research_portfolio": report_path_v1(root, "aegis_research_portfolio_v1", day_utc, "research_portfolio.v1.json"),
        "hypothesis_registry": report_path_v1(root, "aegis_hypothesis_registry_v1", day_utc, "hypothesis_registry.v1.json"),
        "hypothesis_workflow_state": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day_utc, "hypothesis_workflow_state.v1.json"),
        "candidate_state": report_path_v1(root, "aegis_candidate_state_v1", day_utc, "candidate_state.v1.json"),
        "candidate_lifecycle": report_path_v1(root, "aegis_candidate_to_paper_lifecycle_v1", day_utc, "candidate_to_paper_lifecycle.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day_utc, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day_utc, "validation_samples.v1.json"),
        "statistical_sufficiency": report_path_v1(root, "aegis_statistical_sufficiency_v1", day_utc, "statistical_sufficiency.v1.json"),
        "research_allocation": report_path_v1(root, "aegis_research_allocation_v1", day_utc, "research_allocation.v1.json"),
        "outcome_performance_metrics": outcome_performance_metrics_path_v1(truth_root=root, day_utc=day_utc),
    }


def _combined_hypothesis_rows(portfolio: Mapping[str, Any], workflow: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in portfolio.get("hypotheses") or []:
        if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")):
            hid = text_v1(row.get("hypothesis_id"))
            rows[hid] = {**dict(row), "source_type": row.get("source_type") or "SEEDED_PORTFOLIO"}
    for row in workflow.get("hypotheses") or []:
        if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")):
            hid = text_v1(row.get("hypothesis_id"))
            merged = {**rows.get(hid, {}), **dict(row)}
            merged["name"] = merged.get("name") or merged.get("display_name")
            rows[hid] = merged
    return [rows[key] for key in sorted(rows)]


def _quality_row(row: Mapping[str, Any], suff: Mapping[str, Any], perf: Mapping[str, Any], paths: Mapping[str, Path], name_counts: Counter[str]) -> dict[str, Any]:
    hid = text_v1(row.get("hypothesis_id"))
    name = text_v1(row.get("name") or row.get("display_name") or hid)
    candidates = _count(row, "linked_candidates")
    positions = _count(row, "linked_paper_positions")
    outcomes = _count(row, "linked_outcomes")
    performance_samples = int(perf.get("included_validation_sample_count") or 0)
    samples = max(int(row.get("sample_count") or 0) + int(suff.get("usable_sample_count") or 0), performance_samples)
    time_in_state = int(row.get("time_in_state_days") or 0)
    current_state = text_v1(row.get("current_state"))
    reason_codes = list(dict.fromkeys([text_v1(code) for code in (row.get("reason_codes") or []) if text_v1(code)]))
    blockers = list(dict.fromkeys([text_v1(code) for code in (row.get("blocker_codes") or []) if text_v1(code)]))
    hard = _hard_gates(row, name_counts, candidates, positions, samples, time_in_state, paths)
    source_paths = [str(path) for path in paths.values()]
    source_hashes = {key: file_hash_v1(path) for key, path in paths.items()}
    performance = _performance_snapshot(perf)
    performance_reasons = list(performance.get("reason_codes") or [])
    sample_counts = {"candidate_count": candidates, "paper_position_count": positions, "outcome_count": max(outcomes, int(performance.get("closed_outcome_count") or 0)), "closed_outcome_count": int(performance.get("closed_outcome_count") or 0), "included_sample_count": samples, "included_validation_sample_count": int(performance.get("included_validation_sample_count") or samples), "excluded_validation_sample_count": int(performance.get("excluded_validation_sample_count") or 0), "minimum_required_samples": MIN_INCLUDED_SAMPLES, "distance_to_sufficiency": int(performance.get("distance_to_sufficiency") or max(MIN_INCLUDED_SAMPLES - samples, 0))}
    validation_grade, validation_reasons, validation_confidence = _validation_evidence_grade(suff, performance, candidates, positions, samples)
    grades = {
        "economic_rationale": _grade("PASS" if text_v1(row.get("formal_claim")) or row.get("source_type") == "GENERATED_PROPOSAL" else "WATCH", ["ECONOMIC_RATIONALE_PRESENT" if text_v1(row.get("formal_claim")) else "GENERATED_OR_LEGACY_RATIONALE"], source_paths, source_hashes, sample_counts, "MEDIUM"),
        "data_quality": _grade("BLOCKED" if any(g["gate_code"] == "NO_DATA_SOURCE" for g in hard) else "WATCH" if blockers else "PASS", (["NO_DATA_SOURCE"] if any(g["gate_code"] == "NO_DATA_SOURCE" for g in hard) else blockers or ["DATA_SOURCE_PRESENT"]), source_paths, source_hashes, sample_counts, "HIGH" if blockers else "MEDIUM"),
        "implementation_completeness": _grade("FAIL" if any(g["gate_code"] == "NO_PAPER_PATH" and g["severity"] == "BLOCKED" for g in hard) else "WATCH" if positions == 0 else "PASS", ["NO_PAPER_PATH"] if positions == 0 and current_state != "PAPER_TRACKING_READY" else ["PAPER_PATH_PRESENT_OR_READY"], source_paths, source_hashes, sample_counts, "MEDIUM"),
        "sample_production": _grade("UNDERPOWERED" if samples < MIN_INCLUDED_SAMPLES and (candidates or positions or current_state == "PAPER_TRACKING_READY") else "WATCH" if candidates == 0 else "PASS", ["INCLUDED_SAMPLES_BELOW_MINIMUM"] if samples < MIN_INCLUDED_SAMPLES else ["SAMPLE_MINIMUM_MET"], source_paths, source_hashes, sample_counts, "MEDIUM"),
        "validation_evidence": _grade(validation_grade, validation_reasons, source_paths, source_hashes, sample_counts, validation_confidence),
        "outcome_performance": _grade(_outcome_performance_grade(performance), performance_reasons or ["NO_OUTCOME_PERFORMANCE_METRICS"], source_paths, source_hashes, sample_counts, text_v1(performance.get("performance_confidence")) or "LOW"),
        "robustness": _grade("PASS" if text_v1(suff.get("sample_independence_status")) == "PASS" and text_v1(suff.get("regime_coverage_status")) == "PASS" else "UNDERPOWERED", ["ROBUSTNESS_UNDERPOWERED"] if samples < MIN_INCLUDED_SAMPLES else ["ROBUSTNESS_PASS"], source_paths, source_hashes, sample_counts, "LOW" if samples < MIN_INCLUDED_SAMPLES else "MEDIUM"),
    }
    active_codes = [g["gate_code"] for g in hard]
    sufficient_poor = _performance_is_sufficient_poor(performance)
    status = "REJECT" if "DUPLICATE_HYPOTHESIS" in active_codes else "BLOCKED" if any(g["severity"] == "BLOCKED" for g in hard) else "UNDERPOWERED" if "INCLUDED_SAMPLES_BELOW_MINIMUM" in active_codes else "WATCH" if hard or sufficient_poor else "PASS"
    all_reasons = list(dict.fromkeys(reason_codes + blockers + active_codes + performance_reasons + [r for g in grades.values() for r in g["reason_codes"]]))
    return {
        "hypothesis_id": hid,
        "name": name,
        "thesis_id": row.get("thesis_id") or "",
        "quality_status": status,
        "grades": grades,
        "hard_gates": hard,
        "active_hard_gate_codes": active_codes,
        "reason_codes": all_reasons or ["NO_REASON_CODE"],
        "source_artifact_paths": source_paths,
        "source_artifact_hashes": source_hashes,
        "sample_counts": sample_counts,
        "outcome_performance_metrics": performance,
        "confidence_level": "HIGH" if any(g["severity"] == "BLOCKED" for g in hard) else text_v1(performance.get("performance_confidence")) or "MEDIUM",
        "scoring_version": SCORING_VERSION,
        "no_broker_execution": True,
        "no_trade_advice": True,
        "no_live_trading": True,
        "no_real_capital": True,
    }



def _performance_snapshot(perf: Mapping[str, Any]) -> dict[str, Any]:
    if not perf:
        return {
            "closed_outcome_count": 0,
            "included_validation_sample_count": 0,
            "excluded_validation_sample_count": 0,
            "minimum_required_samples": MIN_INCLUDED_SAMPLES,
            "distance_to_sufficiency": MIN_INCLUDED_SAMPLES,
            "performance_confidence": "LOW",
            "underpowered": True,
            "reason_codes": ["NO_OUTCOME_PERFORMANCE_METRICS"],
        }
    keys = [
        "closed_outcome_count", "included_validation_sample_count", "excluded_validation_sample_count", "win_count", "loss_count",
        "win_rate", "average_realized_return", "median_realized_return", "best_realized_return", "worst_realized_return",
        "average_holding_period", "exit_reason_distribution", "take_profit_count", "stop_loss_count", "time_stop_count",
        "average_return_by_exit_reason", "sample_sufficiency_status", "minimum_required_samples", "distance_to_sufficiency",
        "performance_confidence", "underpowered", "source_outcome_ids", "source_validation_sample_ids", "source_artifact_paths",
        "source_artifact_hashes", "computed_at_utc", "scoring_version", "performance_policy_version", "reason_codes",
    ]
    return {key: perf.get(key) for key in keys if key in perf}


def _validation_evidence_grade(suff: Mapping[str, Any], perf: Mapping[str, Any], candidates: int, positions: int, samples: int) -> tuple[str, list[str], str]:
    reasons = list(dict.fromkeys(list(suff.get("state_reason_codes") or []) + list(perf.get("reason_codes") or [])))
    confidence = text_v1(perf.get("performance_confidence")) or ("LOW" if samples < MIN_INCLUDED_SAMPLES else "MEDIUM")
    included = int(perf.get("included_validation_sample_count") or samples or 0)
    if included == 0:
        return ("UNDERPOWERED" if candidates or positions else "NOT_APPLICABLE", reasons or ["NO_INCLUDED_VALIDATION_SAMPLES"], "LOW")
    if included < MIN_INCLUDED_SAMPLES:
        return "UNDERPOWERED", reasons or ["OUTCOME_PERFORMANCE_UNDERPOWERED"], "LOW"
    if _performance_is_sufficient_poor(perf):
        return "FAIL", reasons or ["SUFFICIENT_OUTCOME_PERFORMANCE_POOR"], confidence
    if text_v1(suff.get("sufficiency_state")) == "VALIDATED":
        return "PASS", reasons or ["STATISTICAL_SUFFICIENCY_AND_VALIDATION_PASS"], confidence
    return "WATCH", reasons or ["OUTCOME_PERFORMANCE_ACCUMULATING"], confidence


def _outcome_performance_grade(perf: Mapping[str, Any]) -> str:
    if int(perf.get("included_validation_sample_count") or 0) == 0:
        return "NOT_APPLICABLE"
    if _performance_is_sufficient_poor(perf):
        return "FAIL"
    if _performance_is_underpowered_poor(perf):
        return "WATCH"
    if bool(perf.get("underpowered")):
        return "UNDERPOWERED"
    if _performance_is_sufficient_strong(perf):
        return "PASS"
    return "WATCH"


def _performance_is_underpowered_poor(perf: Mapping[str, Any]) -> bool:
    return bool(perf.get("underpowered")) and int(perf.get("included_validation_sample_count") or 0) > 0 and _performance_is_poor(perf)


def _performance_is_underpowered_positive(perf: Mapping[str, Any]) -> bool:
    avg = _float(perf.get("average_realized_return"))
    return bool(perf.get("underpowered")) and int(perf.get("included_validation_sample_count") or 0) > 0 and avg is not None and avg > 0


def _performance_is_sufficient_poor(perf: Mapping[str, Any]) -> bool:
    return not bool(perf.get("underpowered")) and _performance_is_poor(perf)


def _performance_is_sufficient_strong(perf: Mapping[str, Any]) -> bool:
    avg = _float(perf.get("average_realized_return"))
    win_rate = _float(perf.get("win_rate"))
    return not bool(perf.get("underpowered")) and avg is not None and avg > 0 and (win_rate is None or win_rate >= 0.5)


def _performance_is_poor(perf: Mapping[str, Any]) -> bool:
    avg = _float(perf.get("average_realized_return"))
    win_rate = _float(perf.get("win_rate"))
    return (avg is not None and avg < 0) or (win_rate is not None and win_rate < 0.4)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _hard_gates(row: Mapping[str, Any], name_counts: Counter[str], candidates: int, positions: int, samples: int, time_in_state: int, paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    name = _norm(row.get("name") or row.get("display_name"))
    current_state = text_v1(row.get("current_state"))
    missing_dataset = text_v1(row.get("missing_dataset"))
    reasons = {text_v1(code).lower() for code in row.get("reason_codes") or []}
    blockers = {text_v1(code).lower() for code in row.get("blocker_codes") or []}
    if missing_dataset or current_state == "NEEDS_DATA" or any("missing_macro_event_calendar" in code for code in reasons | blockers):
        gates.append(_gate("NO_DATA_SOURCE", "BLOCKED", ["missing_dataset", missing_dataset or "data source missing"]))
    if name and name_counts[name] > 1:
        gates.append(_gate("DUPLICATE_HYPOTHESIS", "REJECT", ["duplicate normalized hypothesis name"]))
    if samples < MIN_INCLUDED_SAMPLES:
        gates.append(_gate("INCLUDED_SAMPLES_BELOW_MINIMUM", "UNDERPOWERED", [f"{samples}<{MIN_INCLUDED_SAMPLES}"]))
    if candidates == 0 and current_state != "NEEDS_DATA":
        gates.append(_gate("NO_CANDIDATE_FLOW", "BLOCKED" if time_in_state >= 2 else "WATCH", [f"time_in_state_days={time_in_state}"]))
    if positions == 0 and current_state not in {"NEEDS_DATA"}:
        gates.append(_gate("NO_OBSERVATION_FLOW", "BLOCKED" if time_in_state >= 2 else "WATCH", [f"time_in_state_days={time_in_state}"]))
    if positions == 0 and current_state not in {"PAPER_TRACKING_READY", "NEEDS_DATA"} and candidates == 0:
        gates.append(_gate("NO_PAPER_PATH", "BLOCKED", ["no linked paper positions or paper tracking readiness"]))
    missing = [key for key, path in paths.items() if key in {"research_portfolio", "hypothesis_workflow_state", "statistical_sufficiency"} and not path.exists()]
    if missing:
        gates.append(_gate("STALE_OR_MISSING_EVIDENCE", "BLOCKED", missing))
    return gates


def _decision_row(row: Mapping[str, Any], q_path: Path) -> dict[str, Any]:
    grades = row.get("grades") if isinstance(row.get("grades"), Mapping) else {}
    gates = row.get("active_hard_gate_codes") or []
    counts = row.get("sample_counts") if isinstance(row.get("sample_counts"), Mapping) else {}
    candidates = int(counts.get("candidate_count") or 0)
    positions = int(counts.get("paper_position_count") or 0)
    samples = int(counts.get("included_sample_count") or 0)
    reasons = list(row.get("reason_codes") or [])
    performance = row.get("outcome_performance_metrics") if isinstance(row.get("outcome_performance_metrics"), Mapping) else {}
    poor_underpowered = _performance_is_underpowered_poor(performance)
    positive_underpowered = _performance_is_underpowered_positive(performance)
    sufficient_poor = _performance_is_sufficient_poor(performance)
    sufficient_strong = _performance_is_sufficient_strong(performance)
    if (grades.get("data_quality") or {}).get("grade") == "BLOCKED":
        rec, extra = "NEEDS_DATA", ["DATA_QUALITY_BLOCKED"]
    elif "DUPLICATE_HYPOTHESIS" in gates:
        rec, extra = "RETIRE_RECOMMENDED", ["DUPLICATE_OR_INVALID_HYPOTHESIS"]
    elif sufficient_poor and not gates:
        rec, extra = "REDESIGN", ["SUFFICIENT_OUTCOME_PERFORMANCE_POOR_REDESIGN_REVIEW"]
    elif "NO_PAPER_PATH" in gates or (grades.get("implementation_completeness") or {}).get("grade") == "FAIL":
        rec, extra = "REDESIGN", ["IMPLEMENTATION_INCOMPLETE_OR_NO_PAPER_PATH"]
    elif poor_underpowered:
        rec, extra = "DECREASE_ATTENTION", ["UNDERPOWERED_EARLY_OUTCOME_PERFORMANCE_POOR_LOW_CONFIDENCE"]
    elif samples < MIN_INCLUDED_SAMPLES and (candidates or positions):
        rec, extra = "CONTINUE", ["UNDERPOWERED_BUT_PRODUCING_EVIDENCE", "UNDERPOWERED_POSITIVE_OUTCOMES_NOT_CAPITAL_REVIEW" if positive_underpowered else "UNDERPOWERED_OUTCOME_EVIDENCE_ACCUMULATING"]
    elif "PAPER_READINESS_CHECKLIST_PASSED" in reasons:
        rec, extra = "CONTINUE", ["PAPER_TRACKING_READY_AWAITING_CANDIDATE_FLOW"]
    elif not gates and sufficient_strong and (grades.get("validation_evidence") or {}).get("grade") == "PASS" and (grades.get("robustness") or {}).get("grade") == "PASS":
        rec, extra = "READY_FOR_CAPITAL_REVIEW", ["STATISTICAL_SUFFICIENCY_AND_VALIDATION_PASS", "SUFFICIENT_OUTCOME_PERFORMANCE_STRONG"]
    elif candidates >= 10 and samples >= MIN_INCLUDED_SAMPLES and (grades.get("validation_evidence") or {}).get("grade") in {"WATCH", "PASS"}:
        rec, extra = "INCREASE_ATTENTION", ["SAMPLE_PRODUCTION_STRONG"]
    elif candidates == 0 and positions == 0:
        rec, extra = "DECREASE_ATTENTION", ["SAMPLE_PRODUCTION_WEAK_NOT_BROKEN"]
    else:
        rec, extra = "DECREASE_ATTENTION", ["SAMPLE_PRODUCTION_WEAK_NOT_BROKEN"]
    if gates and rec == "READY_FOR_CAPITAL_REVIEW":
        rec, extra = "CONTINUE", ["HARD_GATE_PREVENTS_CAPITAL_REVIEW"]
    return {
        "hypothesis_id": row.get("hypothesis_id"),
        "name": row.get("name"),
        "recommendation": rec,
        "quality_status": row.get("quality_status"),
        "active_hard_gates": gates,
        "reason_codes": list(dict.fromkeys(reasons + extra)),
        "confidence_level": "LOW" if poor_underpowered else row.get("confidence_level") or "MEDIUM",
        "outcome_performance_metrics": dict(performance),
        "source_artifact_paths": [str(q_path)] + list(row.get("source_artifact_paths") or []),
        "input_artifact_hashes": {"aegis_research_quality_engine_v1": file_hash_v1(q_path)},
        "requires_david_review": rec in {"NEEDS_DATA", "REDESIGN", "RETIRE_RECOMMENDED", "READY_FOR_CAPITAL_REVIEW"},
        "decision_policy_version": DECISION_POLICY_VERSION,
        **SAFETY,
    }


def _allocation_row(row: Mapping[str, Any], current_weight: float, d_path: Path) -> dict[str, Any]:
    rec = text_v1(row.get("recommendation"))
    mapping = {
        "CONTINUE": ("HOLD", 0.0, False),
        "INCREASE_ATTENTION": ("INCREASE", 0.05, False),
        "DECREASE_ATTENTION": ("DECREASE", -0.05, False),
        "REDESIGN": ("PAUSE", -0.10, True),
        "NEEDS_DATA": ("PAUSE", -0.10, True),
        "RETIRE_RECOMMENDED": ("RETIRE_REVIEW", -current_weight, True),
        "READY_FOR_CAPITAL_REVIEW": ("CAPITAL_REVIEW", 0.0, True),
    }
    action, delta, review = mapping.get(rec, ("HOLD", 0.0, True))
    if row.get("active_hard_gates") and action == "CAPITAL_REVIEW":
        action, delta, review = "HOLD", 0.0, True
    return {
        "hypothesis_id": row.get("hypothesis_id"),
        "name": row.get("name"),
        "decision_recommendation": rec,
        "current_allocation_weight": round(float(current_weight), 6),
        "recommended_allocation_action": action,
        "recommended_weight_delta": round(float(delta), 6),
        "reason_codes": list(row.get("reason_codes") or []),
        "confidence_level": row.get("confidence_level") or "MEDIUM",
        "outcome_performance_metrics": dict(row.get("outcome_performance_metrics") or {}) if isinstance(row.get("outcome_performance_metrics"), Mapping) else {},
        "requires_david_review": bool(review or row.get("requires_david_review")),
        "source_artifact_paths": [str(d_path)] + list(row.get("source_artifact_paths") or []),
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        **SAFETY,
    }


def _follow_through_input_paths(root: Path, day_utc: str) -> dict[str, Path]:
    return {
        "aegis_research_quality_engine_v1": research_quality_engine_path_v1(truth_root=root, day_utc=day_utc),
        "aegis_hypothesis_decision_policy_v1": hypothesis_decision_policy_path_v1(truth_root=root, day_utc=day_utc),
        "aegis_research_allocation_recommendation_v1": research_allocation_recommendation_path_v1(truth_root=root, day_utc=day_utc),
        "aegis_hypothesis_workflow_state_v1": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day_utc, "hypothesis_workflow_state.v1.json"),
    }


def _follow_up_row(
    *,
    decision: Mapping[str, Any],
    quality: Mapping[str, Any],
    allocation: Mapping[str, Any],
    workflow: Mapping[str, Any],
    source_paths: Mapping[str, str],
    source_hashes: Mapping[str, str],
    day_utc: str,
) -> dict[str, Any]:
    hid = text_v1(decision.get("hypothesis_id"))
    name = text_v1(decision.get("name") or quality.get("name") or workflow.get("display_name") or workflow.get("name") or hid)
    recommendation = text_v1(decision.get("recommendation"))
    allocation_action = text_v1(allocation.get("recommended_allocation_action"))
    current_state = text_v1(workflow.get("current_state"))
    gates = set(text_v1(code) for code in (decision.get("active_hard_gates") or quality.get("active_hard_gate_codes") or []) if text_v1(code))
    counts = quality.get("sample_counts") if isinstance(quality.get("sample_counts"), Mapping) else {}
    candidates = int(counts.get("candidate_count") or 0)
    samples = int(counts.get("included_sample_count") or 0)
    state_age_days = int(workflow.get("time_in_state_days") or 0)
    reasons = list(dict.fromkeys([text_v1(code) for code in list(decision.get("reason_codes") or []) + list(quality.get("reason_codes") or []) + list(allocation.get("reason_codes") or []) if text_v1(code)]))

    if recommendation == "NEEDS_DATA":
        follow_type = "DATA_SOURCE_RESOLUTION"
        status = "OPEN"
        next_action = "PROVIDE_DATA_SOURCE"
        requires_david = True
        blocking = "shadow validation and promotion eligibility"
        expected = "connect, upload, or mark unavailable the missing data source; then re-run quality scoring"
        review_after_days = 1
    elif recommendation == "REDESIGN" or allocation_action == "PAUSE":
        follow_type = "REPAIR_INVESTIGATION"
        status = "OPEN"
        next_action = "INVESTIGATE_REPAIR"
        requires_david = True
        blocking = "research implementation repair, paper path restoration, or redirect decision"
        expected = "repair the investigation path, redirect the hypothesis, defer it, or move it to retirement review"
        review_after_days = 2
    elif current_state == "PAPER_TRACKING_READY" and candidates == 0:
        follow_type = "PAPER_TRACKING_FLOW_WATCH"
        status = "STALLED" if state_age_days >= 3 else "WATCHING"
        next_action = "REVIEW_REDIRECT" if status == "STALLED" else "MONITOR_AUTOMATICALLY"
        requires_david = status == "STALLED"
        blocking = "candidate generation has not started"
        expected = "observe first deterministic candidates or escalate to redesign if candidate flow stalls"
        review_after_days = 1
    elif recommendation == "CONTINUE" and "NO_CANDIDATE_FLOW" in gates:
        follow_type = "CANDIDATE_FLOW_WATCH"
        status = "STALLED" if state_age_days >= 3 else "WATCHING"
        next_action = "REVIEW_REDIRECT" if status == "STALLED" else "MONITOR_AUTOMATICALLY"
        requires_david = status == "STALLED"
        blocking = "candidate flow has not started"
        expected = "observe candidate flow or escalate the hypothesis to redesign"
        review_after_days = 1
    elif recommendation == "CONTINUE" and candidates > 0 and samples == 0:
        follow_type = "CANDIDATE_FLOW_WATCH"
        status = "FLOW_STARTED"
        next_action = "MONITOR_AUTOMATICALLY"
        requires_david = False
        blocking = "candidates exist but validation samples have not accumulated"
        expected = "candidate flow should convert into included validation samples"
        review_after_days = 1
    elif recommendation == "CONTINUE" and samples < MIN_INCLUDED_SAMPLES:
        follow_type = "SAMPLE_ACCUMULATION_WATCH"
        status = "SAMPLE_FLOW_OK" if samples > 0 else "WATCHING"
        next_action = "MONITOR_AUTOMATICALLY"
        requires_david = False
        blocking = "statistical sufficiency"
        expected = "continue deterministic observation until included samples reach the minimum sufficiency threshold"
        review_after_days = 5
    else:
        follow_type = "SAMPLE_ACCUMULATION_WATCH"
        status = "SUFFICIENCY_REACHED" if samples >= MIN_INCLUDED_SAMPLES else "WATCHING"
        next_action = "MONITOR_AUTOMATICALLY"
        requires_david = False
        blocking = "ongoing validation evidence monitoring"
        expected = "re-score after new validation evidence is produced"
        review_after_days = 5

    follow_id = "follow_up_" + stable_hash_v1({"day_utc": day_utc, "hypothesis_id": hid, "follow_up_type": follow_type})[:24]
    return {
        "follow_up_id": follow_id,
        "hypothesis_id": hid,
        "hypothesis_name": name,
        "source_recommendation": recommendation,
        "source_quality_hash": source_hashes.get("aegis_research_quality_engine_v1", ""),
        "source_decision_hash": source_hashes.get("aegis_hypothesis_decision_policy_v1", ""),
        "source_allocation_recommendation_hash": source_hashes.get("aegis_research_allocation_recommendation_v1", ""),
        "follow_up_type": follow_type,
        "current_status": status,
        "prior_status": "",
        "reason_codes": reasons or ["FOLLOW_THROUGH_REQUIRED"],
        "blocking_what": blocking,
        "expected_resolution": expected,
        "review_after_days": review_after_days,
        "state_age_days": state_age_days,
        "next_action": next_action,
        "requires_david_action": requires_david,
        "source_artifact_paths": dict(source_paths),
        "source_artifact_hashes": dict(source_hashes),
        "computed_at_utc": _now(),
        "re_score_trigger": "RUN_RESEARCH_QUALITY_ENGINE_AFTER_FOLLOW_UP_RESULT",
        "is_overdue": False,
        "allocation_mutation_performed": False,
        "automatic_repair_performed": False,
        **SAFETY,
    }


def _grade(grade: str, reasons: list[str], paths: list[str], hashes: Mapping[str, str], counts: Mapping[str, Any], confidence: str) -> dict[str, Any]:
    return {"grade": grade, "reason_codes": reasons or ["NO_REASON_CODE"], "source_artifact_paths": paths, "source_artifact_hashes": dict(hashes), "sample_counts": dict(counts), "confidence_level": confidence, "scoring_version": SCORING_VERSION}


def _gate(code: str, severity: str, reasons: list[str]) -> dict[str, Any]:
    return {"gate_code": code, "severity": severity, "reason_codes": reasons}


def _count(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    return len(value) if isinstance(value, list) else int(value or 0) if isinstance(value, int) else 0


def _input_generated_at(inputs: Mapping[str, Any]) -> str:
    values = []
    for payload in inputs.values():
        if isinstance(payload, Mapping):
            value = payload.get("generated_at") or payload.get("computed_at_utc") or payload.get("generated_at_utc")
            if value:
                values.append(str(value))
    return "|".join(sorted(values))


def _load_or_build_quality(root: Path, day_utc: str) -> dict[str, Any]:
    payload = read_json_v1(research_quality_engine_path_v1(truth_root=root, day_utc=day_utc))
    return payload or build_research_quality_engine_v1(truth_root=root, day_utc=day_utc)


def _load_or_build_decisions(root: Path, day_utc: str) -> dict[str, Any]:
    payload = read_json_v1(hypothesis_decision_policy_path_v1(truth_root=root, day_utc=day_utc))
    return payload or build_hypothesis_decision_policy_v1(truth_root=root, day_utc=day_utc)


def _load_or_build_allocation(root: Path, day_utc: str) -> dict[str, Any]:
    payload = read_json_v1(research_allocation_recommendation_path_v1(truth_root=root, day_utc=day_utc))
    return payload or build_research_allocation_recommendation_v1(truth_root=root, day_utc=day_utc)


def _decision_values() -> list[str]:
    return ["CONTINUE", "INCREASE_ATTENTION", "DECREASE_ATTENTION", "REDESIGN", "NEEDS_DATA", "RETIRE_RECOMMENDED", "READY_FOR_CAPITAL_REVIEW"]


def _allocation_values() -> list[str]:
    return ["HOLD", "INCREASE", "DECREASE", "PAUSE", "RETIRE_REVIEW", "CAPITAL_REVIEW"]


def _without_generated_time(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _without_generated_time(v) for k, v in value.items() if k not in {"computed_at_utc", "generated_at", "generated_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated_time(v) for v in value]
    return value


def _norm(value: Any) -> str:
    return " ".join(text_v1(value).lower().split())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
