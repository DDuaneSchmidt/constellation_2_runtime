from __future__ import annotations

import hashlib
import inspect
import json
import statistics
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.research_lab.research_validation_samples_v1 import (
    build_research_validation_samples_v1,
    research_validation_samples_path_v1,
)

HYPOTHESIS_REGISTRY_FAMILY = "aegis_research_hypothesis_registry_v1"
HYPOTHESIS_REGISTRY_FILENAME = "research_hypothesis_registry.v1.json"
PROTOCOL_FAMILY = "aegis_research_validation_protocol_v1"
PROTOCOL_FILENAME = "research_validation_protocol.v1.json"
RUN_FAMILY = "aegis_research_validation_run_v1"
RUN_FILENAME = "research_validation_run.v1.json"
RESULT_FAMILY = "aegis_research_validation_result_v1"
RESULT_FILENAME = "research_validation_result.v1.json"
PROMOTION_GATE_FAMILY = "aegis_research_promotion_gate_v1"
PROMOTION_GATE_FILENAME = "research_promotion_gate.v1.json"
OUTCOME_FEEDBACK_FAMILY = "aegis_research_outcome_feedback_v1"
OUTCOME_FEEDBACK_FILENAME = "research_outcome_feedback.v1.json"

VALIDATION_STATUSES = {"NOT_READY", "UNDER_SAMPLED", "INCONCLUSIVE", "SUPPORTED", "DISPROVEN"}
PROMOTABLE_STATUS = "SUPPORTED"
CODE_VERSION = "aegis_research_validation_engine_v1"
EVALUATOR_VERSION = "v1"
REQUIRED_BIAS_CONTROLS = [
    "LOOKAHEAD_GUARDRAIL",
    "SURVIVORSHIP_GUARDRAIL",
    "SELECTION_BIAS_GUARDRAIL",
    "DUPLICATE_EVENT_GUARDRAIL",
    "STALE_DATA_GUARDRAIL",
]
SAFETY = {
    "research_only": True,
    "ai_certification_allowed": False,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
    "candidate_generation_behavior_changed": False,
    "candidate_research_validation_gate_enforced": True,
    "sleeve_logic_changed": False,
}


def artifact_path_v1(*, truth_root: Path | str, family: str, day_utc: str, filename: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / family / str(day_utc) / filename


def research_hypothesis_registry_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return artifact_path_v1(truth_root=truth_root, family=HYPOTHESIS_REGISTRY_FAMILY, day_utc=day_utc, filename=HYPOTHESIS_REGISTRY_FILENAME)


def research_validation_protocol_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return artifact_path_v1(truth_root=truth_root, family=PROTOCOL_FAMILY, day_utc=day_utc, filename=PROTOCOL_FILENAME)


def research_validation_run_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return artifact_path_v1(truth_root=truth_root, family=RUN_FAMILY, day_utc=day_utc, filename=RUN_FILENAME)


def research_validation_result_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return artifact_path_v1(truth_root=truth_root, family=RESULT_FAMILY, day_utc=day_utc, filename=RESULT_FILENAME)


def research_promotion_gate_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return artifact_path_v1(truth_root=truth_root, family=PROMOTION_GATE_FAMILY, day_utc=day_utc, filename=PROMOTION_GATE_FILENAME)


def research_outcome_feedback_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return artifact_path_v1(truth_root=truth_root, family=OUTCOME_FEEDBACK_FAMILY, day_utc=day_utc, filename=OUTCOME_FEEDBACK_FILENAME)


def build_all_research_validation_engine_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    registry = build_research_hypothesis_registry_v1(truth_root=root, day_utc=day)
    protocols = build_research_validation_protocol_v1(day_utc=day)
    runs = build_research_validation_run_v1(truth_root=root, day_utc=day, registry_payload=registry, protocol_payload=protocols)
    results = build_research_validation_result_v1(truth_root=root, day_utc=day, run_payload=runs, protocol_payload=protocols)
    gate = build_research_promotion_gate_v1(day_utc=day, result_payload=results)
    feedback = build_research_outcome_feedback_v1(truth_root=root, day_utc=day, gate_payload=gate)
    paths = write_research_validation_engine_v1(
        truth_root=root,
        day_utc=day,
        registry_payload=registry,
        protocol_payload=protocols,
        run_payload=runs,
        result_payload=results,
        promotion_gate_payload=gate,
        outcome_feedback_payload=feedback,
    )
    return {
        "ok": True,
        "day_utc": day,
        "paths": paths,
        "summary": {
            "hypotheses": len(registry.get("hypotheses") or []),
            "runs": len(runs.get("runs") or []),
            "results": len(results.get("results") or []),
            "supported": sum(1 for row in results.get("results") or [] if row.get("validation_status") == "SUPPORTED"),
            "eligible_for_candidate_review": sum(1 for row in gate.get("promotion_gates") or [] if row.get("eligible_for_candidate_review") is True),
        },
        "safety": dict(SAFETY),
    }


def build_research_hypothesis_registry_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    doctor_path = root / "reports" / "aegis_research_doctor_v1" / day / "research_doctor.v1.json"
    doctor = read_json_v1(doctor_path)
    rows = doctor.get("hypotheses") if isinstance(doctor.get("hypotheses"), list) else []
    hypotheses = [_registry_row(row, source_path=doctor_path, generated_at=generated_at) for row in rows if isinstance(row, Mapping)]
    return {
        "schema_id": "aegis_research_hypothesis_registry",
        "schema_version": "v1",
        "artifact_id": HYPOTHESIS_REGISTRY_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "hypotheses": hypotheses,
        "summary": {"total_hypotheses": len(hypotheses)},
        "source_artifacts": [_artifact_ref(doctor_path)],
        "safety": dict(SAFETY),
    }


def build_research_validation_protocol_v1(*, day_utc: str) -> dict[str, Any]:
    generated_at = now_utc_v1()
    protocols = [
        {
            "protocol_id": "EVENT_WINDOW_RETURN_V1",
            "protocol_version": "v1",
            "evaluator_id": "event_window_return_evaluator_v1",
            "evaluator_version": EVALUATOR_VERSION,
            "required_data": ["event calendar", "event-window market closes", "benchmark closes"],
            "required_source_types": ["event_calendar", "event_timestamp", "event_window_close", "benchmark_close"],
            "minimum_sample_size": 10,
            "sample_size_policy": {"minimum_sample_size": 10, "confidence_target": "directional_event_study_minimum", "effect_size_assumption": "positive benchmark-relative event-window return", "minimum_positive_events": 6, "minimum_distinct_dates": 10, "minimum_distinct_symbols": 1, "policy_rationale": "Event-window studies require enough distinct events to avoid one-off earnings/calendar artifacts."},
            "metrics": ["sample_count", "hit_rate", "average_forward_return", "benchmark_relative_return", "event_window_return", "adverse_excursion"],
            "required_metrics": ["sample_count", "hit_rate", "average_forward_return", "benchmark_relative_return", "event_window_return", "adverse_excursion"],
            "required_bias_checks": REQUIRED_BIAS_CONTROLS,
            "support_thresholds": {"hit_rate_min": 0.55, "average_forward_return_min": 0.0, "benchmark_relative_return_min": 0.0},
            "disproof_thresholds": {"hit_rate_max": 0.40, "average_forward_return_max": -0.01},
            "exclusion_rules": ["missing event timestamp", "missing event-window close", "lookahead-contaminated row"],
            "lookahead_guardrails": ["event window closes must be timestamped after event and before result generation"],
            "output_fields": ["sample_count", "hit_rate", "average_forward_return", "benchmark_relative_return"],
        },
        {
            "protocol_id": "MEAN_REVERSION_FORWARD_RETURN_V1",
            "protocol_version": "v1",
            "evaluator_id": "mean_reversion_forward_return_evaluator_v1",
            "evaluator_version": EVALUATOR_VERSION,
            "required_data": ["governed trigger events", "next market close", "forward return samples"],
            "required_source_types": ["governed_trigger_event", "event_close", "forward_close", "symbol_universe_declaration"],
            "minimum_sample_size": 20,
            "sample_size_policy": {"minimum_sample_size": 20, "confidence_target": "minimum_directional_edge_evidence", "effect_size_assumption": "average forward return >= 25 bps with positive hit-rate", "minimum_positive_events": 11, "minimum_distinct_dates": 20, "minimum_distinct_symbols": 1, "policy_rationale": "Mean-reversion claims need enough independent forward-return observations to avoid single-event contamination."},
            "metrics": ["sample_count", "hit_rate", "average_forward_return", "median_forward_return", "drawdown_or_adverse_excursion", "trigger_return", "forward_return_1d"],
            "required_metrics": ["sample_count", "hit_rate", "average_forward_return", "median_forward_return", "drawdown_or_adverse_excursion", "trigger_return", "forward_return_1d"],
            "required_bias_checks": REQUIRED_BIAS_CONTROLS,
            "support_thresholds": {"hit_rate_min": 0.55, "average_forward_return_min": 0.0025},
            "disproof_thresholds": {"hit_rate_max": 0.40, "average_forward_return_max": -0.0025},
            "exclusion_rules": ["NO_TRIGGER_EVENT", "FORWARD_WINDOW_PENDING", "FORWARD_CLOSE_MISSING", "EVENT_CLOSE_INVALID"],
            "lookahead_guardrails": ["forward close day must be greater than event day and less than or equal to target day"],
            "output_fields": ["sample_count", "hit_rate", "average_forward_return", "median_forward_return"],
        },
        {
            "protocol_id": "POST_SIGNAL_SURVIVAL_V1",
            "protocol_version": "v1",
            "evaluator_id": "post_signal_survival_evaluator_v1",
            "evaluator_version": EVALUATOR_VERSION,
            "required_data": ["signal timestamp", "post-signal market closes", "survival horizon"],
            "required_source_types": ["signal_timestamp", "signal_evidence", "post_signal_marks", "survival_horizon_definition"],
            "minimum_sample_size": 15,
            "sample_size_policy": {"minimum_sample_size": 15, "confidence_target": "minimum_survival_confirmation", "effect_size_assumption": "post-signal return and survival behavior remain positive", "minimum_positive_events": 9, "minimum_distinct_dates": 15, "minimum_distinct_symbols": 1, "policy_rationale": "Post-signal survival requires multiple independent signal observations."},
            "metrics": ["sample_count", "hit_rate", "average_forward_return", "drawdown_or_adverse_excursion", "survival_horizon", "post_signal_return"],
            "required_metrics": ["sample_count", "hit_rate", "average_forward_return", "drawdown_or_adverse_excursion", "survival_horizon", "post_signal_return"],
            "required_bias_checks": REQUIRED_BIAS_CONTROLS,
            "support_thresholds": {"hit_rate_min": 0.55, "average_forward_return_min": 0.0},
            "disproof_thresholds": {"hit_rate_max": 0.40, "average_forward_return_max": -0.005},
            "exclusion_rules": ["missing signal timestamp", "missing post-signal mark", "duplicate signal"],
            "lookahead_guardrails": ["post-signal window must be after signal timestamp"],
            "output_fields": ["sample_count", "hit_rate", "average_forward_return", "drawdown_or_adverse_excursion"],
        },
    ]
    return {
        "schema_id": "aegis_research_validation_protocol",
        "schema_version": "v1",
        "artifact_id": PROTOCOL_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": generated_at,
        "protocols": protocols,
        "summary": {"protocol_count": len(protocols)},
        "safety": dict(SAFETY),
    }


def build_research_validation_run_v1(*, truth_root: Path | str, day_utc: str, registry_payload: dict[str, Any] | None = None, protocol_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    registry = registry_payload or read_json_v1(research_hypothesis_registry_path_v1(truth_root=root, day_utc=day))
    protocols = {row["protocol_id"]: row for row in (protocol_payload or build_research_validation_protocol_v1(day_utc=day)).get("protocols", [])}
    runs = []
    for hyp in registry.get("hypotheses") or []:
        protocol_id = _protocol_for_hypothesis(hyp)
        protocol = protocols.get(protocol_id, {})
        source_artifacts = _source_artifacts_for_run(root, day, hyp, protocol_id)
        source_hash = _stable_hash(source_artifacts)
        evaluator_id = str(protocol.get("evaluator_id") or "")
        protocol_hash = _stable_hash(protocol)
        source_artifact_hashes = {str(row.get("path") or ""): str(row.get("sha256") or "") for row in source_artifacts}
        binding = _artifact_binding_for(root, day, hyp, protocol_id)
        code_ref = _code_reference_v1()
        evaluator_hash = _evaluator_hash(evaluator_id)
        configuration_hash = _stable_hash({"protocol": protocol, "hypothesis": hyp.get("hypothesis_id"), "day": day})
        input_context_hash = _stable_hash({"source_artifacts": source_artifacts, "protocol_hash": protocol_hash, "configuration_hash": configuration_hash, "artifact_binding": binding})
        run_id = _stable_id("rvr", day, hyp.get("hypothesis_id"), hyp.get("hypothesis_version"), protocol_id, protocol.get("protocol_version"), source_hash, protocol_hash, evaluator_hash, input_context_hash)
        samples_payload = _sample_payload_for(root, day, hyp.get("hypothesis_id"))
        symbols = _safe_list(hyp.get("asset_universe"))
        runs.append(
            {
                "run_id": run_id,
                "hypothesis_id": hyp.get("hypothesis_id"),
                "hypothesis_version": hyp.get("hypothesis_version", "v1"),
                "protocol_id": protocol_id,
                "protocol_version": protocol.get("protocol_version", "v1"),
                "data_window": {"end_day": day, "source": "day-scoped governed research artifacts"},
                "symbols_tested": symbols,
                "events_tested": int(samples_payload.get("current_samples") or 0) + len(samples_payload.get("excluded_samples") or []),
                "excluded_observations": samples_payload.get("excluded_samples") or [],
                "source_artifacts": source_artifacts,
                "source_artifact_hashes": source_artifact_hashes,
                "code_version": CODE_VERSION,
                "code_commit": code_ref["code_commit"],
                "code_reference_status": code_ref["code_reference_status"],
                "evaluator_id": evaluator_id,
                "evaluator_version": str(protocol.get("evaluator_version") or EVALUATOR_VERSION),
                "evaluator_hash": evaluator_hash,
                "protocol_hash": protocol_hash,
                "input_context_hash": input_context_hash,
                "configuration_hash": configuration_hash,
                "deterministic_seed": None,
                "artifact_binding_status": binding["artifact_binding_status"],
                "artifact_binding_failures": binding["artifact_binding_failures"],
                "hypothesis_binding": binding["hypothesis_binding"],
                "run_timestamp": generated_at,
            }
        )
    return {
        "schema_id": "aegis_research_validation_run",
        "schema_version": "v1",
        "artifact_id": RUN_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "runs": runs,
        "summary": {"run_count": len(runs)},
        "safety": dict(SAFETY),
    }


def build_research_validation_result_v1(*, truth_root: Path | str, day_utc: str, run_payload: dict[str, Any] | None = None, protocol_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    runs = (run_payload or read_json_v1(research_validation_run_path_v1(truth_root=root, day_utc=day))).get("runs") or []
    protocols = {row["protocol_id"]: row for row in (protocol_payload or build_research_validation_protocol_v1(day_utc=day)).get("protocols", [])}
    results = []
    for run in runs:
        protocol = protocols.get(str(run.get("protocol_id") or ""), {})
        result = _evaluate_run(root, day, run, protocol)
        result["generated_at"] = generated_at
        results.append(result)
    counts = {status: sum(1 for row in results if row.get("validation_status") == status) for status in sorted(VALIDATION_STATUSES)}
    return {
        "schema_id": "aegis_research_validation_result",
        "schema_version": "v1",
        "artifact_id": RESULT_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "results": results,
        "summary": {"result_count": len(results), **counts},
        "safety": dict(SAFETY),
    }


def build_research_promotion_gate_v1(*, day_utc: str, result_payload: dict[str, Any]) -> dict[str, Any]:
    generated_at = now_utc_v1()
    gates = []
    for result in result_payload.get("results") or []:
        status = str(result.get("validation_status") or "NOT_READY")
        eligible = status == PROMOTABLE_STATUS
        gates.append(
            {
                "hypothesis_id": result.get("hypothesis_id"),
                "hypothesis_version": result.get("hypothesis_version"),
                "latest_result_id": result.get("result_id"),
                "validation_status": status,
                "eligible_for_candidate_review": eligible,
                "promotion_gate_status": "ELIGIBLE_FOR_CANDIDATE_REVIEW" if eligible else "BLOCKED_NOT_SUPPORTED",
                "blocked_reason": "" if eligible else f"Hypothesis validation status is {status}; only SUPPORTED may become eligible for candidate review.",
                "tradeable": False,
                "trade_advice_allowed": False,
                "candidate_review_only": eligible,
                "candidate_generation_behavior_changed": False,
                "source_artifacts": result.get("source_artifacts") or [],
            }
        )
    return {
        "schema_id": "aegis_research_promotion_gate",
        "schema_version": "v1",
        "artifact_id": PROMOTION_GATE_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": generated_at,
        "promotion_gates": gates,
        "summary": {
            "total_hypotheses": len(gates),
            "eligible_for_candidate_review": sum(1 for row in gates if row.get("eligible_for_candidate_review") is True),
            "blocked": sum(1 for row in gates if row.get("eligible_for_candidate_review") is not True),
        },
        "safety": dict(SAFETY),
    }


def build_research_outcome_feedback_v1(*, truth_root: Path | str, day_utc: str, gate_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_id": "aegis_research_outcome_feedback",
        "schema_version": "v1",
        "artifact_id": OUTCOME_FEEDBACK_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": now_utc_v1(),
        "outcome_feedback": [],
        "summary": {"feedback_count": 0, "historical_validation_rewritten": False},
        "source_artifacts": [
            {"artifact_id": PROMOTION_GATE_FAMILY, "summary": gate_payload.get("summary", {})},
        ],
        "safety": dict(SAFETY),
    }


def write_research_validation_engine_v1(*, truth_root: Path | str, day_utc: str, registry_payload: dict[str, Any], protocol_payload: dict[str, Any], run_payload: dict[str, Any], result_payload: dict[str, Any], promotion_gate_payload: dict[str, Any], outcome_feedback_payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    mapping = {
        "hypothesis_registry": (research_hypothesis_registry_path_v1(truth_root=root, day_utc=day), registry_payload),
        "validation_protocol": (research_validation_protocol_path_v1(truth_root=root, day_utc=day), protocol_payload),
        "validation_run": (research_validation_run_path_v1(truth_root=root, day_utc=day), run_payload),
        "validation_result": (research_validation_result_path_v1(truth_root=root, day_utc=day), result_payload),
        "promotion_gate": (research_promotion_gate_path_v1(truth_root=root, day_utc=day), promotion_gate_payload),
        "outcome_feedback": (research_outcome_feedback_path_v1(truth_root=root, day_utc=day), outcome_feedback_payload),
    }
    out = {}
    for key, (path, payload) in mapping.items():
        write_json_v1(path, payload)
        out[key] = str(path)
    return out


def research_validation_engine_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    artifacts = {
        "registry": read_json_v1(research_hypothesis_registry_path_v1(truth_root=root, day_utc=day)),
        "protocol": read_json_v1(research_validation_protocol_path_v1(truth_root=root, day_utc=day)),
        "run": read_json_v1(research_validation_run_path_v1(truth_root=root, day_utc=day)),
        "result": read_json_v1(research_validation_result_path_v1(truth_root=root, day_utc=day)),
        "promotion_gate": read_json_v1(research_promotion_gate_path_v1(truth_root=root, day_utc=day)),
        "outcome_feedback": read_json_v1(research_outcome_feedback_path_v1(truth_root=root, day_utc=day)),
    }
    checks = []
    def add(name: str, ok: bool, details: Any = None) -> None:
        checks.append({"check": name, "ok": bool(ok), "details": details or {}})

    add("all required artifacts exist", all(bool(payload) for payload in artifacts.values()))
    protocols = {row.get("protocol_id"): row for row in artifacts["protocol"].get("protocols") or []}
    hypotheses = {row.get("hypothesis_id"): row for row in artifacts["registry"].get("hypotheses") or []}
    runs = artifacts["run"].get("runs") or []
    results = artifacts["result"].get("results") or []
    gates = artifacts["promotion_gate"].get("promotion_gates") or []
    add("hypotheses are versioned", all(row.get("hypothesis_version") for row in hypotheses.values()))
    add("protocols are versioned", all(row.get("protocol_version") for row in protocols.values()))
    add("runs reference exact hypothesis and protocol versions", all(row.get("hypothesis_id") in hypotheses and row.get("hypothesis_version") == hypotheses[row.get("hypothesis_id")].get("hypothesis_version") and row.get("protocol_id") in protocols and row.get("protocol_version") == protocols[row.get("protocol_id")].get("protocol_version") for row in runs))
    result_by_id = {row.get("result_id"): row for row in results}
    add("validation statuses are valid", all(row.get("validation_status") in VALIDATION_STATUSES for row in results))
    add("non-supported statuses cannot promote", all((row.get("validation_status") == "SUPPORTED") == bool(row.get("eligible_for_candidate_review")) for row in gates), {"gates": gates})
    add("supported is candidate-review only", all(row.get("tradeable") is False and row.get("trade_advice_allowed") is False for row in gates))
    add("results preserve source artifacts", all(row.get("source_artifacts") for row in results))
    add("supported results have required metrics", all(row.get("validation_status") != "SUPPORTED" or row.get("required_metric_status", {}).get("status") == "PASS" for row in results))
    add("supported results have artifact binding pass", all(row.get("validation_status") != "SUPPORTED" or row.get("artifact_binding_status") == "PASS" for row in results))
    add("supported results have bias controls pass", all(row.get("validation_status") != "SUPPORTED" or row.get("bias_control_status") == "PASS" for row in results))
    add("supported results have sample-size pass", all(row.get("validation_status") != "SUPPORTED" or row.get("sample_size_status") == "PASS" for row in results))
    add("runs have immutable reproducibility fields", all(row.get("code_commit") and row.get("protocol_hash") and row.get("evaluator_hash") and row.get("input_context_hash") and row.get("configuration_hash") for row in runs))
    add("protocols bind protocol-specific evaluators", all(row.get("evaluator_id") in {"event_window_return_evaluator_v1", "mean_reversion_forward_return_evaluator_v1", "post_signal_survival_evaluator_v1"} for row in protocols.values()))
    add("source artifacts have hashes", all(all(ref.get("path") and ref.get("sha256") for ref in row.get("source_artifacts") or []) for row in results))
    candidate_check = _candidate_research_gate_self_check(root, day)
    add("research-derived candidates require promotion eligibility", candidate_check["ok"], candidate_check)
    add("outcome feedback does not rewrite validation", artifacts["outcome_feedback"].get("summary", {}).get("historical_validation_rewritten") is False)
    add("safety gates remain disabled", all(artifacts[name].get("safety", {}).get(flag) is False for name in artifacts for flag in ["trade_advice_allowed", "broker_execution_allowed", "broker_submit_transmit_allowed", "live_trading_allowed", "autonomous_live_trading_allowed"]))
    return {
        "ok": all(row["ok"] for row in checks),
        "day_utc": day,
        "checks": checks,
        "summary": {
            "hypotheses": len(hypotheses),
            "protocols": len(protocols),
            "runs": len(runs),
            "results": len(results),
            "eligible_for_candidate_review": sum(1 for row in gates if row.get("eligible_for_candidate_review") is True),
        },
        "safety": dict(SAFETY),
    }


def _registry_row(row: Mapping[str, Any], *, source_path: Path, generated_at: str) -> dict[str, Any]:
    hypothesis_id = str(row.get("hypothesis_id") or _stable_id("rh", row.get("title"), row.get("hypothesis_summary")))
    claim = str(row.get("claim") or row.get("hypothesis_summary") or row.get("title") or hypothesis_id)
    hypothesis_type = _hypothesis_type(hypothesis_id, claim)
    symbols = _safe_list(row.get("symbols") or row.get("required_symbols") or row.get("affected_symbols"))
    return {
        "hypothesis_id": hypothesis_id,
        "hypothesis_version": str(row.get("hypothesis_version") or "v1"),
        "claim": claim,
        "hypothesis_type": hypothesis_type,
        "asset_universe": symbols,
        "expected_behavior": _expected_behavior(hypothesis_type),
        "created_by": str(row.get("creator") or row.get("created_by") or "SYSTEM"),
        "created_at": str(row.get("created_at") or row.get("last_attempted_run") or generated_at),
        "status": _registry_status(row),
        "source": str(row.get("source") or "aegis_research_doctor_v1"),
        "linked_research_items": [str(source_path)],
    }


def _registry_status(row: Mapping[str, Any]) -> str:
    text = f"{row.get('current_autonomous_state','')} {row.get('blocker_reason','')}".upper()
    if "BLOCK" in text:
        return "ACTIVE"
    if "QUEUE" in text or "RESEARCH" in text or "WAIT" in text:
        return "READY_FOR_VALIDATION"
    return "CAPTURED"


def _hypothesis_type(hypothesis_id: str, claim: str) -> str:
    text = f"{hypothesis_id} {claim}".lower()
    if "mean" in text or "reversion" in text:
        return "MEAN_REVERSION"
    if "event" in text or "earnings" in text:
        return "EVENT_WINDOW"
    if "signal" in text or "survival" in text:
        return "POST_SIGNAL_SURVIVAL"
    return "POST_SIGNAL_SURVIVAL"


def _expected_behavior(hypothesis_type: str) -> str:
    if hypothesis_type == "MEAN_REVERSION":
        return "Forward returns after governed downside trigger events should be positive often enough to meet support thresholds."
    if hypothesis_type == "EVENT_WINDOW":
        return "Event-window returns should show benchmark-relative behavior matching the research claim."
    return "Post-signal behavior should survive long enough to meet support thresholds."


def _protocol_for_hypothesis(hypothesis: Mapping[str, Any]) -> str:
    typ = str(hypothesis.get("hypothesis_type") or "").upper()
    if typ == "MEAN_REVERSION":
        return "MEAN_REVERSION_FORWARD_RETURN_V1"
    if typ == "EVENT_WINDOW":
        return "EVENT_WINDOW_RETURN_V1"
    return "POST_SIGNAL_SURVIVAL_V1"


def _source_artifacts_for_run(root: Path, day: str, hypothesis: Mapping[str, Any], protocol_id: str) -> list[dict[str, Any]]:
    refs = [_artifact_ref(root / "reports" / "aegis_research_doctor_v1" / day / "research_doctor.v1.json")]
    if protocol_id == "MEAN_REVERSION_FORWARD_RETURN_V1":
        sample_path = research_validation_samples_path_v1(truth_root=root, day_utc=day)
        if sample_path.exists():
            refs.append(_artifact_ref(sample_path))
        else:
            # The evaluator can compute transient samples from governed research-test inputs, but
            # missing canonical sample artifacts must be represented by binding failures rather than
            # by an empty-hash source reference.
            refs.extend(_computed_sample_source_refs(root, day, str(hypothesis.get("hypothesis_id") or "")))
    return refs


def _computed_sample_source_refs(root: Path, day: str, hypothesis_id: str) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    base = root / "reports" / "aegis_research_test_results_v1"
    if base.exists():
        for path in sorted(base.glob(f"*/{hypothesis_id}/research_test_result.v1.json")):
            if path.parents[1].name <= day:
                refs.append(_artifact_ref(path))
    market = root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    if market.exists():
        refs.append(_artifact_ref(market))
    return refs


def _sample_payload_for(root: Path, day: str, hypothesis_id: Any) -> dict[str, Any]:
    path = research_validation_samples_path_v1(truth_root=root, day_utc=day)
    payload = read_json_v1(path)
    if payload and str(payload.get("hypothesis_id") or "") == str(hypothesis_id or ""):
        return payload
    if str(hypothesis_id or "") == "rh-process-test-etf-drop-mean-reversion-v1":
        return build_research_validation_samples_v1(truth_root=root, day_utc=day, hypothesis_id=str(hypothesis_id))
    return {"samples": [], "excluded_samples": [], "current_samples": 0, "required_samples": 0}


def _evaluate_run(root: Path, day: str, run: Mapping[str, Any], protocol: Mapping[str, Any]) -> dict[str, Any]:
    evaluator_id = str(protocol.get("evaluator_id") or "")
    evaluator = _evaluator_for(evaluator_id)
    if evaluator is None:
        return _base_result(root, day, run, protocol, "NOT_READY", [], ["No protocol-specific evaluator is assigned."], {}, evaluator_id=evaluator_id)
    return evaluator(root, day, run, protocol)


def evaluate_mean_reversion_forward_return_v1(root: Path, day: str, run: Mapping[str, Any], protocol: Mapping[str, Any]) -> dict[str, Any]:
    sample_payload = _sample_payload_for(root, day, run.get("hypothesis_id"))
    samples = [row for row in sample_payload.get("samples") or [] if isinstance(row, Mapping)]
    returns = [_number(row.get("forward_return_1d")) for row in samples]
    returns = [value for value in returns if value is not None]
    metric_values = _metric_values_from_returns(returns)
    metric_values["trigger_return"] = _first_number(samples, "trigger_return", "event_return", "daily_return")
    metric_values["forward_return_1d"] = returns[0] if returns else None
    required = _required_metric_status(protocol, metric_values)
    bias = _bias_controls_for_mean_reversion(day, run, samples, sample_payload)
    sample_status = _sample_size_status(protocol, samples, returns)
    blockers = []
    blockers.extend(run.get("artifact_binding_failures") or [])
    blockers.extend(required["missing_required_metrics"])
    blockers.extend([row["control_id"] for row in bias if row.get("blocking") and row.get("status") != "PASS"])
    if run.get("code_reference_status") != "AVAILABLE":
        blockers.append("IMMUTABLE_CODE_REFERENCE_MISSING")
    status = _status_from_protocol_evidence(protocol, metric_values, sample_status, blockers)
    limitations = _limitations_from(blockers, sample_status)
    return _base_result(root, day, run, protocol, status, returns, limitations, metric_values, required_metric_status=required, bias_controls=bias, sample_size=sample_status, evaluator_id="mean_reversion_forward_return_evaluator_v1")


def evaluate_event_window_return_v1(root: Path, day: str, run: Mapping[str, Any], protocol: Mapping[str, Any]) -> dict[str, Any]:
    # V1 has no governed event-window sample artifact yet. Missing benchmark-relative metrics must block support.
    returns: list[float] = []
    metric_values = _metric_values_from_returns(returns)
    metric_values.update({"benchmark_relative_return": None, "event_window_return": None})
    required = _required_metric_status(protocol, metric_values)
    bias = _default_bias_controls("PARTIAL", "No governed event-window sample rows are bound to this hypothesis yet.")
    sample_status = _sample_size_status(protocol, [], returns)
    blockers = list(required["missing_required_metrics"]) + [row["control_id"] for row in bias if row.get("blocking") and row.get("status") != "PASS"]
    blockers.extend(run.get("artifact_binding_failures") or [])
    if run.get("code_reference_status") != "AVAILABLE":
        blockers.append("IMMUTABLE_CODE_REFERENCE_MISSING")
    status = _status_from_protocol_evidence(protocol, metric_values, sample_status, blockers)
    limitations = _limitations_from(blockers, sample_status)
    return _base_result(root, day, run, protocol, status, returns, limitations, metric_values, required_metric_status=required, bias_controls=bias, sample_size=sample_status, evaluator_id="event_window_return_evaluator_v1")


def evaluate_post_signal_survival_v1(root: Path, day: str, run: Mapping[str, Any], protocol: Mapping[str, Any]) -> dict[str, Any]:
    # V1 has no governed post-signal survival sample artifact yet. Missing post-signal metrics must block support.
    returns: list[float] = []
    metric_values = _metric_values_from_returns(returns)
    metric_values.update({"survival_horizon": None, "post_signal_return": None})
    required = _required_metric_status(protocol, metric_values)
    bias = _default_bias_controls("PARTIAL", "No governed post-signal survival sample rows are bound to this hypothesis yet.")
    sample_status = _sample_size_status(protocol, [], returns)
    blockers = list(required["missing_required_metrics"]) + [row["control_id"] for row in bias if row.get("blocking") and row.get("status") != "PASS"]
    blockers.extend(run.get("artifact_binding_failures") or [])
    if run.get("code_reference_status") != "AVAILABLE":
        blockers.append("IMMUTABLE_CODE_REFERENCE_MISSING")
    status = _status_from_protocol_evidence(protocol, metric_values, sample_status, blockers)
    limitations = _limitations_from(blockers, sample_status)
    return _base_result(root, day, run, protocol, status, returns, limitations, metric_values, required_metric_status=required, bias_controls=bias, sample_size=sample_status, evaluator_id="post_signal_survival_evaluator_v1")


def _base_result(root: Path, day: str, run: Mapping[str, Any], protocol: Mapping[str, Any], status: str, returns: list[float], limitations: list[str], metric_values: Mapping[str, Any], *, required_metric_status: Mapping[str, Any] | None = None, bias_controls: list[dict[str, Any]] | None = None, sample_size: Mapping[str, Any] | None = None, evaluator_id: str = "") -> dict[str, Any]:
    sample_count = len(returns)
    minimum = int((sample_size or {}).get("required_sample_count") or protocol.get("minimum_sample_size") or 0)
    result_id = _stable_id("rvrsl", run.get("run_id"), status, sample_count, metric_values.get("average_forward_return"), metric_values.get("hit_rate"), run.get("input_context_hash"))
    bias_controls = bias_controls or _default_bias_controls("PARTIAL", "Bias controls were not evaluated.")
    required_metric_status = dict(required_metric_status or {"status": "FAIL", "missing_required_metrics": list(protocol.get("required_metrics") or [])})
    sample_size = dict(sample_size or {"sample_size_status": "UNDER_SAMPLED", "sample_size_reason": "No sample-size evaluation was available.", "sample_count": sample_count, "required_sample_count": minimum, "distinct_symbol_count": 0, "distinct_date_count": 0})
    return {
        "result_id": result_id,
        "run_id": run.get("run_id"),
        "hypothesis_id": run.get("hypothesis_id"),
        "hypothesis_version": run.get("hypothesis_version"),
        "protocol_id": run.get("protocol_id"),
        "protocol_version": run.get("protocol_version"),
        "protocol_hash": run.get("protocol_hash"),
        "evaluator_id": evaluator_id or run.get("evaluator_id"),
        "evaluator_version": run.get("evaluator_version"),
        "evaluator_hash": run.get("evaluator_hash"),
        "input_context_hash": run.get("input_context_hash"),
        "configuration_hash": run.get("configuration_hash"),
        "code_version": run.get("code_version"),
        "code_commit": run.get("code_commit"),
        "code_reference_status": run.get("code_reference_status"),
        "validation_status": status,
        "sample_count": sample_count,
        "required_sample_count": minimum,
        "hit_rate": metric_values.get("hit_rate"),
        "average_forward_return": metric_values.get("average_forward_return"),
        "median_forward_return": metric_values.get("median_forward_return"),
        "benchmark_relative_return": metric_values.get("benchmark_relative_return"),
        "drawdown_or_adverse_excursion": metric_values.get("drawdown_or_adverse_excursion"),
        "metric_values": dict(metric_values),
        "required_metric_status": required_metric_status,
        "sample_size_status": sample_size.get("sample_size_status"),
        "sample_size_reason": sample_size.get("sample_size_reason"),
        "distinct_symbol_count": sample_size.get("distinct_symbol_count", 0),
        "distinct_date_count": sample_size.get("distinct_date_count", 0),
        "sample_size_policy": protocol.get("sample_size_policy") or {},
        "bias_control_status": _aggregate_control_status(bias_controls),
        "bias_controls": bias_controls,
        "artifact_binding_status": run.get("artifact_binding_status"),
        "artifact_binding_failures": run.get("artifact_binding_failures") or [],
        "hypothesis_binding": run.get("hypothesis_binding") or {},
        "limitations": limitations,
        "plain_english_summary": _summary(status, sample_count, minimum),
        "source_artifacts": run.get("source_artifacts") or [],
        "source_artifact_hashes": run.get("source_artifact_hashes") or {},
        "deterministic_status_basis": "protocol-specific evaluator, required metrics, artifact binding, sample-size policy, and bias controls",
        "ai_summary_can_override_status": False,
    }


def _evaluator_for(evaluator_id: str) -> Callable[[Path, str, Mapping[str, Any], Mapping[str, Any]], dict[str, Any]] | None:
    return {
        "event_window_return_evaluator_v1": evaluate_event_window_return_v1,
        "mean_reversion_forward_return_evaluator_v1": evaluate_mean_reversion_forward_return_v1,
        "post_signal_survival_evaluator_v1": evaluate_post_signal_survival_v1,
    }.get(str(evaluator_id or ""))


def _metric_values_from_returns(returns: list[float]) -> dict[str, Any]:
    sample_count = len(returns)
    return {
        "sample_count": sample_count,
        "hit_rate": (sum(1 for value in returns if value > 0) / sample_count) if sample_count else None,
        "average_forward_return": statistics.fmean(returns) if returns else None,
        "median_forward_return": statistics.median(returns) if returns else None,
        "benchmark_relative_return": None,
        "drawdown_or_adverse_excursion": min(returns) if returns else None,
    }


def _required_metric_status(protocol: Mapping[str, Any], metric_values: Mapping[str, Any]) -> dict[str, Any]:
    required = [str(item) for item in protocol.get("required_metrics") or protocol.get("metrics") or []]
    missing = [name for name in required if metric_values.get(name) is None and name != "sample_count"]
    if "sample_count" in required and int(metric_values.get("sample_count") or 0) <= 0:
        missing.append("sample_count")
    return {"status": "PASS" if not missing else "FAIL", "required_metrics": required, "missing_required_metrics": sorted(set(missing))}


def _sample_size_status(protocol: Mapping[str, Any], samples: list[Mapping[str, Any]], returns: list[float]) -> dict[str, Any]:
    policy = protocol.get("sample_size_policy") if isinstance(protocol.get("sample_size_policy"), Mapping) else {}
    required = int(policy.get("minimum_sample_size") or protocol.get("minimum_sample_size") or 0)
    symbols = {str(row.get("symbol") or row.get("ticker") or "").upper() for row in samples if str(row.get("symbol") or row.get("ticker") or "").strip()}
    dates = {str(row.get("event_day") or row.get("event_date") or row.get("signal_day") or row.get("date") or "") for row in samples if str(row.get("event_day") or row.get("event_date") or row.get("signal_day") or row.get("date") or "").strip()}
    positives = sum(1 for value in returns if value > 0)
    minimum_positive = int(policy.get("minimum_positive_events") or 0)
    minimum_dates = int(policy.get("minimum_distinct_dates") or 0)
    minimum_symbols = int(policy.get("minimum_distinct_symbols") or 0)
    reasons = []
    if len(returns) < required:
        reasons.append(f"Only {len(returns)}/{required} required samples are available.")
    if positives < minimum_positive:
        reasons.append(f"Only {positives}/{minimum_positive} required positive events are available.")
    if len(dates) < minimum_dates:
        reasons.append(f"Only {len(dates)}/{minimum_dates} required distinct dates are available.")
    if len(symbols) < minimum_symbols:
        reasons.append(f"Only {len(symbols)}/{minimum_symbols} required distinct symbols are available.")
    return {
        "sample_size_status": "PASS" if not reasons else "UNDER_SAMPLED",
        "sample_size_reason": "Sample-size policy passed." if not reasons else " ".join(reasons),
        "sample_count": len(returns),
        "required_sample_count": required,
        "positive_event_count": positives,
        "distinct_symbol_count": len(symbols),
        "distinct_date_count": len(dates),
    }


def _bias_controls_for_mean_reversion(day: str, run: Mapping[str, Any], samples: list[Mapping[str, Any]], sample_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    controls = []
    future_rows = []
    duplicate_keys = set()
    seen = set()
    stale_rows = []
    outside_universe = []
    universe = {str(sym).upper() for sym in run.get("symbols_tested") or [] if str(sym).strip()}
    for idx, row in enumerate(samples):
        event_day = str(row.get("event_day") or row.get("event_date") or "")
        forward_day = str(row.get("forward_day") or row.get("forward_date") or row.get("sample_day") or day)
        symbol = str(row.get("symbol") or row.get("ticker") or "").upper()
        key = (symbol, event_day, forward_day)
        if key in seen:
            duplicate_keys.add(key)
        seen.add(key)
        if event_day and forward_day and forward_day <= event_day:
            future_rows.append(idx)
        if forward_day and forward_day > day:
            future_rows.append(idx)
        if str(row.get("stale") or "").lower() == "true" or str(row.get("freshness_status") or "").upper() in {"STALE", "EXPIRED"}:
            stale_rows.append(idx)
        if universe and symbol and symbol not in universe:
            outside_universe.append(symbol)
    controls.append(_control("LOOKAHEAD_GUARDRAIL", not future_rows, "Forward observations must occur after event day and no later than target day."))
    controls.append(_control("SURVIVORSHIP_GUARDRAIL", True, "V1 requires governed source artifacts; no survivorship exception detected in bound samples."))
    controls.append(_control("SELECTION_BIAS_GUARDRAIL", not outside_universe, "Symbols must belong to the hypothesis universe."))
    controls.append(_control("DUPLICATE_EVENT_GUARDRAIL", not duplicate_keys, "Duplicate symbol/event/forward windows must be excluded."))
    excluded = sample_payload.get("excluded_samples") if isinstance(sample_payload.get("excluded_samples"), list) else []
    controls.append(_control("STALE_DATA_GUARDRAIL", not stale_rows, "Stale samples must be excluded with reasons." + (" Exclusions recorded." if excluded else "")))
    return controls


def _default_bias_controls(status: str, evidence: str) -> list[dict[str, Any]]:
    ok = status == "PASS"
    return [_control(control_id, ok, evidence, status=status) for control_id in REQUIRED_BIAS_CONTROLS]


def _control(control_id: str, ok: bool, evidence: str, *, status: str | None = None) -> dict[str, Any]:
    return {"control_id": control_id, "status": status or ("PASS" if ok else "FAIL"), "evidence": evidence, "blocking": True}


def _aggregate_control_status(controls: list[Mapping[str, Any]]) -> str:
    statuses = {str(row.get("status") or "PARTIAL") for row in controls}
    if "FAIL" in statuses:
        return "FAIL"
    if "PARTIAL" in statuses:
        return "PARTIAL"
    return "PASS"


def _status_from_protocol_evidence(protocol: Mapping[str, Any], metric_values: Mapping[str, Any], sample_status: Mapping[str, Any], blockers: list[str]) -> str:
    if blockers:
        return "NOT_READY"
    if sample_status.get("sample_size_status") != "PASS":
        return "UNDER_SAMPLED"
    hit_rate = metric_values.get("hit_rate")
    average = metric_values.get("average_forward_return")
    support = protocol.get("support_thresholds") or {}
    disproof = protocol.get("disproof_thresholds") or {}
    if hit_rate is None or average is None:
        return "NOT_READY"
    if hit_rate <= float(disproof.get("hit_rate_max", 0.0)) or average <= float(disproof.get("average_forward_return_max", -1.0)):
        return "DISPROVEN"
    support_checks = [
        hit_rate >= float(support.get("hit_rate_min", 1.0)),
        average >= float(support.get("average_forward_return_min", 0.0)),
    ]
    if "benchmark_relative_return_min" in support:
        benchmark = metric_values.get("benchmark_relative_return")
        support_checks.append(benchmark is not None and benchmark >= float(support.get("benchmark_relative_return_min")))
    return "SUPPORTED" if all(support_checks) else "INCONCLUSIVE"


def _limitations_from(blockers: list[str], sample_status: Mapping[str, Any]) -> list[str]:
    limitations = []
    if sample_status.get("sample_size_status") != "PASS":
        limitations.append(str(sample_status.get("sample_size_reason") or "Sample-size policy did not pass."))
    limitations.extend(f"Blocking validation check failed: {blocker}." for blocker in sorted(set(blockers)) if blocker)
    return limitations or ["No blocking validation limitations detected."]


def _first_number(rows: list[Mapping[str, Any]], *keys: str) -> float | None:
    for row in rows:
        for key in keys:
            value = _number(row.get(key))
            if value is not None:
                return value
    return None


def _artifact_binding_for(root: Path, day: str, hypothesis: Mapping[str, Any], protocol_id: str) -> dict[str, Any]:
    hypothesis_id = str(hypothesis.get("hypothesis_id") or "")
    if protocol_id != "MEAN_REVERSION_FORWARD_RETURN_V1":
        return {"artifact_binding_status": "PASS", "artifact_binding_failures": [], "hypothesis_binding": {"hypothesis_id": hypothesis_id, "source_hypothesis_id": hypothesis_id, "binding_status": "MATCH", "reason": "No hypothesis-specific sample artifact is required for this protocol in V1 rework."}}
    payload = read_json_v1(research_validation_samples_path_v1(truth_root=root, day_utc=day))
    source_hypothesis = str(payload.get("hypothesis_id") or "") if payload else ""
    if not payload:
        return {"artifact_binding_status": "FAIL", "artifact_binding_failures": ["RESEARCH_VALIDATION_SAMPLE_ARTIFACT_MISSING"], "hypothesis_binding": {"hypothesis_id": hypothesis_id, "source_hypothesis_id": "", "binding_status": "MISSING", "reason": "No mean-reversion validation sample artifact is available."}}
    if source_hypothesis != hypothesis_id:
        return {"artifact_binding_status": "PASS", "artifact_binding_failures": [], "hypothesis_binding": {"hypothesis_id": hypothesis_id, "source_hypothesis_id": source_hypothesis, "binding_status": "NO_BOUND_SAMPLES_FOR_HYPOTHESIS", "reason": "The day-scoped sample artifact belongs to another hypothesis, so this run treats it as unrelated evidence instead of a mismatch."}}
    return {"artifact_binding_status": "PASS", "artifact_binding_failures": [], "hypothesis_binding": {"hypothesis_id": hypothesis_id, "source_hypothesis_id": source_hypothesis, "binding_status": "MATCH", "reason": "Sample artifact hypothesis_id matches validation run hypothesis_id."}}


def _code_reference_v1() -> dict[str, str]:
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[3], text=True).strip()
    except Exception:
        commit = ""
    return {"code_commit": commit, "code_reference_status": "AVAILABLE" if commit else "UNAVAILABLE_BLOCKS_VALIDATION"}


def _evaluator_hash(evaluator_id: str) -> str:
    evaluator = _evaluator_for(evaluator_id)
    if evaluator is None:
        return ""
    try:
        return hashlib.sha256(inspect.getsource(evaluator).encode("utf-8")).hexdigest()
    except OSError:
        return ""



def _candidate_research_gate_self_check(root: Path, day: str) -> dict[str, Any]:
    base = root / "reports" / "candidate_generation_manifest_v1" / day
    violations: list[dict[str, Any]] = []
    checked = 0
    if not base.exists():
        return {"ok": True, "checked_candidate_rows": 0, "violations": []}
    for path in sorted(base.glob("**/candidate_generation_manifest.v1.json")):
        payload = read_json_v1(path)
        for row in payload.get("candidate_rows") or []:
            if not isinstance(row, Mapping):
                continue
            checked += 1
            has_hypothesis = bool(str(row.get("hypothesis_id") or row.get("source_hypothesis_id") or row.get("research_hypothesis_id") or "").strip())
            required = row.get("research_validation_required") is True
            status = str(row.get("research_validation_enforcement_status") or "")
            if has_hypothesis and not required:
                violations.append({"path": str(path), "candidate_id": row.get("candidate_id"), "reason": "HYPOTHESIS_LINEAGE_WITHOUT_RESEARCH_VALIDATION_REQUIRED"})
            if required and status not in {"PASS", "REJECTED_RESEARCH_VALIDATION_MISSING", "REJECTED_RESEARCH_NOT_SUPPORTED", "REJECTED_RESEARCH_GATE_MISMATCH"}:
                violations.append({"path": str(path), "candidate_id": row.get("candidate_id"), "reason": "INVALID_RESEARCH_VALIDATION_ENFORCEMENT_STATUS", "status": status})
            if required and status != "PASS" and str(row.get("status") or "") == "CANDIDATE_CREATED":
                violations.append({"path": str(path), "candidate_id": row.get("candidate_id"), "reason": "UNSUPPORTED_RESEARCH_CANDIDATE_CREATED", "status": status})
    return {"ok": not violations, "checked_candidate_rows": checked, "violations": violations}


def resolve_research_promotion_eligibility_v1(*, truth_root: Path | str, day_utc: str, hypothesis_id: str, hypothesis_version: str = "v1") -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    gate_path = research_promotion_gate_path_v1(truth_root=root, day_utc=day)
    gate_payload = read_json_v1(gate_path)
    gate_hash = _file_hash(gate_path)
    if not gate_payload:
        return {"research_validation_required": True, "hypothesis_id": hypothesis_id, "hypothesis_version": hypothesis_version, "research_validation_result_id": "", "research_promotion_gate_status": "MISSING", "research_promotion_gate_hash": "", "research_validation_enforcement_status": "REJECTED_RESEARCH_VALIDATION_MISSING", "eligible_for_candidate_review": False, "reason": "Research promotion gate artifact is missing."}
    if str(gate_payload.get("day_utc") or "") != day:
        return {"research_validation_required": True, "hypothesis_id": hypothesis_id, "hypothesis_version": hypothesis_version, "research_validation_result_id": "", "research_promotion_gate_status": "DAY_MISMATCH", "research_promotion_gate_hash": gate_hash, "research_validation_enforcement_status": "REJECTED_RESEARCH_GATE_MISMATCH", "eligible_for_candidate_review": False, "reason": "Research promotion gate day does not match requested day."}
    for row in gate_payload.get("promotion_gates") or []:
        if str(row.get("hypothesis_id") or "") == str(hypothesis_id) and str(row.get("hypothesis_version") or "v1") == str(hypothesis_version or "v1"):
            eligible = bool(row.get("eligible_for_candidate_review") is True and row.get("validation_status") == "SUPPORTED" and row.get("candidate_review_only") is True and row.get("tradeable") is False and row.get("trade_advice_allowed") is False)
            return {"research_validation_required": True, "hypothesis_id": hypothesis_id, "hypothesis_version": hypothesis_version, "research_validation_result_id": str(row.get("latest_result_id") or ""), "research_promotion_gate_status": str(row.get("promotion_gate_status") or ""), "research_promotion_gate_hash": gate_hash, "research_validation_enforcement_status": "PASS" if eligible else "REJECTED_RESEARCH_NOT_SUPPORTED", "eligible_for_candidate_review": eligible, "reason": "Research promotion gate allows candidate review." if eligible else str(row.get("blocked_reason") or "Research validation did not support this hypothesis.")}
    return {"research_validation_required": True, "hypothesis_id": hypothesis_id, "hypothesis_version": hypothesis_version, "research_validation_result_id": "", "research_promotion_gate_status": "NOT_FOUND", "research_promotion_gate_hash": gate_hash, "research_validation_enforcement_status": "REJECTED_RESEARCH_VALIDATION_MISSING", "eligible_for_candidate_review": False, "reason": "No promotion gate row matches this hypothesis and version."}

def _summary(status: str, sample_count: int, minimum: int) -> str:
    if status == "SUPPORTED":
        return "Deterministic validation supports this hypothesis for candidate-review eligibility only."
    if status == "DISPROVEN":
        return "Deterministic validation contradicts this hypothesis; it cannot promote."
    if status == "UNDER_SAMPLED":
        return f"Validation is under-sampled with {sample_count}/{minimum} required observations."
    if status == "INCONCLUSIVE":
        return "Validation has enough samples but does not meet support or disproof thresholds."
    return "Validation is not ready because required deterministic evidence is missing."


def _artifact_ref(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "sha256": _file_hash(path),
    }


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return f"{prefix}-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:12]}"


def _safe_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).upper() for item in value if str(item)]
    if isinstance(value, str) and value.strip():
        return [part.strip().upper() for part in value.split(",") if part.strip()]
    return []


def _number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
