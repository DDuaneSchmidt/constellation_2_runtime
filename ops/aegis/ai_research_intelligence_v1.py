from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_quality_control_v1 import (
    build_hypothesis_decision_policy_v1,
    build_research_allocation_recommendation_v1,
    build_research_follow_through_control_v1,
    build_research_quality_engine_v1,
)

AI_RESEARCH_INTELLIGENCE_POLICY_VERSION = "AEGIS_AI_RESEARCH_INTELLIGENCE_POLICY_V1"
CRITIC_FAMILY = "aegis_ai_research_critic_v1"
ROOT_CAUSE_FAMILY = "aegis_ai_root_cause_analysis_v1"
REPAIR_FAMILY = "aegis_ai_hypothesis_repair_advisor_v1"
EVIDENCE_FAMILY = "aegis_ai_evidence_synthesis_v1"
DUPLICATE_FAMILY = "aegis_ai_duplicate_detection_v1"
SUMMARY_FAMILY = "aegis_ai_research_intelligence_summary_v1"
CRITIC_FILENAME = "ai_research_critic.v1.json"
ROOT_CAUSE_FILENAME = "ai_root_cause_analysis.v1.json"
REPAIR_FILENAME = "ai_hypothesis_repair_advisor.v1.json"
EVIDENCE_FILENAME = "ai_evidence_synthesis.v1.json"
DUPLICATE_FILENAME = "ai_duplicate_detection.v1.json"
SUMMARY_FILENAME = "ai_research_intelligence_summary.v1.json"
SAFETY = {
    "research_only": True,
    "ai_is_advisory_only": True,
    "deterministic_state_authority": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_allocation_mutation": True,
    "no_automatic_retirement": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "allocation_mutation_performed": False,
    "automatic_retirement_performed": False,
    "paper_observation_created": False,
}


def ai_research_critic_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, CRITIC_FAMILY, day_utc, CRITIC_FILENAME)


def ai_root_cause_analysis_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, ROOT_CAUSE_FAMILY, day_utc, ROOT_CAUSE_FILENAME)


def ai_hypothesis_repair_advisor_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPAIR_FAMILY, day_utc, REPAIR_FILENAME)


def ai_evidence_synthesis_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, EVIDENCE_FAMILY, day_utc, EVIDENCE_FILENAME)


def ai_duplicate_detection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, DUPLICATE_FAMILY, day_utc, DUPLICATE_FILENAME)


def ai_research_intelligence_summary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, SUMMARY_FAMILY, day_utc, SUMMARY_FILENAME)


def build_ai_research_intelligence_bundle_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, dict[str, Any]]:
    ctx = _context(Path(truth_root), str(day_utc))
    critic = _artifact(CRITIC_FAMILY, str(day_utc), ctx, "critic_rows", [_critic_row(row, ctx) for row in ctx["hypotheses"]])
    root_cause = _artifact(ROOT_CAUSE_FAMILY, str(day_utc), ctx, "root_cause_rows", [_root_cause_row(row, ctx) for row in ctx["hypotheses"]])
    repair = _artifact(REPAIR_FAMILY, str(day_utc), ctx, "repair_rows", [_repair_row(row, ctx) for row in ctx["hypotheses"] if _needs_repair(row, ctx)])
    evidence = _artifact(EVIDENCE_FAMILY, str(day_utc), ctx, "evidence_rows", [_evidence_row(row, ctx) for row in ctx["hypotheses"]])
    duplicate = _artifact(DUPLICATE_FAMILY, str(day_utc), ctx, "duplicate_rows", _duplicate_rows(ctx))
    summary = _summary_artifact(str(day_utc), ctx, critic, root_cause, repair, evidence, duplicate)
    return {
        "critic": critic,
        "root_cause": root_cause,
        "repair": repair,
        "evidence": evidence,
        "duplicate": duplicate,
        "summary": summary,
    }


def write_ai_research_intelligence_bundle_v1(*, truth_root: Path | str, day_utc: str, bundle: Mapping[str, Any] | None = None) -> dict[str, Path]:
    payloads = dict(bundle or build_ai_research_intelligence_bundle_v1(truth_root=truth_root, day_utc=day_utc))
    return {
        "critic": write_json_v1(ai_research_critic_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payloads["critic"])),
        "root_cause": write_json_v1(ai_root_cause_analysis_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payloads["root_cause"])),
        "repair": write_json_v1(ai_hypothesis_repair_advisor_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payloads["repair"])),
        "evidence": write_json_v1(ai_evidence_synthesis_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payloads["evidence"])),
        "duplicate": write_json_v1(ai_duplicate_detection_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payloads["duplicate"])),
        "summary": write_json_v1(ai_research_intelligence_summary_path_v1(truth_root=truth_root, day_utc=day_utc), dict(payloads["summary"])),
    }


def _context(root: Path, day: str) -> dict[str, Any]:
    paths = {
        "workflow": report_path_v1(root, "aegis_hypothesis_workflow_state_v1", day, "hypothesis_workflow_state.v1.json"),
        "action_queue": report_path_v1(root, "aegis_operator_action_queue_v1", day, "operator_action_queue.v1.json"),
        "throughput": report_path_v1(root, "aegis_generated_hypothesis_throughput_v1", day, "generated_hypothesis_throughput.v1.json"),
        "quality": report_path_v1(root, "aegis_research_quality_engine_v1", day, "research_quality_engine.v1.json"),
        "decisions": report_path_v1(root, "aegis_hypothesis_decision_policy_v1", day, "hypothesis_decision_policy.v1.json"),
        "allocation": report_path_v1(root, "aegis_research_allocation_recommendation_v1", day, "research_allocation_recommendation.v1.json"),
        "follow_through": report_path_v1(root, "aegis_research_follow_through_control_v1", day, "research_follow_through_control.v1.json"),
        "outcome_registry": report_path_v1(root, "aegis_outcome_registry_v1", day, "outcome_registry.v1.json"),
        "validation_samples": report_path_v1(root, "aegis_validation_samples_v1", day, "validation_samples.v1.json"),
        "statistical_sufficiency": report_path_v1(root, "aegis_statistical_sufficiency_v1", day, "statistical_sufficiency.v1.json"),
        "portfolio": report_path_v1(root, "aegis_research_portfolio_v1", day, "research_portfolio.v1.json"),
    }
    payloads = {key: read_json_v1(path) for key, path in paths.items()}
    if not payloads["quality"]:
        payloads["quality"] = build_research_quality_engine_v1(truth_root=root, day_utc=day)
    if not payloads["decisions"]:
        payloads["decisions"] = build_hypothesis_decision_policy_v1(truth_root=root, day_utc=day, quality=payloads["quality"])
    if not payloads["allocation"]:
        payloads["allocation"] = build_research_allocation_recommendation_v1(truth_root=root, day_utc=day, decisions=payloads["decisions"])
    if not payloads["follow_through"]:
        payloads["follow_through"] = build_research_follow_through_control_v1(truth_root=root, day_utc=day, quality=payloads["quality"], decisions=payloads["decisions"], allocation=payloads["allocation"])
    rows = _combined_rows(payloads)
    return {
        "root": root,
        "day": day,
        "paths": paths,
        "payloads": payloads,
        "hashes": {key: file_hash_v1(path) for key, path in paths.items()},
        "hypotheses": rows,
        "by_id": {row["hypothesis_id"]: row for row in rows},
    }


def _combined_rows(payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for key, collection_key in [("portfolio", "hypotheses"), ("workflow", "hypotheses"), ("quality", "hypotheses")]:
        for row in payloads.get(key, {}).get(collection_key, []) or []:
            if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")):
                hid = text_v1(row.get("hypothesis_id"))
                current = rows.get(hid, {})
                rows[hid] = {**current, **dict(row), "hypothesis_id": hid, "display_name": text_v1(row.get("display_name") or row.get("name") or current.get("display_name") or current.get("name") or hid)}
    for key, collection_key, dest in [("decisions", "decisions", "decision"), ("allocation", "recommendations", "allocation"), ("follow_through", "follow_ups", "follow_up")]:
        for row in payloads.get(key, {}).get(collection_key, []) or []:
            if isinstance(row, Mapping) and text_v1(row.get("hypothesis_id")):
                hid = text_v1(row.get("hypothesis_id"))
                current = rows.get(hid, {"hypothesis_id": hid, "display_name": text_v1(row.get("name") or row.get("hypothesis_name") or hid)})
                current[dest] = dict(row)
                if not current.get("display_name"):
                    current["display_name"] = text_v1(row.get("name") or row.get("hypothesis_name") or hid)
                rows[hid] = current
    return [rows[key] for key in sorted(rows)]


def _artifact(family: str, day: str, ctx: Mapping[str, Any], row_key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_id": family,
        "schema_version": "v1",
        "artifact_id": family,
        "day_utc": day,
        "ai_policy_version": AI_RESEARCH_INTELLIGENCE_POLICY_VERSION,
        "input_artifact_hashes": dict(ctx["hashes"]),
        "source_artifact_paths": {key: str(path) for key, path in ctx["paths"].items()},
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"family": family, "day_utc": day, "inputs": ctx["hashes"]}),
        row_key: rows,
        "summary": {"row_count": len(rows), "confidence_counts": dict(Counter(row.get("confidence_level", "MEDIUM") for row in rows))},
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def _critic_row(row: Mapping[str, Any], ctx: Mapping[str, Any]) -> dict[str, Any]:
    counts = row.get("sample_counts") if isinstance(row.get("sample_counts"), Mapping) else {}
    gates = set(row.get("active_hard_gate_codes") or [])
    samples = int(counts.get("included_sample_count") or 0)
    candidates = int(counts.get("candidate_count") or 0)
    reasons = []
    if not text_v1(row.get("formal_claim")):
        reasons.append("ECONOMIC_RATIONALE_NEEDS_CLARITY")
    if samples < 30:
        reasons.append("EVIDENCE_TOO_SPARSE")
    if "NO_DATA_SOURCE" in gates:
        reasons.append("DATA_SOURCE_MISSING")
    if candidates == 0:
        reasons.append("NO_CANDIDATE_FLOW")
    if "DUPLICATE_HYPOTHESIS" in gates:
        reasons.append("POSSIBLE_DUPLICATE")
    return _row_base(row, ctx) | {
        "economic_plausibility_critique": "Plausibility is present but remains unproven until deterministic validation evidence is sufficient." if text_v1(row.get("formal_claim")) else "Economic rationale is underspecified in deterministic artifacts.",
        "testability_critique": "Testable through existing candidate and paper-observation flow." if candidates else "Testability is limited until candidate flow exists.",
        "sample_frequency_warning": "Evidence is too sparse for strong claims." if samples < 30 else "Sample count meets the current minimum threshold.",
        "data_quality_warning": "Missing or blocked data source prevents reliable research interpretation." if "NO_DATA_SOURCE" in gates else "No AI data-quality concern beyond deterministic quality artifact.",
        "regime_dependency_warning": "Regime coverage is likely underpowered until more samples span market conditions." if samples < 30 else "Regime dependency should still be monitored against validation slices.",
        "false_discovery_warning": "False-discovery risk is elevated because validation evidence is sparse." if samples < 30 else "False-discovery risk is lower but still advisory-only.",
        "confidence_level": "HIGH" if "NO_DATA_SOURCE" in gates else "MEDIUM",
        "reason_codes": reasons or ["NO_AI_CRITIC_CONCERN"],
    }


def _root_cause_row(row: Mapping[str, Any], ctx: Mapping[str, Any]) -> dict[str, Any]:
    follow = row.get("follow_up") if isinstance(row.get("follow_up"), Mapping) else {}
    decision = row.get("decision") if isinstance(row.get("decision"), Mapping) else {}
    allocation = row.get("allocation") if isinstance(row.get("allocation"), Mapping) else {}
    follow_type = text_v1(follow.get("follow_up_type"))
    recommendation = text_v1(decision.get("recommendation"))
    allocation_action = text_v1(allocation.get("recommended_allocation_action"))
    likely = []
    if recommendation == "NEEDS_DATA" or follow_type == "DATA_SOURCE_RESOLUTION":
        likely.append("missing or unresolved data source")
    if follow_type == "PAPER_TRACKING_FLOW_WATCH":
        likely.append("PAPER_TRACKING_READY with no candidate flow")
    if follow_type == "CANDIDATE_FLOW_WATCH":
        likely.append("candidate flow has not converted into validation samples")
    if recommendation == "REDESIGN" or allocation_action == "PAUSE" or follow_type == "REPAIR_INVESTIGATION":
        likely.append("implementation or paper-path repair is required")
    if follow_type == "SAMPLE_ACCUMULATION_WATCH":
        likely.append("validation sample count is below sufficiency threshold")
    return _row_base(row, ctx) | {
        "likely_causes": likely or ["no AI root-cause concern from deterministic artifacts"],
        "supporting_artifacts": [str(ctx["paths"][key]) for key in ["quality", "decisions", "allocation", "follow_through", "workflow"]],
        "confidence_level": "HIGH" if likely else "LOW",
        "suggested_investigation": _investigation_text(follow_type, recommendation),
        "david_action_likely_required": bool(follow.get("requires_david_action") or recommendation in {"NEEDS_DATA", "REDESIGN"} or allocation_action == "PAUSE"),
        "reason_codes": list(dict.fromkeys(list(follow.get("reason_codes") or []) + likely)) or ["NO_ROOT_CAUSE_FLAG"],
    }


def _repair_row(row: Mapping[str, Any], ctx: Mapping[str, Any]) -> dict[str, Any]:
    gates = set(row.get("active_hard_gate_codes") or [])
    decision = row.get("decision") if isinstance(row.get("decision"), Mapping) else {}
    options = []
    if "NO_DATA_SOURCE" in gates or text_v1(decision.get("recommendation")) == "NEEDS_DATA":
        options.append("add missing data")
    if "NO_CANDIDATE_FLOW" in gates:
        options.extend(["expand universe", "adjust signal threshold", "improve candidate construction"])
    if "NO_PAPER_PATH" in gates or "NO_OBSERVATION_FLOW" in gates:
        options.extend(["improve entry logic", "improve exit logic"])
    options.extend(["narrow/merge hypothesis", "retire candidate if likely invalid"])
    return _row_base(row, ctx) | {
        "repair_options": list(dict.fromkeys(options)),
        "repair_summary": "; ".join(list(dict.fromkeys(options[:4]))),
        "advisory_only": True,
        "confidence_level": "MEDIUM",
        "reason_codes": list(decision.get("reason_codes") or []) or ["REPAIR_ADVISORY_FROM_REDIRECT_OR_PAUSE"],
    }


def _evidence_row(row: Mapping[str, Any], ctx: Mapping[str, Any]) -> dict[str, Any]:
    counts = row.get("sample_counts") if isinstance(row.get("sample_counts"), Mapping) else {}
    samples = int(counts.get("included_sample_count") or 0)
    outcomes = int(counts.get("outcome_count") or 0)
    sparse = samples < 30
    return _row_base(row, ctx) | {
        "what_appears_to_be_working": "Candidate or paper flow exists, but evidence remains advisory until validation sufficiency passes." if int(counts.get("candidate_count") or 0) else "No working condition can be inferred without candidate flow.",
        "what_appears_to_be_failing": "Sample accumulation is below sufficiency threshold." if sparse else "No deterministic failure is apparent from sample count alone.",
        "common_winning_conditions": "Not enough deterministic outcome evidence to describe stable winning conditions." if sparse else "Winning conditions require outcome-slice review before promotion.",
        "common_losing_conditions": "Not enough deterministic outcome evidence to describe stable losing conditions." if sparse else "Losing conditions require outcome-slice review before promotion.",
        "sample_limitations": f"included_samples={samples}; outcomes={outcomes}; minimum=30",
        "evidence_too_sparse": sparse,
        "confidence_level": "HIGH" if sparse else "MEDIUM",
        "reason_codes": ["EVIDENCE_TOO_SPARSE"] if sparse else ["EVIDENCE_SAMPLE_MINIMUM_MET_ADVISORY_ONLY"],
    }


def _duplicate_rows(ctx: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = ctx["hypotheses"]
    out = []
    for i, left in enumerate(rows):
        for right in rows[i + 1:]:
            lt, rt = _tokens(left), _tokens(right)
            union = lt | rt
            score = 0.0 if not union else len(lt & rt) / len(union)
            exact_norm = _norm(left.get("display_name")) and _norm(left.get("display_name")) == _norm(right.get("display_name"))
            if exact_norm:
                score = 1.0
            if score >= 0.45:
                out.append({
                    "pair_id": stable_hash_v1([left.get("hypothesis_id"), right.get("hypothesis_id")])[:16],
                    "left_hypothesis_id": left.get("hypothesis_id"),
                    "right_hypothesis_id": right.get("hypothesis_id"),
                    "left_display_name": left.get("display_name"),
                    "right_display_name": right.get("display_name"),
                    "duplicate_score": round(score, 4),
                    "overlap_reason": "normalized names match" if exact_norm else "substantial token overlap in hypothesis names or claims",
                    "shared_instruments": sorted((lt & rt) & _instrument_tokens()),
                    "shared_factor_exposure": sorted((lt & rt) & {"momentum", "trend", "spread", "volatility", "event", "macro", "oil", "equity"}),
                    "shared_signal_behavior": sorted((lt & rt) & {"momentum", "trend", "reversion", "convergence", "dislocation", "shock"}),
                    "merge_reject_suggestion": "review merge or reject as duplicate" if score >= 0.7 else "watch for overlap; no automatic rejection",
                    "confidence_level": "HIGH" if score >= 0.7 else "MEDIUM",
                    "source_artifact_paths": {key: str(path) for key, path in ctx["paths"].items()},
                    "source_artifact_hashes": dict(ctx["hashes"]),
                    **SAFETY,
                })
    return out


def _summary_artifact(day: str, ctx: Mapping[str, Any], critic: Mapping[str, Any], root_cause: Mapping[str, Any], repair: Mapping[str, Any], evidence: Mapping[str, Any], duplicate: Mapping[str, Any]) -> dict[str, Any]:
    critic_by_h = {r["hypothesis_id"]: r for r in critic["critic_rows"]}
    root_by_h = {r["hypothesis_id"]: r for r in root_cause["root_cause_rows"]}
    repair_by_h = {r["hypothesis_id"]: r for r in repair["repair_rows"]}
    evidence_by_h = {r["hypothesis_id"]: r for r in evidence["evidence_rows"]}
    duplicate_by_h: dict[str, list[Mapping[str, Any]]] = {}
    for row in duplicate["duplicate_rows"]:
        duplicate_by_h.setdefault(text_v1(row.get("left_hypothesis_id")), []).append(row)
        duplicate_by_h.setdefault(text_v1(row.get("right_hypothesis_id")), []).append(row)
    rows = []
    for row in ctx["hypotheses"]:
        hid = row["hypothesis_id"]
        rec_type = _ai_recommendation_type(row, critic_by_h.get(hid, {}), root_by_h.get(hid, {}), repair_by_h.get(hid, {}), evidence_by_h.get(hid, {}), duplicate_by_h.get(hid, []))
        rows.append({
            "hypothesis_id": hid,
            "display_name": row.get("display_name") or hid,
            "critic_summary": "; ".join((critic_by_h.get(hid, {}).get("reason_codes") or ["NO_AI_CRITIC_CONCERN"])[:3]),
            "root_cause_summary": "; ".join(root_by_h.get(hid, {}).get("likely_causes") or ["No AI root-cause concern"]),
            "repair_summary": repair_by_h.get(hid, {}).get("repair_summary") or "No repair suggestion from AI advisory layer.",
            "evidence_summary": evidence_by_h.get(hid, {}).get("sample_limitations") or "No evidence row.",
            "duplicate_summary": _duplicate_summary(duplicate_by_h.get(hid, [])),
            "ai_confidence": _max_confidence([critic_by_h.get(hid, {}), root_by_h.get(hid, {}), repair_by_h.get(hid, {}), evidence_by_h.get(hid, {})]),
            "ai_recommendation_type": rec_type,
            "safety_statement": "AI research analysis is advisory only; deterministic Aegis artifacts remain authoritative.",
            **SAFETY,
        })
    payload = {
        "schema_id": SUMMARY_FAMILY,
        "schema_version": "v1",
        "artifact_id": SUMMARY_FAMILY,
        "day_utc": day,
        "ai_policy_version": AI_RESEARCH_INTELLIGENCE_POLICY_VERSION,
        "input_artifact_hashes": dict(ctx["hashes"]),
        "source_artifact_paths": {key: str(path) for key, path in ctx["paths"].items()},
        "computed_at_utc": _now(),
        "deterministic_rerun_id": stable_hash_v1({"family": SUMMARY_FAMILY, "day_utc": day, "inputs": ctx["hashes"]}),
        "hypotheses": rows,
        "summary": {"hypothesis_count": len(rows), "recommendation_counts": dict(Counter(row["ai_recommendation_type"] for row in rows))},
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1(_without_generated_time(payload))
    return payload


def _row_base(row: Mapping[str, Any], ctx: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "hypothesis_id": row.get("hypothesis_id"),
        "display_name": row.get("display_name") or row.get("name") or row.get("hypothesis_id"),
        "source_artifact_paths": {key: str(path) for key, path in ctx["paths"].items()},
        "source_artifact_hashes": dict(ctx["hashes"]),
        **SAFETY,
    }


def _needs_repair(row: Mapping[str, Any], ctx: Mapping[str, Any]) -> bool:
    decision = row.get("decision") if isinstance(row.get("decision"), Mapping) else {}
    allocation = row.get("allocation") if isinstance(row.get("allocation"), Mapping) else {}
    return text_v1(decision.get("recommendation")) == "REDESIGN" or text_v1(allocation.get("recommended_allocation_action")) == "PAUSE"


def _investigation_text(follow_type: str, recommendation: str) -> str:
    if recommendation == "NEEDS_DATA" or follow_type == "DATA_SOURCE_RESOLUTION":
        return "resolve the missing data source or mark it unavailable"
    if follow_type == "PAPER_TRACKING_FLOW_WATCH":
        return "inspect why paper-tracking readiness has not produced candidates"
    if follow_type == "REPAIR_INVESTIGATION":
        return "review construction, paper path, entry logic, exit logic, and retirement criteria"
    if follow_type == "CANDIDATE_FLOW_WATCH":
        return "inspect candidate-to-sample conversion and blocker evidence"
    return "monitor deterministic evidence and re-score after new samples"


def _ai_recommendation_type(row: Mapping[str, Any], critic: Mapping[str, Any], root: Mapping[str, Any], repair: Mapping[str, Any], evidence: Mapping[str, Any], duplicates: list[Mapping[str, Any]]) -> str:
    decision = row.get("decision") if isinstance(row.get("decision"), Mapping) else {}
    if duplicates:
        return "POSSIBLE_DUPLICATE"
    if text_v1(decision.get("recommendation")) == "NEEDS_DATA":
        return "DATA_NEEDED"
    if repair:
        return "REPAIR_SUGGESTED"
    if evidence.get("evidence_too_sparse"):
        return "EVIDENCE_TOO_SPARSE"
    if root.get("likely_causes") and root.get("likely_causes") != ["no AI root-cause concern from deterministic artifacts"]:
        return "INVESTIGATE"
    if (critic.get("reason_codes") or []) != ["NO_AI_CRITIC_CONCERN"]:
        return "WATCH"
    return "NO_AI_CONCERN"


def _duplicate_summary(rows: list[Mapping[str, Any]]) -> str:
    if not rows:
        return "No likely duplicate detected."
    best = sorted(rows, key=lambda row: float(row.get("duplicate_score") or 0), reverse=True)[0]
    other = best.get("right_display_name") if best.get("left_hypothesis_id") else best.get("left_display_name")
    return f"Possible overlap score {best.get('duplicate_score')} with {other}: {best.get('overlap_reason')}"


def _max_confidence(rows: list[Mapping[str, Any]]) -> str:
    order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    best = "LOW"
    for row in rows:
        confidence = text_v1(row.get("confidence_level")) or "LOW"
        if order.get(confidence, 0) > order.get(best, 0):
            best = confidence
    return best


def _tokens(row: Mapping[str, Any]) -> set[str]:
    text = " ".join(text_v1(row.get(key)) for key in ["display_name", "name", "formal_claim", "market_universe", "signal_definition"])
    return {token for token in _norm(text).split() if len(token) > 2 and token not in {"the", "and", "for", "with", "when", "from", "that", "can", "edge"}}


def _instrument_tokens() -> set[str]:
    return {"equity", "equities", "oil", "energy", "etf", "etfs", "macro", "volatility", "spread", "cross", "asset"}


def _norm(value: Any) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in text_v1(value)).split())


def _without_generated_time(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _without_generated_time(v) for k, v in value.items() if k not in {"computed_at_utc", "generated_at", "generated_at_utc", "content_hash"}}
    if isinstance(value, list):
        return [_without_generated_time(v) for v in value]
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
