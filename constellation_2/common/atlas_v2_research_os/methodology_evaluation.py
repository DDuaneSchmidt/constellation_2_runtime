from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

from .artifact_models import ArtifactType
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .autonomous_research_execution import _edge_input_from_hypothesis
from .edge_qualification import compute_edge_score
from .historical_replay_results import edge_input_with_historical_replay
from .paper_trade_candidate_reports import build_paper_trade_candidate_report
from .research_backlog import ResearchBacklog

REPORT_DIRNAME = "methodology_evaluation"


def run_methodology_evaluation(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, limit: int = 8) -> dict[str, Any]:
    root_path = Path(root)
    day_value = day or _today()
    report = build_methodology_evaluation(root_path, day=day_value, limit=limit)
    paths = write_methodology_evaluation(report, root_path, day=day_value)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    paths["json"].write_text(payload, encoding="utf-8")
    paths["latest_json"].write_text(payload, encoding="utf-8")
    return report


def build_methodology_evaluation(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, limit: int = 8) -> dict[str, Any]:
    root_path = Path(root)
    day_value = day or _today()
    candidates = _latest_candidate_reviews(root_path, limit=limit)
    metrics = _metrics(candidates)
    rejection_groups = dict(Counter(reason for row in candidates for reason in row.get("rejection_reasons", [])))
    classification, rationale = _classify(candidates, metrics, rejection_groups)
    near_threshold = [row for row in candidates if 0.6 <= float(row.get("edge_score", 0.0)) < 0.7]
    replay_attached = [row for row in candidates if row.get("historical_replay_contribution", {}).get("candidate_historical_sample_size", 0)]
    return {
        "schema_id": "atlas_v2_research_os_methodology_evaluation_v1",
        "schema_version": "v1",
        "day": day_value,
        "created_at": _now(),
        "core_question": "Does Atlas generate hypotheses and candidate ideas that improve after historical replay, edge qualification, and rejection feedback?",
        "candidate_count_analyzed": len(candidates),
        "latest_candidates": candidates,
        "metrics": metrics,
        "rejection_groups": rejection_groups,
        "historical_replay_impact": {
            "candidate_level_replay_attachment_count": len(replay_attached),
            "candidate_level_replay_attachment_rate": metrics["candidate_level_historical_replay_attachment_rate"],
            "candidate_level_positive_replay_count": sum(1 for row in replay_attached if float(row.get("historical_replay_contribution", {}).get("candidate_historical_replay_score") or 0.0) >= 0.5),
            "assessment": _replay_assessment(metrics),
        },
        "near_threshold_candidates": near_threshold,
        "methodology_classification": classification,
        "classification_rationale": rationale,
        "human_summary": {
            "does_methodology_show_promise": classification in {"PROMISING", "WEAK_BUT_LEARNING"},
            "what_atlas_generated": f"{len(candidates)} latest reconstructed PaperTradeCandidate evaluations from completed edge reviews.",
            "why_candidates_were_rejected": rejection_groups,
            "are_rejections_useful_or_noise": "Useful if replay-backed; replay attachment now separates evidence weakness from mechanism weakness." if replay_attached else "Mostly evidence-routing noise until replay attaches to candidates.",
            "near_threshold_candidate_count": len(near_threshold),
            "one_change_to_improve_quality": "Run historical replay before edge qualification and require replay-backed edge inputs whenever replay is available.",
            "should_atlas_run_another_cycle": classification != "BROKEN_FUNNEL",
        },
        "allowed_recommendations": _recommendations(classification, near_threshold, rejection_groups),
        "forbidden_recommendations_omitted": [
            "live trading",
            "capital allocation",
            "broker execution",
            "position sizing",
            "portfolio construction",
            "candidate production promotion",
        ],
        "guardrails": {
            "live_trading_added": False,
            "broker_execution_added": False,
            "capital_authority_added": False,
            "candidate_promotion_added": False,
            "automatic_paper_trade_placement_added": False,
        },
        "source_report_summaries": _source_report_summaries(root_path),
    }


def write_methodology_evaluation(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    out_root = Path(root) / REPORT_DIRNAME
    day_value = day or str(report.get("day") or _today())
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "methodology_evaluation.json"
    summary_path = out_dir / "methodology_evaluation_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_methodology_evaluation_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_methodology_evaluation_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics", {})
    lines = [
        "# Atlas Research OS Methodology Evaluation",
        "",
        f"Classification: {report.get('methodology_classification')}",
        f"Candidates analyzed: {report.get('candidate_count_analyzed', 0)}",
        f"Average edge score: {metrics.get('average_edge_score', 0.0)}",
        f"Median edge score: {metrics.get('median_edge_score', 0.0)}",
        f"Candidate replay attachment rate: {metrics.get('candidate_level_historical_replay_attachment_rate', 0.0)}",
        f"Generated-only rejection rate: {metrics.get('generated_only_rejection_rate', 0.0)}",
        f"Low-score rejection rate: {metrics.get('low_score_rejection_rate', 0.0)}",
        f"Eligibility rate: {metrics.get('candidate_eligibility_rate', 0.0)}",
        f"Historical replay impact: {report.get('historical_replay_impact', {}).get('assessment', '')}",
        "",
        "## Latest Candidates",
    ]
    for row in report.get("latest_candidates", []):
        lines.extend([
            f"- {row.get('candidate_id')}: mechanism={row.get('mechanism')}, edge_score={row.get('edge_score')}, evidence={row.get('evidence_level')}, replay_score={row.get('historical_replay_contribution', {}).get('candidate_historical_replay_score')}, eligible={row.get('paper_trade_eligible')}",
            f"  rejection_reasons={json.dumps(row.get('rejection_reasons', []), sort_keys=True)}",
        ])
    lines.extend([
        "",
        "Allowed recommendations: " + json.dumps(report.get("allowed_recommendations", []), sort_keys=True),
        "Authority: research-only; no live trading, broker execution, capital authority, candidate promotion, position sizing, portfolio construction, sleeve deployment, or automatic paper placement.",
        "",
    ])
    return "\n".join(lines)


def _latest_candidate_reviews(root: Path, *, limit: int) -> list[dict[str, Any]]:
    store = ArtifactStore(root)
    rows = [row for row in ResearchBacklog(root).list_backlog_items(item_type="EDGE_QUALIFICATION_REVIEW") if row.get("state") == "COMPLETED"]
    rows.sort(key=lambda row: str(row.get("source_execution_id") or row.get("created_at") or row.get("backlog_item_id")), reverse=True)
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        source_ids = [str(value) for value in row.get("source_artifact_ids", [])]
        if not source_ids:
            continue
        source_artifacts = []
        for artifact_id in source_ids:
            try:
                source_artifacts.append(store.get_artifact(artifact_id))
            except Exception:
                pass
        hypothesis = next((item for item in source_artifacts if item.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value), None)
        if not hypothesis:
            continue
        replay = next((item for item in source_artifacts if item.get("artifact_type") == "HistoricalReplayResult"), None)
        edge_input = _edge_input_from_hypothesis(hypothesis, row)
        if replay:
            replay_result = dict((replay.get("metadata") or {}).get("historical_replay_result") or {})
            if replay_result:
                edge_input = edge_input_with_historical_replay(edge_input, replay_result).to_dict()
        report = build_paper_trade_candidate_report(edge_input)
        candidate = report["candidate"]
        key = candidate["candidate_id"]
        if key in seen:
            continue
        seen.add(key)
        score = compute_edge_score(edge_input)
        hist = candidate.get("historical_replay_summary") or {}
        contribution = {
            "candidate_historical_replay_score": hist.get("score"),
            "candidate_historical_sample_size": hist.get("sample_size", 0),
            "candidate_historical_expectancy": hist.get("expectancy"),
            "candidate_historical_regime_consistency": hist.get("regime_consistency"),
            "candidate_historical_failure_rate": hist.get("failure_rate"),
            "contribution": "historical replay contributed to score" if hist else "none: candidate edge input had no historical replay fields attached",
        }
        candidates.append({
            "candidate_id": candidate["candidate_id"],
            "backlog_item_id": row.get("backlog_item_id"),
            "mechanism": (candidate.get("mechanism_tags") or ["UNKNOWN"])[0],
            "mechanism_tags": candidate.get("mechanism_tags", []),
            "regime_context": candidate.get("regime_context", {}),
            "edge_score": candidate.get("edge_score", 0.0),
            "confidence": candidate.get("confidence", 0.0),
            "evidence_level": candidate.get("evidence_level", "UNKNOWN"),
            "qualification_reasons": candidate.get("qualification_reasons", []),
            "rejection_reasons": candidate.get("disqualification_reasons", []),
            "historical_replay_contribution": contribution,
            "needed_to_reach_eligibility": _needed_to_reach_eligibility(candidate, contribution),
            "source_artifact_ids": candidate.get("source_artifact_ids", []),
            "source_hypothesis_ids": candidate.get("source_hypothesis_ids", []),
            "human_review_required": candidate.get("human_review_required", True),
            "paper_trade_eligible": candidate.get("paper_trade_eligible", False),
            "score_components": score.get("components", {}),
            "score_explanation": score.get("explanation", []),
        })
        if len(candidates) >= limit:
            break
    return candidates


def _metrics(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [float(row.get("edge_score", 0.0)) for row in candidates]
    count = len(candidates)
    if not count:
        return {
            "hypothesis_to_candidate_rate": 0.0,
            "candidate_eligibility_rate": 0.0,
            "average_edge_score": 0.0,
            "median_edge_score": 0.0,
            "historical_replay_positive_rate": 0.0,
            "candidate_level_historical_replay_attachment_rate": 0.0,
            "candidate_level_historical_replay_positive_rate": 0.0,
            "generated_only_rejection_rate": 0.0,
            "low_score_rejection_rate": 0.0,
            "duplicate_rejection_rate": 0.0,
            "governance_rejection_rate": 0.0,
        }
    replay_attached = [row for row in candidates if row.get("historical_replay_contribution", {}).get("candidate_historical_sample_size", 0)]
    positive_replay = [row for row in replay_attached if float(row.get("historical_replay_contribution", {}).get("candidate_historical_replay_score") or 0.0) >= 0.5]
    return {
        "hypothesis_to_candidate_rate": 1.0 if count else 0.0,
        "candidate_eligibility_rate": round(sum(1 for row in candidates if row.get("paper_trade_eligible")) / count, 6),
        "average_edge_score": round(sum(scores) / count, 6),
        "median_edge_score": round(float(median(scores)), 6),
        "historical_replay_positive_rate": round(len(positive_replay) / len(replay_attached), 6) if replay_attached else 0.0,
        "candidate_level_historical_replay_attachment_rate": round(len(replay_attached) / count, 6),
        "candidate_level_historical_replay_positive_rate": round(len(positive_replay) / count, 6),
        "generated_only_rejection_rate": _reason_rate(candidates, "generated-only evidence"),
        "low_score_rejection_rate": _reason_rate(candidates, "edge_score below"),
        "duplicate_rejection_rate": _reason_rate(candidates, "duplicate"),
        "governance_rejection_rate": _reason_rate(candidates, "governance failed"),
    }


def _reason_rate(candidates: list[dict[str, Any]], needle: str) -> float:
    if not candidates:
        return 0.0
    return round(sum(1 for row in candidates if any(needle.lower() in str(reason).lower() for reason in row.get("rejection_reasons", []))) / len(candidates), 6)


def _needed_to_reach_eligibility(candidate: dict[str, Any], contribution: dict[str, Any]) -> list[str]:
    needs: list[str] = []
    if float(candidate.get("edge_score", 0.0)) < 0.7:
        needs.append(f"Raise edge score from {candidate.get('edge_score')} to >= 0.70 through stronger replay, evidence maturity, research effectiveness, or hypothesis survival.")
    if any("generated-only" in str(reason).lower() for reason in candidate.get("disqualification_reasons", [])):
        needs.append("Attach non-generated evidence, preferably historical replay or paper-forward observations.")
    if not contribution.get("candidate_historical_sample_size"):
        needs.append("Run historical replay for the generated hypothesis and carry replay metrics into edge qualification.")
    if not needs:
        needs.append("Human review for paper-testing consideration; no live or capital authority.")
    return needs


def _classify(candidates: list[dict[str, Any]], metrics: dict[str, Any], rejection_groups: dict[str, int]) -> tuple[str, list[str]]:
    if not candidates:
        return "BROKEN_FUNNEL", ["No candidates could be reconstructed from completed edge qualification reviews."]
    if metrics["governance_rejection_rate"] >= 0.5:
        return "BROKEN_FUNNEL", ["Governance failures dominate candidate review."]
    if metrics["candidate_level_historical_replay_attachment_rate"] == 0.0:
        return "NO_SIGNAL_YET", ["Candidates are inspectable, but candidate-level historical replay is still missing."]
    near_threshold = any(float(row.get("edge_score", 0.0)) >= 0.6 for row in candidates)
    if metrics["candidate_eligibility_rate"] > 0 or near_threshold:
        return "PROMISING", ["Replay-backed candidates are coherent and at least one is near or above threshold."]
    return "WEAK_BUT_LEARNING", ["Candidates remain mostly rejected, but replay-backed rejection reasons are actionable."]


def _replay_assessment(metrics: dict[str, Any]) -> str:
    if metrics["candidate_level_historical_replay_attachment_rate"] <= 0.0:
        return "Candidate-level replay remains absent; methodology signal is not fully testable."
    if metrics["historical_replay_positive_rate"] > 0.0:
        return "Candidate-level replay is attached and contributes measurable support or rejection evidence."
    return "Candidate-level replay is attached, but replay evidence is not positive yet."


def _recommendations(classification: str, near_threshold: list[dict[str, Any]], rejection_groups: dict[str, int]) -> list[str]:
    recommendations = []
    if near_threshold:
        recommendations.append("Review near-threshold replay-backed candidates.")
    if rejection_groups.get("edge_score below 0.7") or any("edge_score below" in key for key in rejection_groups):
        recommendations.append("Collect more paper-forward observations and replay evidence for low-score candidates.")
    if classification != "BROKEN_FUNNEL":
        recommendations.append("Run another research cycle.")
    else:
        recommendations.append("Fix worker compatibility or replay attachment before another cycle.")
    return recommendations


def _source_report_summaries(root: Path) -> dict[str, Any]:
    names = ["overnight_review", "throughput_review", "paper_trade_candidates", "historical_replay", "research_effectiveness", "learning_validation", "failures", "claim_hypothesis_funnel"]
    return {name: _read_json(root / name / "latest.json", {}) for name in names if (root / name / "latest.json").exists()}


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
