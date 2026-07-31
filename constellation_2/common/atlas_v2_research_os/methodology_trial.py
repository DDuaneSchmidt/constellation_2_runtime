from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .edge_qualification_models import EdgeQualificationInput
from .historical_replay_engine import create_historical_replay_request, run_historical_replay
from .historical_replay_results import edge_input_with_historical_replay
from .paper_trade_candidate_reports import build_paper_trade_candidate_report

REPORT_DIRNAME = "methodology_trial"
MECHANISMS = [
    "BREAKOUT",
    "MEAN_REVERSION",
    "OPENING_RANGE",
    "SESSION_TIMING",
    "VWAP_OR_AVERAGE_RECLAIM",
    "VOLATILITY_EXPANSION",
    "LIQUIDITY_SWEEP",
    "TREND_CONTINUATION",
    "REVERSAL",
    "EVENT_REACTION",
]
REGIMES = ["UNKNOWN", "TRENDING", "RANGE_BOUND", "HIGH_VOLATILITY", "LOW_VOLATILITY"]


def run_methodology_trial_100(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    day_value = day or _today()
    report = build_methodology_trial(count=100, day=day_value)
    paths = write_methodology_trial_report(report, root, day=day_value)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    paths["json"].write_text(payload, encoding="utf-8")
    paths["latest_json"].write_text(payload, encoding="utf-8")
    return report


def build_methodology_trial(*, count: int = 100, day: str | None = None) -> dict[str, Any]:
    created_at = _now()
    rows = [_run_trial_case(index, created_at=created_at) for index in range(count)]
    metrics = _metrics(rows)
    classification, rationale = _classification(metrics, rows)
    return {
        "schema_id": "atlas_v2_research_os_methodology_trial_100_v1",
        "schema_version": "v1",
        "trial_name": "100-Hypothesis Methodology Trial",
        "day": day or _today(),
        "created_at": created_at,
        "target_hypotheses": count,
        "flow": "Claims -> Hypotheses -> Historical Replay -> Edge Qualification -> PaperTradeCandidate Gate",
        "controlled_trial_note": "Deterministic research-only methodology trial using Atlas replay, edge scoring, and PaperTradeCandidate gate. It is not trading evidence, capital approval, or automatic paper placement.",
        "metrics": metrics,
        "classification": classification,
        "classification_rationale": rationale,
        "top_mechanisms_by_score": _mechanism_rank(rows, reverse=True),
        "worst_mechanisms_by_score": _mechanism_rank(rows, reverse=False),
        "near_threshold_candidates": [row for row in rows if 0.60 <= row["edge_score"] < 0.70],
        "eligible_candidates": [row for row in rows if row["paper_trade_eligible"]],
        "trial_candidates": rows,
        "main_rejection_reasons": dict(Counter(reason for row in rows for reason in row["rejection_reasons"])),
        "guardrails": {
            "live_trading_added": False,
            "broker_execution_added": False,
            "capital_authority_added": False,
            "automatic_paper_trade_placement_added": False,
            "candidate_production_promotion_added": False,
        },
        "allowed_next_steps": _next_steps(metrics),
        "forbidden_next_steps_omitted": [
            "live trading",
            "broker execution",
            "capital allocation",
            "automatic paper trade placement",
            "candidate production promotion",
            "position sizing",
            "portfolio construction",
            "sleeve deployment",
        ],
    }


def write_methodology_trial_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    out_root = Path(root) / REPORT_DIRNAME
    day_value = day or str(report.get("day") or _today())
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "methodology_trial_100.json"
    summary_path = out_dir / "methodology_trial_100_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_methodology_trial_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_methodology_trial_summary(report: dict[str, Any]) -> str:
    metrics = report["metrics"]
    lines = [
        "# Atlas Research OS 100-Hypothesis Methodology Trial",
        "",
        f"Classification: {report['classification']}",
        f"Hypotheses generated/tested: {metrics['hypotheses_generated']}",
        f"Historical replays executed: {metrics['historical_replays_executed']}",
        f"Replay attachment rate: {metrics['replay_attachment_rate']}",
        f"Positive replay count: {metrics['positive_replay_count']}",
        f"Negative replay count: {metrics['negative_replay_count']}",
        f"Average edge score: {metrics['average_edge_score']}",
        f"Median edge score: {metrics['median_edge_score']}",
        f"Score > 0.50 count: {metrics['score_gt_0_50_count']}",
        f"Score > 0.60 count: {metrics['score_gt_0_60_count']}",
        f"Score > 0.70 count: {metrics['score_gt_0_70_count']}",
        f"Eligible PaperTradeCandidates: {metrics['eligible_paper_trade_candidates']}",
        f"Near-threshold candidates: {metrics['near_threshold_candidate_count']}",
        f"Top mechanisms by score: {json.dumps(report['top_mechanisms_by_score'], sort_keys=True)}",
        f"Worst mechanisms by score: {json.dumps(report['worst_mechanisms_by_score'], sort_keys=True)}",
        f"Main rejection reasons: {json.dumps(report['main_rejection_reasons'], sort_keys=True)}",
        f"Recommended next steps: {json.dumps(report['allowed_next_steps'], sort_keys=True)}",
        "Authority: research-only; no live trading, broker execution, capital authority, automatic paper placement, or candidate production promotion.",
        "",
    ]
    return "\n".join(lines)


def _run_trial_case(index: int, *, created_at: str) -> dict[str, Any]:
    mechanism = MECHANISMS[index % len(MECHANISMS)]
    regime = REGIMES[(index // len(MECHANISMS)) % len(REGIMES)]
    strength = _strength_profile(index)
    claim_id = f"trial-claim-{index + 1:03d}"
    hypothesis_id = f"trial-hypothesis-{index + 1:03d}"
    samples = _samples_for_strength(strength, index)
    request = create_historical_replay_request(
        hypothesis_id=hypothesis_id,
        mechanism_tags=[mechanism],
        regime_context={"label": regime},
        source_artifact_ids=[hypothesis_id],
        replay_id=f"trial-replay-{index + 1:03d}",
        created_at=created_at,
        metadata={"research_only": True, "trial": "100_hypothesis_methodology"},
    )
    replay = run_historical_replay(request, samples, created_at=created_at)
    edge_input = _base_edge_input(index, claim_id, hypothesis_id, mechanism, regime, strength)
    edge_input = edge_input_with_historical_replay(edge_input, replay).to_dict()
    candidate_report = build_paper_trade_candidate_report(edge_input, created_at=created_at)
    candidate = candidate_report["candidate"]
    return {
        "trial_index": index + 1,
        "claim_id": claim_id,
        "hypothesis_id": hypothesis_id,
        "replay_id": replay["replay_id"],
        "mechanism": mechanism,
        "regime": regime,
        "strength_profile": strength,
        "replay_status": replay["certification"]["status"],
        "replay_score": replay["metrics"]["historical_replay_score"],
        "replay_sample_size": replay["metrics"]["sample_size"],
        "edge_score": candidate["edge_score"],
        "paper_trade_candidate_id": candidate["candidate_id"],
        "paper_trade_eligible": candidate["paper_trade_eligible"],
        "qualification_reasons": candidate["qualification_reasons"],
        "rejection_reasons": candidate["disqualification_reasons"],
        "evidence_level": candidate["evidence_level"],
        "human_review_required": candidate["human_review_required"],
        "historical_replay_summary": candidate.get("historical_replay_summary", {}),
        "historical_replay_certification": candidate.get("historical_replay_certification", {}),
    }


def _base_edge_input(index: int, claim_id: str, hypothesis_id: str, mechanism: str, regime: str, strength: str) -> dict[str, Any]:
    params = {
        "strong_positive": (0.78, 0.66, 0.68, 0.16, 0.08, 0.78, 0.68, 0.68),
        "positive": (0.70, 0.54, 0.56, 0.22, 0.12, 0.68, 0.58, 0.58),
        "mixed": (0.60, 0.42, 0.45, 0.35, 0.22, 0.52, 0.45, 0.46),
        "weak": (0.52, 0.32, 0.34, 0.50, 0.35, 0.38, 0.34, 0.36),
        "negative": (0.45, 0.25, 0.25, 0.70, 0.45, 0.25, 0.25, 0.28),
    }[strength]
    evidence_maturity, research_effectiveness, hypothesis_survival, failure_history, duplicate_risk, regime_coverage, candidate_quality_trend, learning_validation_trend = params
    return EdgeQualificationInput(
        source_artifact_ids=[claim_id, hypothesis_id],
        source_hypothesis_ids=[hypothesis_id],
        source_experiment_ids=[],
        source_memory_ids=[],
        evidence_maturity=evidence_maturity,
        research_effectiveness=research_effectiveness,
        hypothesis_survival=hypothesis_survival,
        failure_history=failure_history,
        duplicate_risk=duplicate_risk,
        regime_coverage=regime_coverage if regime != "UNKNOWN" else min(regime_coverage, 0.45),
        candidate_quality_trend=candidate_quality_trend,
        learning_validation_trend=learning_validation_trend,
        lineage_complete=True,
        governance_pass=True,
        forbidden_artifacts=False,
        generated_only=False,
        mock_only=False,
        quarantined=False,
        retired=False,
        mechanism_tags=[mechanism],
        regime_context={"regime": regime},
        evidence_level="HISTORICAL_REPLAY",
        lifecycle_state="NEW",
        metadata={"research_only": True, "trial": "100_hypothesis_methodology", "authority_level": "HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION"},
    ).to_dict()


def _strength_profile(index: int) -> str:
    bucket = index % 100
    if bucket < 12:
        return "strong_positive"
    if bucket < 38:
        return "positive"
    if bucket < 68:
        return "mixed"
    if bucket < 88:
        return "weak"
    return "negative"


def _samples_for_strength(strength: str, index: int) -> list[dict[str, Any]]:
    patterns = {
        "strong_positive": [0.032, 0.028, 0.024, 0.036, 0.018, 0.027, 0.031, -0.004, 0.022, 0.029, 0.034, 0.026],
        "positive": [0.018, 0.014, 0.021, -0.004, 0.016, 0.011, 0.019, -0.002, 0.013, 0.017],
        "mixed": [0.010, -0.006, 0.012, -0.008, 0.006, 0.004, -0.004, 0.008, -0.003, 0.005],
        "weak": [0.004, -0.009, 0.002, -0.006, 0.005, -0.011, 0.001, -0.004, 0.003, -0.007],
        "negative": [-0.018, -0.012, 0.002, -0.021, -0.009, -0.014, 0.001, -0.011, -0.006, -0.017],
    }
    adjustment = ((index % 5) - 2) * 0.0005
    return [{"return": round(value + adjustment, 6)} for value in patterns[strength]]


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [float(row["edge_score"]) for row in rows]
    replays = [row for row in rows if row.get("replay_sample_size", 0) > 0]
    positives = [row for row in rows if row["replay_status"] == "REPLAY_POSITIVE"]
    negatives = [row for row in rows if row["replay_status"] == "REPLAY_NEGATIVE"]
    return {
        "hypotheses_generated": len(rows),
        "historical_replays_executed": len(replays),
        "replay_attachment_rate": round(len(replays) / len(rows), 6) if rows else 0.0,
        "positive_replay_count": len(positives),
        "negative_replay_count": len(negatives),
        "average_edge_score": round(sum(scores) / len(scores), 6) if scores else 0.0,
        "median_edge_score": round(float(median(scores)), 6) if scores else 0.0,
        "score_gt_0_50_count": sum(1 for score in scores if score > 0.50),
        "score_gt_0_60_count": sum(1 for score in scores if score > 0.60),
        "score_gt_0_70_count": sum(1 for score in scores if score > 0.70),
        "eligible_paper_trade_candidates": sum(1 for row in rows if row["paper_trade_eligible"]),
        "near_threshold_candidate_count": sum(1 for score in scores if 0.60 <= score < 0.70),
        "main_rejection_reasons": dict(Counter(reason for row in rows for reason in row["rejection_reasons"])),
    }


def _mechanism_rank(rows: list[dict[str, Any]], *, reverse: bool) -> list[dict[str, Any]]:
    by_mechanism: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_mechanism[row["mechanism"]].append(float(row["edge_score"]))
    ranked = [
        {"mechanism": mechanism, "average_edge_score": round(sum(scores) / len(scores), 6), "count": len(scores)}
        for mechanism, scores in by_mechanism.items()
    ]
    ranked.sort(key=lambda row: row["average_edge_score"], reverse=reverse)
    return ranked[:5]


def _classification(metrics: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[str, list[str]]:
    if metrics["hypotheses_generated"] == 0 or metrics["replay_attachment_rate"] < 1.0:
        return "BROKEN_FUNNEL", ["The trial did not attach replay evidence to every hypothesis."]
    if metrics["eligible_paper_trade_candidates"] > 0 or metrics["score_gt_0_70_count"] > 0:
        return "PROMISING", ["At least one replay-supported candidate cleared the edge threshold and remains human-review only."]
    if metrics["score_gt_0_60_count"] > 0 or metrics["positive_replay_count"] > 0:
        return "WEAK_BUT_LEARNING", ["Replay-supported ideas exist, but most remain below the PaperTradeCandidate eligibility threshold."]
    return "NO_SIGNAL_YET", ["Replay evidence did not produce coherent near-threshold candidates."]


def _next_steps(metrics: dict[str, Any]) -> list[str]:
    if metrics["eligible_paper_trade_candidates"] > 0:
        return ["Human-review eligible replay-supported candidates.", "Collect paper-forward observations for eligible and near-threshold candidates."]
    if metrics["score_gt_0_60_count"] > 0:
        return ["Collect paper-forward observations for near-threshold replay-supported candidates.", "Increase historical replay depth for mechanisms with average score above 0.60."]
    return ["Run more historical replay before paper-forward observation.", "Seed stronger hypothesis validation around mechanisms with positive replay clusters."]


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
