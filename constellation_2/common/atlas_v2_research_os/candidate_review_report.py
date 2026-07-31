from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .candidate_review_governance import validate_candidate_review_allowed
from .candidate_review_models import (
    CANDIDATE_REVIEW_ALLOWED_SCOPE,
    CANDIDATE_REVIEW_FORBIDDEN_ACTIONS,
    CANDIDATE_REVIEW_LIMITATION,
    CandidateReviewItem,
    CandidateReviewReport,
)

CANDIDATE_REVIEW_REPORT_ROOT = Path("reports/atlas_v2_research_os/candidate_review")
DEFAULT_METHODOLOGY_TRIAL_PATH = Path("reports/atlas_v2_research_os/methodology_trial/latest.json")


def build_candidate_review_report(
    trial_report: dict[str, Any] | None = None,
    *,
    source_report_path: str | Path = DEFAULT_METHODOLOGY_TRIAL_PATH,
    day: str | None = None,
    review_limit: int = 12,
) -> dict[str, Any]:
    source_path = Path(source_report_path)
    source = dict(trial_report or json.loads(source_path.read_text(encoding="utf-8")))
    trial_candidates = list(source.get("trial_candidates", []))
    eligible = list(source.get("eligible_candidates") or _eligible_candidates(trial_candidates))
    selected = _rank_candidates(eligible)[:review_limit]
    top_candidates = [_review_item(row, index + 1).to_dict() for index, row in enumerate(selected)]
    near_threshold = _monitoring_rows(
        source.get("near_threshold_candidates") or [row for row in trial_candidates if 0.60 <= float(row.get("edge_score", 0.0)) < 0.70],
        limit=10,
    )
    rejected = _monitoring_rows(
        [row for row in trial_candidates if row.get("replay_status") == "REPLAY_NEGATIVE"],
        limit=10,
    )
    report = CandidateReviewReport(
        schema_id="atlas_v2_research_os_candidate_review_report_v1",
        schema_version="v1",
        day=day or str(source.get("day") or _today()),
        created_at=_now(),
        source_report_path=source_path.as_posix(),
        candidates_reviewed=len(top_candidates),
        top_candidates=top_candidates,
        near_threshold_candidates=near_threshold,
        rejected_candidates_worth_monitoring=rejected,
        mechanism_clusters=_mechanism_clusters(trial_candidates),
        common_risks=_common_risks(trial_candidates),
        recommended_next_human_reviews=_next_human_reviews(top_candidates, near_threshold),
        authority_boundary=_authority_boundary(),
        guardrails={
            "allowed": [CANDIDATE_REVIEW_ALLOWED_SCOPE],
            "forbidden": CANDIDATE_REVIEW_FORBIDDEN_ACTIONS,
            "human_review_required": True,
            "paper_forward_observation_only": True,
        },
        sections={
            "Top candidates": [row["candidate_id"] for row in top_candidates],
            "Near-threshold candidates": [row["candidate_id"] for row in near_threshold],
            "Rejected candidates worth monitoring": [row["candidate_id"] for row in rejected],
            "Mechanism clusters": _mechanism_clusters(trial_candidates),
            "Common risks": _common_risks(trial_candidates),
            "Recommended next human reviews": _next_human_reviews(top_candidates, near_threshold),
        },
        limitations=[CANDIDATE_REVIEW_LIMITATION],
    ).to_dict()
    validate_candidate_review_allowed(report)
    return report


def write_candidate_review_report(
    trial_report: dict[str, Any] | None = None,
    *,
    root: str | Path = CANDIDATE_REVIEW_REPORT_ROOT,
    source_report_path: str | Path = DEFAULT_METHODOLOGY_TRIAL_PATH,
    day: str | None = None,
) -> dict[str, Path]:
    report = build_candidate_review_report(trial_report, source_report_path=source_report_path, day=day)
    day_value = day or report["day"]
    out_root = Path(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "candidate_review_report.json"
    md_path = out_dir / "candidate_review_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_candidate_review_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_candidate_review_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas Candidate Review",
        "",
        f"Day: {report['day']}",
        f"Candidates reviewed: {report['candidates_reviewed']}",
        f"Authority: {report['authority_boundary']['allowed_scope']} only.",
        "",
        "## Top candidates",
    ]
    for row in report["top_candidates"]:
        lines.extend(
            [
                f"{row['rank']}. {row['candidate_id']} - {row['mechanism']} - edge {row['edge_score']} - replay {row['replay_score']}",
                f"   Why interesting: {row['why_interesting']}",
                f"   Evidence: {row['evidence_summary']}",
                f"   Invalidates if: {row['invalidating_condition']}",
            ]
        )
    lines.extend(
        [
            "",
            "## Near-threshold candidates",
            json.dumps(report["near_threshold_candidates"], sort_keys=True),
            "",
            "## Rejected candidates worth monitoring",
            json.dumps(report["rejected_candidates_worth_monitoring"], sort_keys=True),
            "",
            "## Mechanism clusters",
            json.dumps(report["mechanism_clusters"], sort_keys=True),
            "",
            "## Common risks",
            json.dumps(report["common_risks"], sort_keys=True),
            "",
            "## Recommended next human reviews",
            json.dumps(report["recommended_next_human_reviews"], sort_keys=True),
            "",
            f"Limitations: {json.dumps(report['limitations'], sort_keys=True)}",
            "Forbidden: no trade recommendation, capital allocation, position sizing, live trading, or broker execution.",
            "",
        ]
    )
    return "\n".join(lines)


def audit_candidate_review_reports(root: str | Path = CANDIDATE_REVIEW_REPORT_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root)
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                validate_candidate_review_allowed(payload)
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"candidate_review_audit_ok": not failures, "candidate_review_audit_failures": failures}


def _review_item(row: dict[str, Any], rank: int) -> CandidateReviewItem:
    mechanism = str(row.get("mechanism") or _first(row.get("mechanism_tags")) or "UNKNOWN")
    regime = str(row.get("regime") or row.get("historical_replay_summary", {}).get("regime_context", {}).get("label") or "UNKNOWN")
    metrics = row.get("historical_replay_summary", {}).get("metrics", {})
    edge_score = round(float(row.get("edge_score", 0.0)), 6)
    replay_score = round(float(row.get("replay_score", metrics.get("historical_replay_score", 0.0))), 6)
    sample_size = int(row.get("replay_sample_size", metrics.get("sample_size", 0)) or 0)
    win_rate = metrics.get("win_rate")
    failure_rate = metrics.get("failure_rate")
    expectancy = metrics.get("expectancy")
    return CandidateReviewItem(
        rank=rank,
        candidate_id=str(row.get("paper_trade_candidate_id") or row.get("candidate_id")),
        mechanism=mechanism,
        edge_score=edge_score,
        replay_score=replay_score,
        hypothesis=(
            f"Paper-forward observe whether {mechanism} in {regime} continues to show replay-supported "
            f"behavior without authority expansion."
        ),
        entry_condition=(
            f"Only log a paper-forward observation when the {mechanism} setup appears in the {regime} regime "
            "and the observation can preserve source evidence."
        ),
        exit_condition="End observation when the setup resolves, the regime changes, or required evidence cannot be preserved.",
        invalidating_condition=(
            "Invalidate watch status if replay support weakens, paper-forward observations fail to reproduce the pattern, "
            "lineage breaks, or the setup requires any forbidden trading/capital/broker action."
        ),
        evidence_summary=(
            f"Replay status {row.get('replay_status')} with replay_score={replay_score}, edge_score={edge_score}, "
            f"sample_size={sample_size}, win_rate={win_rate}, failure_rate={failure_rate}, expectancy={expectancy}."
        ),
        why_interesting=(
            "Cleared the methodology trial edge threshold with positive historical replay evidence and complete candidate lineage."
        ),
        why_risky=(
            "Evidence remains historical replay only; replay may be regime-sensitive, sample-limited, duplicated by mechanism, "
            "or fail during paper-forward observation."
        ),
        human_action_required="Review the evidence packet and decide whether to collect paper-forward observations only.",
        replay_status=str(row.get("replay_status") or "INSUFFICIENT_SAMPLE"),
        regime=regime,
        source_hypothesis_id=str(row.get("hypothesis_id") or row.get("historical_replay_summary", {}).get("hypothesis_id") or ""),
        source_replay_id=str(row.get("replay_id") or row.get("historical_replay_summary", {}).get("replay_id") or ""),
        source_artifact_ids=list(row.get("historical_replay_summary", {}).get("source_artifact_ids", [])),
        metadata={
            "strength_profile": row.get("strength_profile"),
            "evidence_level": row.get("evidence_level"),
            "human_review_required": row.get("human_review_required"),
            "paper_trade_eligible": row.get("paper_trade_eligible"),
        },
    )


def _monitoring_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    output = []
    for row in _rank_candidates(rows)[:limit]:
        output.append(
            {
                "candidate_id": row.get("paper_trade_candidate_id") or row.get("candidate_id"),
                "mechanism": row.get("mechanism") or _first(row.get("mechanism_tags")) or "UNKNOWN",
                "regime": row.get("regime") or "UNKNOWN",
                "edge_score": round(float(row.get("edge_score", 0.0)), 6),
                "replay_score": round(float(row.get("replay_score", 0.0)), 6),
                "replay_status": row.get("replay_status"),
                "why_monitor": _monitoring_reason(row),
                "human_action_required": "Review for paper-forward observation only if evidence quality improves.",
            }
        )
    return output


def _mechanism_clusters(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("mechanism") or _first(row.get("mechanism_tags")) or "UNKNOWN")].append(row)
    clusters = []
    for mechanism, items in grouped.items():
        edge_scores = [float(row.get("edge_score", 0.0)) for row in items]
        replay_scores = [float(row.get("replay_score", 0.0)) for row in items]
        clusters.append(
            {
                "mechanism": mechanism,
                "candidate_count": len(items),
                "positive_replay_count": sum(1 for row in items if row.get("replay_status") == "REPLAY_POSITIVE"),
                "negative_replay_count": sum(1 for row in items if row.get("replay_status") == "REPLAY_NEGATIVE"),
                "average_edge_score": round(sum(edge_scores) / len(edge_scores), 6) if edge_scores else 0.0,
                "average_replay_score": round(sum(replay_scores) / len(replay_scores), 6) if replay_scores else 0.0,
            }
        )
    clusters.sort(key=lambda row: (row["average_edge_score"], row["positive_replay_count"]), reverse=True)
    return clusters


def _common_risks(rows: list[dict[str, Any]]) -> list[str]:
    reasons = Counter(reason for row in rows for reason in row.get("rejection_reasons", []))
    risks = [
        "Historical replay is not paper-forward or live evidence.",
        "Mechanism clusters can duplicate the same structural idea across regimes.",
        "Unknown or weak regime labels reduce interpretability.",
        "Paper-forward observation may fail to reproduce replay metrics.",
        "Any authority expansion would invalidate the review artifact.",
    ]
    risks.extend(f"Observed rejection reason: {reason} ({count} candidates)." for reason, count in reasons.most_common(3))
    return risks


def _next_human_reviews(top_candidates: list[dict[str, Any]], near_threshold: list[dict[str, Any]]) -> list[str]:
    top_ids = [row["candidate_id"] for row in top_candidates[:5]]
    near_ids = [row["candidate_id"] for row in near_threshold[:5]]
    return [
        f"Read evidence packets for top candidates: {', '.join(top_ids)}.",
        "Check whether top candidates are mechanism duplicates before paper-forward observation.",
        f"Compare near-threshold candidates for evidence gaps: {', '.join(near_ids)}.",
        "Document invalidation criteria before any observation is logged.",
    ]


def _eligible_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("paper_trade_eligible") is True
        or (row.get("replay_status") == "REPLAY_POSITIVE" and float(row.get("edge_score", 0.0)) > 0.70)
    ]


def _rank_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (float(row.get("edge_score", 0.0)), float(row.get("replay_score", 0.0))), reverse=True)


def _monitoring_reason(row: dict[str, Any]) -> str:
    if row.get("replay_status") == "REPLAY_NEGATIVE":
        return "Rejected by replay, but useful as a failure pattern comparator."
    return "Below the edge threshold but close enough to inspect evidence gaps."


def _authority_boundary() -> dict[str, Any]:
    return {
        "allowed_scope": CANDIDATE_REVIEW_ALLOWED_SCOPE,
        "human_review_required": True,
        "paper_forward_observation_allowed": True,
        "trade_recommendation_authorized": False,
        "capital_allocation_authorized": False,
        "position_sizing_authorized": False,
        "live_trading_authorized": False,
        "broker_execution_authorized": False,
        "automatic_paper_trade_placement_authorized": False,
    }


def _first(value: Any) -> Any:
    if isinstance(value, list) and value:
        return value[0]
    return None


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
