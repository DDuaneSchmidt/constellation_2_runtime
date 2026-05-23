from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.outcomes.attribution_metrics import build_attribution_metrics, build_decision_quality_summary
from research_lab.storage.hashing import content_hash, short_hash


ATTRIBUTION_SCHEMA_VERSION = "attribution_report.v1"


def validate_attribution_report(report: dict[str, Any]) -> None:
    validate_contract("attribution_report", report)


def build_attribution_report(
    *,
    candidate_batch: dict[str, Any],
    candidates: list[dict[str, Any]],
    outcomes: list[dict[str, Any]],
    dataset_snapshot_id: str,
    cost_model_snapshot_id: str,
    benchmark_symbol: str,
    windows: list[int],
    created_at: str,
) -> dict[str, Any]:
    metrics = build_attribution_metrics(outcomes, candidates)
    decision_quality = build_decision_quality_summary(outcomes, {row["candidate_id"]: row for row in candidates})
    status = "zero_candidate_batch_measured" if len(candidates) == 0 else "measured"
    report = {
        "candidate_batch_id": candidate_batch["candidate_batch_id"],
        "source_evidence_package_id": candidate_batch["source_evidence_package_id"],
        "dataset_snapshot_id": dataset_snapshot_id,
        "regime_snapshot_id": candidate_batch["regime_snapshot_id"],
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "benchmark_symbol": benchmark_symbol,
        "windows_measured": [f"{window}d" for window in windows],
        "candidate_count": metrics["candidate_count"],
        "measured_candidate_count": metrics["measured_candidate_count"],
        "unavailable_candidate_count": metrics["unavailable_candidate_count"],
        "metrics": metrics,
        "decision_quality_summary": decision_quality,
        "status": status,
        "compliance_label": "Candidate outcomes are observational attribution. Source backtests/model outputs remain hypothetical research evidence, not live achieved portfolio results or investment advice.",
        "created_at": created_at,
        "schema_version": ATTRIBUTION_SCHEMA_VERSION,
    }
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    report["attribution_report_id"] = f"attr_{candidate_batch['candidate_batch_id']}_{short_hash(report['content_hash'], 10)}"
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    validate_attribution_report(report)
    return report


def attribution_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Candidate Attribution Report",
        "",
        "This is an observation and attribution report, not trading execution.",
        "Backtests/model outputs remain hypothetical research evidence, not live achieved portfolio results or investment advice.",
        "",
        f"Candidate batch: `{report['candidate_batch_id']}`",
        f"Status: `{report['status']}`",
        f"Candidate count: {report['candidate_count']}",
        f"Measured candidate count: {report['measured_candidate_count']}",
        f"Unavailable candidate count: {report['unavailable_candidate_count']}",
        "",
        "## Mean Post-Cost Return By Window",
    ]
    for window, value in report["metrics"]["mean_post_cost_return_by_window"].items():
        lines.append(f"- {window}: {value}")
    lines.extend(["", "## Win Rate By Window"])
    for window, value in report["metrics"]["win_rate_by_window"].items():
        lines.append(f"- {window}: {value}")
    lines.extend(["", "## Decision Quality"])
    for key, value in report["decision_quality_summary"].items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def candidate_outcome_summary(report: dict[str, Any], attribution_report_path: Path) -> dict[str, Any]:
    summary = {
        "candidate_batch_id": report["candidate_batch_id"],
        "candidate_count": report["candidate_count"],
        "measured_candidate_count": report["measured_candidate_count"],
        "unavailable_candidate_count": report["unavailable_candidate_count"],
        "windows_measured": report["windows_measured"],
        "mean_post_cost_return_by_window": report["metrics"]["mean_post_cost_return_by_window"],
        "win_rate_by_window": report["metrics"]["win_rate_by_window"],
        "decision_quality_summary": report["decision_quality_summary"],
        "status": report["status"],
        "attribution_report_path": str(attribution_report_path),
        "schema_version": "candidate_outcome_summary.v1",
    }
    validate_contract("candidate_outcome_summary", summary)
    return summary

