from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.portfolio.evidence_inventory import RESEARCH_LABEL, _registry, _write_report, build_evidence_inventory
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


def _has_critical_missing(row: dict[str, Any]) -> bool:
    return any("critical" in str(item.get("reason") or "") for item in row.get("missing_artifacts") or [])


def _bucket(row: dict[str, Any], score: float) -> str:
    if _has_critical_missing(row):
        return "blocked"
    if row["overall_health"] in {"challenged", "retire_candidate"}:
        return "blocked"
    if row["paper_trial_status"] == "active" and row["overall_health"] == "watch" and row["recommended_action"] == "continue_research":
        return "high"
    if row["total_candidates"] < 30 or row["measured_candidates"] < 30:
        return "medium"
    if row["overall_health"] in {"retired", "archived"}:
        return "low"
    return "high" if score >= 70 else "medium"


def _priority_score(row: dict[str, Any]) -> float:
    score = 0.0
    if row["paper_trial_status"] == "active":
        score += 40
    if row["overall_health"] == "watch":
        score += 20
    if row["recommended_action"] == "continue_research":
        score += 15
    if row["ranking_quality_status"] == "useful":
        score += 15
    if row["total_candidates"] < 30 or row["measured_candidates"] < 30:
        score += 10
    if _has_critical_missing(row):
        score -= 100
    if row["overall_health"] == "challenged":
        score -= 30
    return score


def build_sleeve_comparison_report(*, store_root: Path | None = None, created_at: str | None = None) -> dict[str, Any]:
    inventory = build_evidence_inventory(store_root=store_root, created_at=created_at)
    comparison_rows: list[dict[str, Any]] = []
    for sleeve in inventory["sleeves"]:
        summary = sleeve.get("latest_summary") or {}
        total_candidates = int(summary.get("longitudinal_total_candidates") or summary.get("candidate_count") or 0)
        measured_candidates = int(summary.get("longitudinal_measured_candidates") or summary.get("paper_trial_measured_candidate_count") or 0)
        paper_trial_status = sleeve.get("paper_trial_status") or "missing"
        row = {
            "sleeve_id": sleeve["sleeve_id"],
            "sleeve_version_id": sleeve["sleeve_version_id"],
            "overall_health": sleeve["overall_health"],
            "backtest_health": "watch" if summary.get("excess_return_vs_benchmark") is not None and float(summary.get("excess_return_vs_benchmark")) < 0 else "insufficient_data",
            "candidate_health": "watch" if total_candidates == 0 else "healthy",
            "attribution_health": "watch" if total_candidates == 0 else "insufficient_data",
            "paper_trial_status": paper_trial_status,
            "ranking_quality_status": summary.get("ranking_quality_status") or "insufficient_sample",
            "total_candidates": total_candidates,
            "measured_candidates": measured_candidates,
            "zero_candidate_batches": int(summary.get("longitudinal_zero_candidate_batches") or (1 if total_candidates == 0 and sleeve["candidate_batch_ids"] else 0)),
            "post_cost_total_return": summary.get("post_cost_total_return"),
            "excess_return_vs_benchmark": summary.get("excess_return_vs_benchmark"),
            "recommended_action": sleeve.get("recommended_action") or summary.get("paper_trial_recommended_next_action") or "continue_research",
            "paper_trial_recommended_next_action": summary.get("paper_trial_recommended_next_action"),
            "missing_artifacts": sleeve.get("missing_artifacts", []),
        }
        # Prefer longitudinal learning totals when available in latest_summary-compatible fields.
        score = _priority_score(row)
        row["priority_score"] = score
        row["priority_bucket"] = _bucket(row, score)
        comparison_rows.append(row)
    comparison_rows.sort(key=lambda row: (-float(row["priority_score"]), row["sleeve_id"]))
    payload = {
        "created_at": created_at or utc_now_iso(),
        "sleeves_compared": len(comparison_rows),
        "comparison_rows": comparison_rows,
        "research_label": RESEARCH_LABEL,
        "schema_version": "sleeve_comparison_report.v1",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["sleeve_comparison_report_id"] = f"scmp_{short_hash(payload['content_hash'], 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_contract("sleeve_comparison_report", payload)
    return payload


def sleeve_comparison_markdown(payload: dict[str, Any]) -> str:
    lines = ["# Sleeve Comparison", "", payload["research_label"], "", "## Rows"]
    for row in payload["comparison_rows"]:
        lines.append(
            f"- {row['sleeve_id']}: health={row['overall_health']} bucket={row['priority_bucket']} "
            f"score={row['priority_score']} next={row['recommended_action']}"
        )
    return "\n".join(lines) + "\n"


def write_sleeve_comparison_report(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    payload = build_sleeve_comparison_report(store_root=store)
    row = {
        "sleeve_comparison_report_id": payload["sleeve_comparison_report_id"],
        "sleeves_compared": payload["sleeves_compared"],
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    return _write_report(
        store=store,
        family="sleeve_comparison",
        report_id=payload["sleeve_comparison_report_id"],
        payload=payload,
        registry_name="sleeve_comparison_reports.jsonl",
        registry_row=row,
        markdown=sleeve_comparison_markdown(payload),
        audit_action="sleeve_comparison_report_written",
        actor=actor,
    )


def latest_sleeve_comparison_report(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(_registry(store, "sleeve_comparison_reports.jsonl"))
    if not rows:
        return {}
    latest = sorted(rows, key=lambda row: str(row.get("created_at") or ""))[-1]
    return read_json(store / "portfolio_reports" / "sleeve_comparison" / f"{latest['sleeve_comparison_report_id']}.json")
