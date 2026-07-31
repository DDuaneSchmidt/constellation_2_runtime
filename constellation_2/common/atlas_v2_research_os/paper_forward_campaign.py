from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "paper_forward_campaign"
AUTHORITY_BOUNDARY = {
    "paper_forward_observation_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


def build_paper_forward_campaign(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    sources = _load_sources(root_path)
    selected: dict[str, dict[str, Any]] = {}

    for row in _approved_readiness_candidates(sources["candidate_observation_readiness"])[:2]:
        _merge_candidate(
            selected,
            row["candidate_id"],
            row,
            source_bucket="approved_candidate",
            why="approved by observation-readiness review for paper-forward observation",
            priority=100 - len(selected),
        )

    for row in _observation_import_ready_candidates(sources["observation_trial"])[:3]:
        _merge_candidate(
            selected,
            row["candidate_id"],
            row,
            source_bucket="observation_import_ready",
            why="observation-import trial marked paper-forward ready with supported proxy backtest",
            priority=80 - len(selected),
        )

    for row in _backtest_supported_candidates(sources["candidate_backtests"])[:8]:
        _merge_candidate(
            selected,
            row["candidate_id"],
            row,
            source_bucket="backtest_supported",
            why="candidate backtest replay classified as BACKTEST_SUPPORTED",
            priority=60 - len(selected),
        )

    campaign = list(selected.values())
    campaign.sort(key=lambda row: (-float(row.get("_priority", 0)), -_score(row), row["candidate_id"]))
    for index, row in enumerate(campaign, start=1):
        row["rank"] = index
        row.pop("_priority", None)

    summary = {
        "campaign_candidate_count": len(campaign),
        "approved_candidates_selected": sum("approved_candidate" in row.get("source_buckets", []) for row in campaign),
        "observation_import_ready_selected": sum("observation_import_ready" in row.get("source_buckets", []) for row in campaign),
        "backtest_supported_selected": sum("backtest_supported" in row.get("source_buckets", []) for row in campaign),
        "deduplicated_overlap_count": sum(max(0, len(row.get("source_buckets", [])) - 1) for row in campaign),
        "human_action_required_count": sum(bool(row.get("human_action_required")) for row in campaign),
    }
    return {
        "schema_id": "atlas_v2_research_os_paper_forward_campaign",
        "schema_version": "1.0",
        "created_at": created_at or _now(),
        "source_reports": sources["source_reports"],
        "candidate_pool_policy": {
            "approved_candidates_target": 2,
            "observation_import_ready_target": 3,
            "backtest_supported_target": 8,
            "deduplicate_by": "candidate_id",
        },
        "summary": summary,
        "campaign_candidates": campaign,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "guardrails": [
            "Paper-forward observation only.",
            "No trade recommendations.",
            "No broker execution, capital allocation, position sizing, portfolio construction, automatic paper trade placement, or production promotion authority.",
        ],
    }


def write_paper_forward_campaign_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    root_path = Path(root)
    report = build_paper_forward_campaign(root_path)
    day_value = day or _today()
    out_root = root_path / REPORT_DIRNAME
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "paper_forward_campaign.json"
    summary_path = out_dir / "paper_forward_campaign_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_paper_forward_campaign_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_paper_forward_campaign_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas Paper-Forward Campaign",
        "",
        f"Campaign candidates: {report.get('summary', {}).get('campaign_candidate_count', 0)}",
        f"Approved candidates selected: {report.get('summary', {}).get('approved_candidates_selected', 0)}",
        f"Observation-import ready selected: {report.get('summary', {}).get('observation_import_ready_selected', 0)}",
        f"Backtest-supported selected: {report.get('summary', {}).get('backtest_supported_selected', 0)}",
        f"Deduplicated overlaps: {report.get('summary', {}).get('deduplicated_overlap_count', 0)}",
        "",
        "## Campaign",
    ]
    for row in report.get("campaign_candidates", []):
        lines.append(
            "- rank={rank} candidate={candidate_id} mechanism={mechanism} status={status} min_sample={minimum_sample_size} sources={sources}".format(
                rank=row.get("rank"),
                candidate_id=row.get("candidate_id"),
                mechanism=row.get("mechanism"),
                status=row.get("status"),
                minimum_sample_size=row.get("minimum_sample_size"),
                sources=",".join(row.get("source_buckets", [])),
            )
        )
        lines.append(f"  why: {row.get('why_selected')}")
        lines.append(f"  observe: {row.get('observation_rule')}")
        lines.append(f"  invalidate: {row.get('invalidation_rule')}")
    lines.extend(
        [
            "",
            "Authority: paper-forward observation only; no trade recommendations, broker execution, capital authority, position sizing, automatic paper trade placement, or production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _load_sources(root: Path) -> dict[str, Any]:
    names = [
        "candidate_backtests",
        "candidate_review",
        "candidate_observation_readiness",
        "paper_forward_observation",
        "observation_trial",
    ]
    reports: dict[str, Any] = {}
    paths: dict[str, str] = {}
    for name in names:
        path = root / name / "latest.json"
        paths[name] = str(path)
        reports[name] = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    reports["source_reports"] = paths
    return reports


def _approved_readiness_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = list(report.get("candidate_reviews") or [])
    ready = [
        row
        for row in rows
        if row.get("classification") == "READY_FOR_OBSERVATION"
        or row.get("recommended_human_decision") == "APPROVE_FOR_PAPER_FORWARD_OBSERVATION"
    ]
    return sorted(ready, key=lambda row: (-_float(row.get("edge_score")), row.get("candidate_id", "")))


def _observation_import_ready_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in report.get("trials", []) if row.get("paper_forward_ready") is True]
    return sorted(rows, key=lambda row: (-_float(((row.get("candidate_backtest") or {}).get("metrics") or {}).get("expectancy")), row.get("candidate_id", "")))


def _backtest_supported_candidates(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in report.get("candidates", []) if row.get("classification") == "BACKTEST_SUPPORTED"]
    return sorted(
        rows,
        key=lambda row: (
            -_float((row.get("metrics") or {}).get("expectancy")),
            -_float((row.get("metrics") or {}).get("profit_factor")),
            row.get("candidate_id", ""),
        ),
    )


def _merge_candidate(
    selected: dict[str, dict[str, Any]],
    candidate_id: str,
    source_row: dict[str, Any],
    *,
    source_bucket: str,
    why: str,
    priority: int,
) -> None:
    base = selected.get(candidate_id)
    normalized = _normalize_candidate(source_row, source_bucket=source_bucket, why=why)
    if base is None:
        normalized["_priority"] = priority
        selected[candidate_id] = normalized
        return
    base["source_buckets"] = sorted(set(base.get("source_buckets", []) + [source_bucket]))
    base["why_selected"] = f"{base['why_selected']}; {why}"
    base["_priority"] = max(float(base.get("_priority", 0)), priority)
    for key in ["backtest_classification", "expectancy", "profit_factor", "edge_score", "replay_score"]:
        if base.get(key) in {None, ""} and normalized.get(key) not in {None, ""}:
            base[key] = normalized[key]


def _normalize_candidate(row: dict[str, Any], *, source_bucket: str, why: str) -> dict[str, Any]:
    candidate_id = str(row.get("candidate_id") or "")
    mechanism = str(row.get("mechanism") or ((row.get("hypothesis") or {}).get("mechanism")) or "UNKNOWN")
    backtest = row.get("candidate_backtest") or {}
    backtest_spec = row.get("backtest_spec") or backtest.get("backtest_spec") or {}
    metrics = row.get("metrics") or backtest.get("metrics") or {}
    edge = row.get("edge_score")
    if edge is None and isinstance(row.get("edge_qualification"), dict):
        edge = row["edge_qualification"].get("edge_score")
    regime_constraints = row.get("regime_constraints") or backtest_spec.get("regime_constraints") or {}
    minimum_sample_size = row.get("minimum_sample_size") or ((backtest_spec.get("data_requirements") or {}).get("minimum_sample_size")) or 30
    observation_rule = (
        row.get("entry_observation_condition")
        or ((row.get("hypothesis") or {}).get("entry_observation_condition") if isinstance(row.get("hypothesis"), dict) else None)
        or backtest_spec.get("entry_observation_condition")
        or f"Record observation-only evidence when the {mechanism} setup recurs with matching regime context; do not place an order."
    )
    invalidation = (
        row.get("invalidating_conditions")
        or row.get("invalidating_condition")
        or ((row.get("hypothesis") or {}).get("invalidation_condition") if isinstance(row.get("hypothesis"), dict) else None)
        or backtest_spec.get("invalidation_condition")
        or ["Lineage breaks, regime context differs, evidence cannot be reconstructed, or any forbidden authority would be required."]
    )
    status = _status_for(source_bucket, row)
    return {
        "candidate_id": candidate_id,
        "rank": None,
        "mechanism": mechanism,
        "regime": row.get("regime") or regime_constraints.get("primary_regime") or backtest_spec.get("primary_regime") or "UNKNOWN",
        "source_buckets": [source_bucket],
        "why_selected": why,
        "observation_rule": observation_rule,
        "invalidation_rule": _stringify_rule(invalidation),
        "minimum_sample_size": int(minimum_sample_size),
        "status": status,
        "human_action_required": "Human review required before logging paper-forward observations; observation only, no order, sizing, capital, or broker action.",
        "edge_score": _nullable_float(edge),
        "replay_score": _nullable_float(row.get("replay_score") or ((row.get("historical_replay") or {}).get("score")) or backtest_spec.get("replay_score")),
        "backtest_classification": row.get("classification") or backtest.get("classification"),
        "expectancy": _nullable_float(metrics.get("expectancy")),
        "profit_factor": _nullable_float(metrics.get("profit_factor")),
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _status_for(source_bucket: str, row: dict[str, Any]) -> str:
    if source_bucket == "approved_candidate":
        return "READY_FOR_OBSERVATION_REVIEW"
    if source_bucket == "observation_import_ready":
        return "OBSERVATION_IMPORT_READY_FOR_REVIEW"
    if row.get("classification") == "BACKTEST_SUPPORTED" or (row.get("candidate_backtest") or {}).get("classification") == "BACKTEST_SUPPORTED":
        return "BACKTEST_SUPPORTED_PENDING_HUMAN_REVIEW"
    return "PENDING_HUMAN_REVIEW"


def _score(row: dict[str, Any]) -> float:
    return max(_float(row.get("edge_score")), _float(row.get("expectancy")), _float(row.get("profit_factor")) / 10.0)


def _stringify_rule(value: Any) -> str:
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    return str(value)


def _nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    return round(_float(value), 6)


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
