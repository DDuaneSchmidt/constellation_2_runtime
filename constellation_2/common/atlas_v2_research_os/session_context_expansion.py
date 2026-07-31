from __future__ import annotations

import json
from datetime import date
from tempfile import TemporaryDirectory
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_family_discovery import build_candidate_family_discovery_report
from .historical_replay_engine import now_utc
from .observation_expansion import build_observation_expansion_report
from .session_context import SESSION_CONTEXTS, session_distribution

REPORT_DIRNAME = "session_context_expansion"

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "observation_analysis_allowed": True,
    "candidate_family_analysis_allowed": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "candidate_promotion_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
}

AUTHORITY_STATEMENT = (
    "Session context expansion is research-only. It preserves session context through observation, claim, "
    "hypothesis, candidate, and family analysis without authorizing trading, capital allocation, broker "
    "execution, position sizing, trade recommendations, candidate promotion, or automatic paper placement."
)


def build_session_context_expansion_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int = 12,
) -> dict[str, Any]:
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    root_path = Path(root)
    with TemporaryDirectory(prefix="atlas_session_context_observation_") as observation_root:
        observation_report = build_observation_expansion_report(
            root=Path(observation_root),
            day=day_value,
            created_at=created,
            dry_run_limit=dry_run_limit,
            write_import_report=False,
        )
    family_report = build_candidate_family_discovery_report(root=root_path, created_at=created)
    observation_metrics = observation_report.get("metrics") or {}
    family_metrics = family_report.get("session_context_metrics") or {}
    metrics = {
        "observations_by_session": observation_metrics.get("observations_by_session") or {},
        "claims_by_session": observation_metrics.get("claims_by_session") or {},
        "hypotheses_by_session": observation_metrics.get("hypotheses_by_session") or {},
        "eligible_candidates_by_session": family_metrics.get("eligible_candidates_by_session") or {},
        "family_count_by_session": family_metrics.get("family_count_by_session") or {},
        "best_session_contexts": family_metrics.get("best_session_contexts") or [],
        "worst_session_contexts": family_metrics.get("worst_session_contexts") or [],
    }
    report = {
        "schema_id": "atlas_v2_research_os_session_context_expansion_report_v1",
        "schema_version": "v1",
        "report_type": "SESSION_CONTEXT_EXPANSION",
        "created_at": created,
        "day": day_value,
        "session_contexts": list(SESSION_CONTEXTS),
        "metrics": metrics,
        "source_reports": {
            "observation_expansion": {
                "schema_id": observation_report.get("schema_id"),
                "selected_profile": observation_report.get("selected_profile"),
                "metrics": observation_metrics,
            },
            "candidate_family_discovery": {
                "schema_id": family_report.get("schema_id"),
                "summary": family_report.get("summary"),
                "session_context_metrics": family_metrics,
            },
        },
        "preservation_checks": {
            "observation_records_preserve_session_context": all(session in metrics["observations_by_session"] for session in SESSION_CONTEXTS),
            "claim_seeds_preserve_session_context": all(session in metrics["claims_by_session"] for session in SESSION_CONTEXTS),
            "hypothesis_dry_runs_preserve_session_context": all(session in metrics["hypotheses_by_session"] for session in SESSION_CONTEXTS),
            "candidate_family_assignments_preserve_session_context": all(session in metrics["eligible_candidates_by_session"] for session in SESSION_CONTEXTS),
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            AUTHORITY_STATEMENT,
            "Session metrics are analysis evidence only, not a trading schedule or recommendation.",
            "Best/worst labels rank research evidence quality in current artifacts only; they do not authorize capital or execution.",
        ],
    }
    _validate_authority(report)
    return report


def write_session_context_expansion_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int = 12,
) -> dict[str, Path]:
    report = build_session_context_expansion_report(root=root, day=day, created_at=created_at, dry_run_limit=dry_run_limit)
    out_root = Path(report_root) if report_root is not None else Path(root) / REPORT_DIRNAME
    day_root = out_root / str(report.get("day"))
    day_root.mkdir(parents=True, exist_ok=True)
    json_path = day_root / "session_context_expansion_report.json"
    summary_path = day_root / "session_context_expansion_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_session_context_expansion_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_session_context_expansion_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics") or {}
    lines = [
        "# Session Context Expansion",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Required Metrics",
        "",
        f"- Observations by session: {metrics.get('observations_by_session', {})}",
        f"- Claims by session: {metrics.get('claims_by_session', {})}",
        f"- Hypotheses by session: {metrics.get('hypotheses_by_session', {})}",
        f"- Eligible candidates by session: {metrics.get('eligible_candidates_by_session', {})}",
        f"- Family count by session: {metrics.get('family_count_by_session', {})}",
        f"- Best session contexts: {metrics.get('best_session_contexts', [])}",
        f"- Worst session contexts: {metrics.get('worst_session_contexts', [])}",
        "",
        "## Guardrails",
        "",
        AUTHORITY_STATEMENT,
        "",
    ]
    return "\n".join(lines)


def _validate_authority(report: dict[str, Any]) -> None:
    boundary = report.get("authority_boundary") or {}
    forbidden_true = [key for key, value in boundary.items() if key != "research_only" and key.endswith("authorized") and value is True]
    if boundary.get("research_only") is not True or forbidden_true:
        raise ValueError(f"session context authority boundary failed: {forbidden_true}")
    text = json.dumps(report, sort_keys=True).lower()
    for phrase in (
        "trade_recommendation_authorized\": true",
        "live_trading_authorized\": true",
        "broker_execution_authorized\": true",
        "capital_authorized\": true",
        "automatic_paper_trade_placement_authorized\": true",
    ):
        if phrase in text:
            raise ValueError(f"forbidden authority phrase present: {phrase}")


def session_metrics_from_records(records: list[dict[str, Any]]) -> dict[str, int]:
    return session_distribution(row.get("session_context") for row in records)
