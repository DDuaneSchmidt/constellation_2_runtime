from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable

from .artifact_store import DEFAULT_STORE_ROOT
from .historical_replay_engine import now_utc

CORE_REGIMES = ["CHOP", "TRENDING"]
EXPANDED_REGIMES = [
    "HIGH_VOL",
    "LOW_VOL",
    "VOL_EXPANSION",
    "VOL_CONTRACTION",
    "BULL",
    "BEAR",
    "RISK_ON",
    "RISK_OFF",
]
UNKNOWN_REGIME = "UNKNOWN"
ATLAS_REGIMES = CORE_REGIMES + EXPANDED_REGIMES + [UNKNOWN_REGIME]

REPORT_DIRNAME = "regime_expansion"

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "observation_generation_allowed": True,
    "claim_research_allowed": True,
    "hypothesis_research_allowed": True,
    "candidate_family_measurement_allowed": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
}

AUTHORITY_STATEMENT = (
    "Regime expansion is research-only. It preserves regime labels through observation, claim, hypothesis, "
    "replay, candidate-eligibility, and family-count reports without authorizing trading, broker execution, "
    "capital allocation, position sizing, recommendations, or automatic paper placement."
)


def normalize_regime(value: Any) -> str:
    regime = str(value or UNKNOWN_REGIME).strip().upper()
    aliases = {
        "HIGH_VOLATILITY": "HIGH_VOL",
        "LOW_VOLATILITY": "LOW_VOL",
        "COMPRESSED_VOLATILITY": "VOL_CONTRACTION",
        "VOLATILITY_COMPRESSION": "VOL_CONTRACTION",
        "VOL_COMPRESSION": "VOL_CONTRACTION",
        "RANGE_BOUND": "CHOP",
        "RANGE": "CHOP",
        "MOMENTUM": "TRENDING",
        "INTRADAY_TREND": "TRENDING",
    }
    return aliases.get(regime, regime or UNKNOWN_REGIME)


def empty_regime_counts() -> dict[str, int]:
    return {regime: 0 for regime in ATLAS_REGIMES}


def regime_counts(rows: Iterable[dict[str, Any]], *, field: str = "regime") -> dict[str, int]:
    counts = empty_regime_counts()
    for row in rows:
        counts[normalize_regime(row.get(field))] = counts.get(normalize_regime(row.get(field)), 0) + 1
    return dict(counts)


def positive_replay_rate_by_regime(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    buckets: dict[str, dict[str, int]] = defaultdict(lambda: {"positive_replays": 0, "total_replays": 0})
    for row in rows:
        regime = normalize_regime(row.get("regime"))
        status = _replay_status(row)
        buckets[regime]["total_replays"] += 1
        if status == "REPLAY_POSITIVE":
            buckets[regime]["positive_replays"] += 1
    output: dict[str, dict[str, Any]] = {}
    for regime in ATLAS_REGIMES:
        values = buckets.get(regime, {"positive_replays": 0, "total_replays": 0})
        total = values["total_replays"]
        output[regime] = {
            "positive_replays": values["positive_replays"],
            "total_replays": total,
            "positive_replay_rate": round(values["positive_replays"] / total, 6) if total else 0.0,
        }
    return output


def eligible_candidates_by_regime(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = empty_regime_counts()
    for row in rows:
        eligible = (
            row.get("paper_forward_ready") is True
            or row.get("final_eligible") is True
            or row.get("paper_trade_eligible") is True
            or row.get("paper_forward_observation_candidate") is True
            or row.get("candidate_review_status") == "REVIEWABLE"
        )
        if eligible:
            regime = normalize_regime(row.get("regime"))
            counts[regime] = counts.get(regime, 0) + 1
    return counts


def family_count_by_regime(families: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = empty_regime_counts()
    for family in families:
        regime = normalize_regime(family.get("dominant_regime") or family.get("regime"))
        counts[regime] = counts.get(regime, 0) + 1
    return counts


def regime_metric_block(
    *,
    observations: Iterable[dict[str, Any]] = (),
    claims: Iterable[dict[str, Any]] = (),
    hypotheses: Iterable[dict[str, Any]] = (),
    replay_rows: Iterable[dict[str, Any]] = (),
    candidates: Iterable[dict[str, Any]] = (),
    families: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "regime_vocabulary": list(ATLAS_REGIMES),
        "observations_by_regime": regime_counts(observations),
        "claims_by_regime": regime_counts(claims),
        "hypotheses_by_regime": regime_counts(hypotheses),
        "positive_replay_rate_by_regime": positive_replay_rate_by_regime(replay_rows),
        "eligible_candidates_by_regime": eligible_candidates_by_regime(candidates),
        "family_count_by_regime": family_count_by_regime(families),
    }


def build_regime_expansion_report(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    day: str | None = None,
    created_at: str | None = None,
    observation_profile: str = "OBSERVATION_IMPORT_1000",
    dry_run_limit: int = 40,
) -> dict[str, Any]:
    from .candidate_family_discovery import build_candidate_family_discovery_report
    from .observation_expansion import build_observation_expansion_report

    root_path = Path(root)
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    observation_report = build_observation_expansion_report(
        root=root_path,
        profile=observation_profile,
        day=day_value,
        created_at=created,
        dry_run_limit=dry_run_limit,
        write_import_report=True,
    )
    try:
        family_report = build_candidate_family_discovery_report(root=root_path, created_at=created)
    except Exception as exc:
        family_report = {"families": [], "candidate_family_assignments": [], "summary": {}, "error": str(exc)}

    observation_regime_metrics = observation_report.get("regime_metrics") or {}
    dry_rows = observation_report.get("dry_run_sample") or []
    families = family_report.get("families") or []
    assignments = family_report.get("candidate_family_assignments") or []
    family_metrics = regime_metric_block(
        replay_rows=dry_rows,
        candidates=assignments,
        families=families,
    )
    metrics = {
        "regime_vocabulary": list(ATLAS_REGIMES),
        "observations_by_regime": observation_regime_metrics.get("observations_by_regime", empty_regime_counts()),
        "claims_by_regime": observation_regime_metrics.get("claims_by_regime", empty_regime_counts()),
        "hypotheses_by_regime": observation_regime_metrics.get("hypotheses_by_regime", empty_regime_counts()),
        "positive_replay_rate_by_regime": observation_regime_metrics.get("positive_replay_rate_by_regime", family_metrics["positive_replay_rate_by_regime"]),
        "eligible_candidates_by_regime": family_metrics["eligible_candidates_by_regime"],
        "family_count_by_regime": family_metrics["family_count_by_regime"],
    }
    return {
        "schema_id": "atlas_v2_research_os_regime_expansion_report_v1",
        "schema_version": "v1",
        "report_type": "REGIME_EXPANSION",
        "created_at": created,
        "day": day_value,
        "objective": "Expand Atlas observation and research reporting beyond CHOP and TRENDING regimes while preserving labels through downstream research metrics.",
        "added_regimes": list(EXPANDED_REGIMES),
        "canonical_regimes": list(ATLAS_REGIMES),
        "metrics": metrics,
        "source_reports": {
            "observation_expansion": str(root_path / "observation_expansion" / "latest.json"),
            "observation_import": "reports/atlas_v2_research_os/observation_import/latest.json",
            "candidate_family_discovery": str(root_path / "candidate_family_discovery" / "latest.json"),
        },
        "source_summaries": {
            "observation_expansion": observation_report.get("metrics", {}),
            "candidate_family_discovery": family_report.get("summary", {}),
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [AUTHORITY_STATEMENT],
    }


def write_regime_expansion_report(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    day: str | None = None,
    created_at: str | None = None,
    observation_profile: str = "OBSERVATION_IMPORT_1000",
    dry_run_limit: int = 40,
) -> dict[str, Path]:
    report = build_regime_expansion_report(root=root, day=day, created_at=created_at, observation_profile=observation_profile, dry_run_limit=dry_run_limit)
    out_root = Path(root) / REPORT_DIRNAME
    out_dir = out_root / str(report.get("day") or date.today().isoformat())
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "regime_expansion_report.json"
    summary_path = out_dir / "regime_expansion_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_regime_expansion_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_regime_expansion_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics") or {}
    lines = [
        "# Regime Expansion",
        "",
        f"Created: {report.get('created_at')}",
        f"Added regimes: {', '.join(report.get('added_regimes') or [])}",
        "",
        "## Required Metrics",
        "",
        f"- Observations by regime: {json.dumps(metrics.get('observations_by_regime', {}), sort_keys=True)}",
        f"- Claims by regime: {json.dumps(metrics.get('claims_by_regime', {}), sort_keys=True)}",
        f"- Hypotheses by regime: {json.dumps(metrics.get('hypotheses_by_regime', {}), sort_keys=True)}",
        f"- Positive replay rate by regime: {json.dumps(metrics.get('positive_replay_rate_by_regime', {}), sort_keys=True)}",
        f"- Eligible candidates by regime: {json.dumps(metrics.get('eligible_candidates_by_regime', {}), sort_keys=True)}",
        f"- Family count by regime: {json.dumps(metrics.get('family_count_by_regime', {}), sort_keys=True)}",
        "",
        "## Guardrails",
        "",
        AUTHORITY_STATEMENT,
        "",
    ]
    return "\n".join(lines)


def _replay_status(row: dict[str, Any]) -> str:
    replay = row.get("historical_replay") if isinstance(row.get("historical_replay"), dict) else {}
    return str(row.get("replay_status") or replay.get("status") or "UNKNOWN")
