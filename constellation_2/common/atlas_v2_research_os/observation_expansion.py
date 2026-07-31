from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .bulk_observation_import import (
    EXPANDED_SYMBOLS,
    MECHANISMS,
    REGIMES,
    SOURCE_TYPES,
    TIMEFRAMES,
    MARKET_STRUCTURES,
    seed_observations_for_profile,
    stable_id,
)
from .historical_replay_engine import now_utc
from .observation_models import OBSERVATION_WORKLOAD_PROFILES
from .observation_to_claim import convert_observation_cluster_to_claim
from .observation_trial import _hypothesis_from_claim, _run_replay
from .session_context import SESSION_CONTEXTS, normalize_session_context, session_distribution
from .regime_expansion import regime_metric_block, normalize_regime

REPORT_DIRNAME = "observation_expansion"
DEFAULT_EXPANSION_PROFILE = "OBSERVATION_IMPORT_1000"
REQUESTED_PROFILES = ["OBSERVATION_IMPORT_100", "OBSERVATION_IMPORT_1000", "OBSERVATION_IMPORT_5000", "EXPANDED_OBSERVATION_TRIAL_5000", "EXPANDED_OBSERVATION_TRIAL_FOCUSED"]

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "observation_import_allowed": True,
    "claim_seed_allowed": True,
    "hypothesis_dry_run_allowed": True,
    "historical_replay_dry_run_allowed": True,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
}

AUTHORITY_STATEMENT = (
    "Observation expansion is research-only. It may generate/import structured observations and run a bounded "
    "claim-to-hypothesis-to-replay dry run, but it does not authorize live trading, broker execution, capital "
    "allocation, position sizing, trade recommendations, candidate promotion, production promotion, or automatic "
    "paper trade placement."
)


def build_observation_expansion_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    profile: str = DEFAULT_EXPANSION_PROFILE,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int = 12,
    write_import_report: bool = True,
) -> dict[str, Any]:
    if profile not in REQUESTED_PROFILES:
        raise ValueError(f"unsupported expansion profile: {profile}")
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    root_path = Path(root)
    seed = seed_observations_for_profile(
        profile,
        root=root_path,
        output_path=root_path / f"{profile.lower()}_observations.json",
        created_at=created,
        write_report=write_import_report,
    )
    import_report = seed["import_report"]
    clusters = [row for row in import_report.get("clusters", []) if isinstance(row, dict)]
    dry_run_rows = _run_claim_hypothesis_replay_sample(clusters[:dry_run_limit], created_at=created)
    metrics = _metrics(import_report, dry_run_rows)
    regime_metrics = regime_metric_block(
        observations=(import_report.get("batch") or {}).get("records") or [],
        claims=import_report.get("claim_seeds") or [],
        hypotheses=dry_run_rows,
        replay_rows=dry_run_rows,
        candidates=dry_run_rows,
    )
    report = {
        "schema_id": "atlas_v2_research_os_observation_expansion_report_v1",
        "schema_version": "v1",
        "report_type": "OBSERVATION_SOURCE_EXPANSION",
        "report_id": stable_id("obs_expansion", [profile, created, metrics]),
        "created_at": created,
        "day": day_value,
        "selected_profile": profile,
        "profiles": _profile_specs(),
        "dimensions_supported": {
            "symbol_universe": EXPANDED_SYMBOLS,
            "timeframe": TIMEFRAMES,
            "mechanism": MECHANISMS,
            "regime": REGIMES,
            "source_type": SOURCE_TYPES,
            "session_context": list(SESSION_CONTEXTS),
            "market_structure": MARKET_STRUCTURES,
            "confidence": "float between 0.0 and 1.0, generated across confidence bands",
        },
        "pipeline_dry_run": ["import", "Claim", "Hypothesis", "Historical Replay"],
        "metrics": metrics,
        "regime_metrics": regime_metrics,
        "coverage": _coverage(import_report),
        "import_report": {
            "profile": profile,
            "source_path": seed["path"],
            "latest_report_path": "reports/atlas_v2_research_os/observation_import/latest.json" if write_import_report else "not written",
        },
        "dry_run_sample": dry_run_rows,
        "next_expansion_plan": [
            "Use OBSERVATION_IMPORT_1000 as the default structured research batch.",
            "Reserve OBSERVATION_IMPORT_5000 or EXPANDED_OBSERVATION_TRIAL_5000 for offline batch review after the 1000-row dry run remains stable.",
            "Monitor invalid rows, duplicate rate, cluster breadth, and replay-positive rate before increasing downstream workload.",
            "Keep all outputs paper-forward observation evidence only until separate human review approves any later workflow.",
        ],
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "limitations": [
            AUTHORITY_STATEMENT,
            "Generated observations are structured seed data, not market instructions or investment advice.",
            "Dry-run replay is bounded validation evidence and does not create candidates or paper positions.",
        ],
    }
    _validate_authority(report)
    return report


def write_observation_expansion_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    profile: str = DEFAULT_EXPANSION_PROFILE,
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int = 12,
) -> dict[str, Path]:
    report = build_observation_expansion_report(
        root=root,
        profile=profile,
        day=day,
        created_at=created_at,
        dry_run_limit=dry_run_limit,
        write_import_report=True,
    )
    out_root = Path(report_root) if report_root is not None else Path(root) / REPORT_DIRNAME
    day_value = day or report["day"]
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "observation_expansion_report.json"
    summary_path = out_dir / "observation_expansion_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_observation_expansion_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_observation_expansion_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics", {})
    coverage = report.get("coverage", {})
    lines = [
        "# Observation Source Expansion",
        "",
        f"Created: {report.get('created_at')}",
        f"Selected profile: {report.get('selected_profile')}",
        "",
        "## Metrics",
        "",
        f"- Observations generated: {metrics.get('observations_generated', 0)}",
        f"- Observations imported: {metrics.get('observations_imported', 0)}",
        f"- Clusters created: {metrics.get('clusters_created', 0)}",
        f"- Claims generated: {metrics.get('claims_generated', 0)}",
        f"- Dry-run hypotheses generated: {metrics.get('dry_run_hypotheses_generated', 0)}",
        f"- Dry-run replays run: {metrics.get('dry_run_replays_run', 0)}",
        f"- Dry-run positive replay rate: {metrics.get('dry_run_positive_replay_rate', 0.0)}",
        f"- Observations by session: {metrics.get('observations_by_session', {})}",
        f"- Claims by session: {metrics.get('claims_by_session', {})}",
        f"- Hypotheses by session: {metrics.get('hypotheses_by_session', {})}",
        f"- Structures covered: {coverage.get('market_structures_covered', 0)}",
        f"- Observations by regime: {report.get('regime_metrics', {}).get('observations_by_regime', {})}",
        f"- Claims by regime: {report.get('regime_metrics', {}).get('claims_by_regime', {})}",
        f"- Hypotheses by regime: {report.get('regime_metrics', {}).get('hypotheses_by_regime', {})}",
        "",
        "## Coverage",
        "",
        f"- Symbols covered: {coverage.get('symbols_covered', 0)}",
        f"- Timeframes covered: {coverage.get('timeframes_covered', 0)}",
        f"- Mechanisms covered: {coverage.get('mechanisms_covered', 0)}",
        f"- Regimes covered: {coverage.get('regimes_covered', 0)}",
        f"- Source types covered: {coverage.get('source_types_covered', 0)}",
        f"- Market structures covered: {coverage.get('market_structures_covered', 0)}",
        "",
        "## Dry-Run Sample",
        "",
    ]
    for row in report.get("dry_run_sample", [])[:20]:
        lines.append(
            f"- {row.get('cluster_id')} -> {row.get('hypothesis_id')}: "
            f"{row.get('mechanism')} {row.get('regime')} replay={row.get('historical_replay', {}).get('status')} "
            f"score={row.get('historical_replay', {}).get('score')}"
        )
    lines.extend(["", "## Authority", "", AUTHORITY_STATEMENT, ""])
    return "\n".join(lines)


def _run_claim_hypothesis_replay_sample(clusters: list[dict[str, Any]], *, created_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, cluster in enumerate(clusters):
        claim = convert_observation_cluster_to_claim(cluster)
        hypothesis = _hypothesis_from_claim(claim, index=index, created_at=created_at)
        replay = _run_replay(hypothesis, claim=claim, created_at=created_at)
        rows.append({
            "cluster_id": cluster.get("cluster_id"),
            "claim_seed_id": claim.get("claim_seed_id"),
            "hypothesis_id": hypothesis.get("hypothesis_id"),
            "mechanism": hypothesis.get("mechanism"),
            "regime": hypothesis.get("regime"),
            "session_context": normalize_session_context((claim.get("metadata") or {}).get("session_context")),
            "market_structure": hypothesis.get("market_structure") or claim.get("market_structure") or "UNKNOWN",
            "claim_text": claim.get("claim_text"),
            "hypothesis": hypothesis.get("hypothesis"),
            "historical_replay": {
                "replay_id": replay.get("replay_id"),
                "status": replay.get("certification", {}).get("status"),
                "score": replay.get("metrics", {}).get("historical_replay_score"),
                "sample_size": replay.get("sample_size"),
                "expectancy": replay.get("metrics", {}).get("expectancy"),
            },
            "authority_boundary": dict(AUTHORITY_BOUNDARY),
        })
    return rows


def _profile_specs() -> dict[str, dict[str, Any]]:
    return {
        name: {
            "max_observations": OBSERVATION_WORKLOAD_PROFILES[name],
            "purpose": _profile_purpose(name),
            "authority": "research_only_observation_import_no_trading_capital_broker_or_promotion_authority",
        }
        for name in REQUESTED_PROFILES
    }


def _profile_purpose(name: str) -> str:
    if name == "OBSERVATION_IMPORT_100":
        return "smoke profile for fast importer and governance validation"
    if name == "OBSERVATION_IMPORT_1000":
        return "default structured expansion profile for observation-driven hypothesis generation"
    if name == "EXPANDED_OBSERVATION_TRIAL_FOCUSED":
        return "focused expanded-search profile using retained high-signal dimensions from the gate audit"
    return "offline deep batch profile for later scale testing after 1000-row stability"


def _metrics(import_report: dict[str, Any], dry_run_rows: list[dict[str, Any]]) -> dict[str, Any]:
    positives = [row for row in dry_run_rows if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    records = [row for row in import_report.get("batch", {}).get("records", []) if isinstance(row, dict)]
    claims = [row for row in import_report.get("claim_seeds", []) if isinstance(row, dict)]
    clusters = [row for row in import_report.get("clusters", []) if isinstance(row, dict)]
    return {
        "observations_generated": int(import_report.get("raw_observations") or 0),
        "observations_imported": int(import_report.get("valid_observations") or 0),
        "invalid_observations": int(import_report.get("invalid_observations") or 0),
        "duplicates_skipped": int(import_report.get("duplicates_skipped") or 0),
        "clusters_created": int(import_report.get("clusters_created") or 0),
        "claims_generated": int(import_report.get("claims_created") or 0),
        "backlog_items_created": int(import_report.get("backlog_items_created") or 0),
        "dry_run_hypotheses_generated": len(dry_run_rows),
        "dry_run_replays_run": len(dry_run_rows),
        "dry_run_positive_replays": len(positives),
        "dry_run_positive_replay_rate": round(len(positives) / len(dry_run_rows), 6) if dry_run_rows else 0.0,
        "observations_by_session": session_distribution((row.get("metadata") or {}).get("session_context") for row in records),
        "claims_by_session": session_distribution((row.get("metadata") or {}).get("session_context") for row in clusters),
        "hypotheses_by_session": session_distribution(row.get("session_context") for row in dry_run_rows),
        "observations_by_structure": _count_structures(records),
        "clusters_by_structure": _count_structures(clusters),
        "claims_by_structure": _count_structures(claims),
        "hypotheses_by_structure": _count_structures(dry_run_rows),
        "positive_replays_by_structure": _count_structures(positives),
        "positive_replay_rate_by_structure": _rate_by_structure(dry_run_rows, positives),
    }


def _coverage(import_report: dict[str, Any]) -> dict[str, Any]:
    records = [row for row in import_report.get("batch", {}).get("records", []) if isinstance(row, dict)]
    source_types = {str(row.get("metadata", {}).get("source_type") or "UNKNOWN") for row in records}
    market_structures = {str(row.get("market_structure") or row.get("metadata", {}).get("market_structure") or "UNKNOWN").upper() for row in records}
    return {
        "symbols_covered": len({str(row.get("symbol") or "").upper() for row in records}),
        "timeframes_covered": len({str(row.get("timeframe") or "").lower() for row in records}),
        "mechanisms_covered": len({str(row.get("mechanism") or "").upper() for row in records}),
        "regimes_covered": len({normalize_regime(row.get("regime")) for row in records}),
        "source_types_covered": len(source_types),
        "session_contexts_covered": len({normalize_session_context((row.get("metadata") or {}).get("session_context")) for row in records if normalize_session_context((row.get("metadata") or {}).get("session_context")) in SESSION_CONTEXTS}),
        "market_structures_covered": len(market_structures),
        "confidence_min": min((float(row.get("confidence") or 0.0) for row in records), default=0.0),
        "confidence_max": max((float(row.get("confidence") or 0.0) for row in records), default=0.0),
        "source_types": sorted(source_types),
        "market_structures": sorted(market_structures),
    }


def _structure(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return str(row.get("market_structure") or metadata.get("market_structure") or "UNKNOWN").upper()


def _count_structures(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(sorted(Counter(_structure(row) for row in rows).items()))


def _rate_by_structure(all_rows: list[dict[str, Any]], positive_rows: list[dict[str, Any]]) -> dict[str, float]:
    totals = Counter(_structure(row) for row in all_rows)
    positives = Counter(_structure(row) for row in positive_rows)
    return {structure: round(positives.get(structure, 0) / total, 6) for structure, total in sorted(totals.items()) if total}


def _validate_authority(report: dict[str, Any]) -> None:
    boundary = report.get("authority_boundary", {})
    forbidden_true = [key for key, value in boundary.items() if key not in {"research_only", "observation_import_allowed", "claim_seed_allowed", "hypothesis_dry_run_allowed", "historical_replay_dry_run_allowed"} and value is True]
    if boundary.get("research_only") is not True or forbidden_true:
        raise ValueError(f"observation expansion authority boundary failed: {forbidden_true}")
    text = json.dumps(report, sort_keys=True).lower()
    forbidden = [
        "live_trading_authorized\": true",
        "broker_execution_authorized\": true",
        "capital_authorized\": true",
        "candidate_promotion_authorized\": true",
        "automatic_paper_trade_placement_authorized\": true",
    ]
    for phrase in forbidden:
        if phrase in text:
            raise ValueError(f"forbidden authority phrase present: {phrase}")


def main() -> int:
    paths = write_observation_expansion_report()
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
