from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .bulk_observation_import import MARKET_STRUCTURES, stable_id
from .historical_replay_engine import now_utc
from .observation_expansion import AUTHORITY_BOUNDARY as OBSERVATION_AUTHORITY_BOUNDARY
from .observation_expansion import AUTHORITY_STATEMENT as OBSERVATION_AUTHORITY_STATEMENT
from .observation_expansion import build_observation_expansion_report

REPORT_DIRNAME = "market_structure_expansion"

AUTHORITY_BOUNDARY = {
    **OBSERVATION_AUTHORITY_BOUNDARY,
    "market_structure_research_only": True,
    "candidate_creation_authorized": False,
    "paper_position_creation_authorized": False,
}

AUTHORITY_STATEMENT = (
    "Market structure expansion is research-only. It preserves structure dimensions across observation, "
    "claim, hypothesis, replay, family, and report evidence, but does not authorize trading, broker execution, "
    "capital allocation, sizing, recommendations, candidate promotion, or paper placement."
)


def build_market_structure_expansion_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    profile: str = "OBSERVATION_IMPORT_1000",
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int = 40,
) -> dict[str, Any]:
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    expansion = build_observation_expansion_report(
        root=root,
        profile=profile,
        day=day_value,
        created_at=created,
        dry_run_limit=dry_run_limit,
        write_import_report=True,
    )
    import_report = expansion.get("import_report", {})
    metrics = _market_structure_metrics(expansion)
    report = {
        "schema_id": "atlas_v2_research_os_market_structure_expansion_report_v1",
        "schema_version": "v1",
        "report_type": "MARKET_STRUCTURE_EXPANSION",
        "report_id": stable_id("market_structure_expansion", [profile, created, metrics]),
        "created_at": created,
        "day": day_value,
        "selected_profile": profile,
        "market_structures_supported": list(MARKET_STRUCTURES),
        "required_metrics": metrics,
        "observation_expansion_report": expansion,
        "source_reports": {
            "observation_import": import_report,
            "observation_expansion": {"embedded": True, "schema_id": expansion.get("schema_id")},
        },
        "preservation_contract": {
            "observation_record_field": "market_structure",
            "cluster_field": "market_structure",
            "claim_seed_field": "market_structure",
            "hypothesis_field": "market_structure",
            "family_dimension": "market_structures",
            "unknown_policy": "Do not infer missing market structure; preserve UNKNOWN.",
        },
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            AUTHORITY_STATEMENT,
            OBSERVATION_AUTHORITY_STATEMENT,
            "No consumer may invent market-structure truth; consumers must read preserved structure fields or UNKNOWN.",
        ],
    }
    _validate_authority(report)
    return report


def write_market_structure_expansion_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    profile: str = "OBSERVATION_IMPORT_1000",
    day: str | None = None,
    created_at: str | None = None,
    dry_run_limit: int = 40,
) -> dict[str, Path]:
    report = build_market_structure_expansion_report(
        root=root,
        profile=profile,
        day=day,
        created_at=created_at,
        dry_run_limit=dry_run_limit,
    )
    out_root = Path(report_root) if report_root is not None else Path(root) / REPORT_DIRNAME
    day_value = day or report["day"]
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "market_structure_expansion_report.json"
    summary_path = out_dir / "market_structure_expansion_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_market_structure_expansion_summary(report)
    for path in (json_path, latest_json):
        path.write_text(payload, encoding="utf-8")
    for path in (summary_path, latest_summary):
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_market_structure_expansion_summary(report: dict[str, Any]) -> str:
    metrics = report.get("required_metrics", {})
    lines = [
        "# Market Structure Expansion",
        "",
        f"Created: {report.get('created_at')}",
        f"Selected profile: {report.get('selected_profile')}",
        "",
        "## Required Metrics",
        "",
        f"- Observations by structure: {json.dumps(metrics.get('observations_by_structure', {}), sort_keys=True)}",
        f"- Claims by structure: {json.dumps(metrics.get('claims_by_structure', {}), sort_keys=True)}",
        f"- Hypotheses by structure: {json.dumps(metrics.get('hypotheses_by_structure', {}), sort_keys=True)}",
        f"- Positive replay rate by structure: {json.dumps(metrics.get('positive_replay_rate_by_structure', {}), sort_keys=True)}",
        f"- Eligible candidates by structure: {json.dumps(metrics.get('eligible_candidates_by_structure', {}), sort_keys=True)}",
        f"- Top structures among winners: {json.dumps(metrics.get('top_structures_among_winners', []), sort_keys=True)}",
        f"- Top structures among failures: {json.dumps(metrics.get('top_structures_among_failures', []), sort_keys=True)}",
        "",
        "## Guardrails",
        "",
        AUTHORITY_STATEMENT,
        "",
    ]
    return "\n".join(lines)


def _market_structure_metrics(expansion: dict[str, Any]) -> dict[str, Any]:
    base = expansion.get("metrics", {})
    dry_rows = [row for row in expansion.get("dry_run_sample", []) if isinstance(row, dict)]
    winners = [row for row in dry_rows if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    failures = [row for row in dry_rows if row.get("historical_replay", {}).get("status") != "REPLAY_POSITIVE"]
    eligible = [row for row in dry_rows if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    return {
        "observations_by_structure": _complete_counts(base.get("observations_by_structure", {})),
        "claims_by_structure": _complete_counts(base.get("claims_by_structure", {})),
        "hypotheses_by_structure": _complete_counts(base.get("hypotheses_by_structure", {})),
        "positive_replay_rate_by_structure": {key: base.get("positive_replay_rate_by_structure", {}).get(key, 0.0) for key in _structure_order()},
        "eligible_candidates_by_structure": _complete_counts(_count_structures(eligible)),
        "top_structures_among_winners": _top_structures(winners),
        "top_structures_among_failures": _top_structures(failures),
    }


def _structure(row: dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    return str(row.get("market_structure") or metadata.get("market_structure") or "UNKNOWN").upper()


def _count_structures(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(_structure(row) for row in rows))


def _complete_counts(counts: dict[str, Any]) -> dict[str, int]:
    source = {str(key).upper(): int(value) for key, value in (counts or {}).items()}
    return {structure: source.get(structure, 0) for structure in _structure_order()}


def _structure_order() -> list[str]:
    return list(MARKET_STRUCTURES) + ["UNKNOWN"]


def _top_structures(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"market_structure": key, "count": count} for key, count in Counter(_structure(row) for row in rows).most_common()]


def _validate_authority(report: dict[str, Any]) -> None:
    boundary = report.get("authority_boundary", {})
    allowed_true = {"research_only", "observation_import_allowed", "claim_seed_allowed", "hypothesis_dry_run_allowed", "historical_replay_dry_run_allowed", "market_structure_research_only"}
    forbidden_true = [key for key, value in boundary.items() if value is True and key not in allowed_true]
    if boundary.get("research_only") is not True or forbidden_true:
        raise ValueError(f"market structure expansion authority boundary failed: {forbidden_true}")


def main() -> int:
    paths = write_market_structure_expansion_report()
    print(json.dumps({key: str(value) for key, value in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
