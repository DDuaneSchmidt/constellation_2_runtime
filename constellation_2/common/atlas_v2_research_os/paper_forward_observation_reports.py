from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .paper_forward_observation_plans import build_paper_forward_observation_plans

REPORT_DIRNAME = "paper_forward_observation"


def write_paper_forward_observation_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, limit: int = 12) -> dict[str, Path]:
    root_path = Path(root)
    report = build_paper_forward_observation_plans(root_path, limit=limit)
    day_value = day or _today()
    out_root = root_path / REPORT_DIRNAME
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "paper_forward_observation_plans.json"
    summary_path = out_dir / "paper_forward_observation_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_paper_forward_observation_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_paper_forward_observation_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas Paper-Forward Observation Plans",
        "",
        f"Plans created: {report.get('plans_created', 0)}",
        f"Eligible candidates found: {report.get('eligible_candidate_count', 0)}",
        f"Candidates covered: {json.dumps(report.get('candidates_covered', []), sort_keys=True)}",
        f"Observation metrics: {json.dumps(report.get('observation_metrics', {}), sort_keys=True)}",
        f"Governance: {json.dumps(report.get('governance_result', {}), sort_keys=True)}",
        "",
        "## Top Ranked Plans",
    ]
    for row in report.get("top_12_ranked", []):
        lines.append(f"- rank={row['rank']} candidate={row['candidate_id']} mechanism={row['mechanism']} edge={row['edge_score']} replay={row['replay_score']} regime={row['regime']}")
    lines.extend([
        "",
        f"Human review queue size: {len(report.get('human_review_queue', []))}",
        "Authority: observation planning only; no live trading, broker execution, capital authority, position sizing, automatic paper trade placement, or candidate production promotion.",
        "",
    ])
    return "\n".join(lines)


def _today() -> str:
    return datetime.now(UTC).date().isoformat()
