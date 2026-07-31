from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .paper_forward_feedback import route_paper_forward_feedback
from .paper_forward_outcome_governance import validate_paper_forward_outcome_allowed
from .paper_forward_outcomes import (
    create_paper_forward_observation_plan,
    record_paper_forward_observation_result,
)

PAPER_FORWARD_OUTCOME_REPORT_ROOT = Path("reports/atlas_v2_research_os/paper_forward_outcomes")


def build_paper_forward_outcome_report(outcomes: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    results = list(outcomes or demo_paper_forward_outcomes())
    for result in results:
        validate_paper_forward_outcome_allowed(result)
    status_counts = Counter(result.get("status", "PENDING") for result in results)
    survived_mechanisms = Counter()
    weakened_mechanisms = Counter()
    falsified_mechanisms = Counter()
    for result in results:
        tags = result.get("mechanism_tags", []) or ["UNKNOWN"]
        if result.get("status") == "SURVIVED":
            survived_mechanisms.update(tags)
        elif result.get("status") == "WEAKENED":
            weakened_mechanisms.update(tags)
        elif result.get("status") == "FALSIFIED":
            falsified_mechanisms.update(tags)
    return {
        "schema_id": "atlas_v2_research_os_paper_forward_outcome_report_v1",
        "schema_version": "v1",
        "day": date.today().isoformat(),
        "outcomes_recorded": len(results),
        "pending": status_counts.get("PENDING", 0),
        "active_observation": status_counts.get("ACTIVE_OBSERVATION", 0),
        "survived": status_counts.get("SURVIVED", 0),
        "weakened": status_counts.get("WEAKENED", 0),
        "falsified": status_counts.get("FALSIFIED", 0),
        "needs_more_data": status_counts.get("NEEDS_MORE_DATA", 0),
        "retired": status_counts.get("RETIRED", 0),
        "top_survived_mechanisms": _top_counter(survived_mechanisms),
        "top_weakened_mechanisms": _top_counter(weakened_mechanisms),
        "top_falsified_mechanisms": _top_counter(falsified_mechanisms),
        "outcomes": results,
        "authority_boundary": {
            "memory_update_allowed": True,
            "research_effectiveness_update_allowed": True,
            "trading_authorized": False,
            "capital_authorized": False,
            "broker_execution_authorized": False,
            "position_sizing_authorized": False,
            "candidate_promotion_authorized": False,
            "production_promotion_authorized": False,
        },
    }


def write_paper_forward_outcome_report(
    outcomes: list[dict[str, Any]] | None = None,
    *,
    root: str | Path = PAPER_FORWARD_OUTCOME_REPORT_ROOT,
    day: str | None = None,
    feedback_root: str | Path | None = None,
) -> dict[str, Path]:
    report = build_paper_forward_outcome_report(outcomes)
    if feedback_root is not None:
        report["feedback_routes"] = [route_paper_forward_feedback(outcome, root=feedback_root) for outcome in report["outcomes"]]
    day_value = day or report["day"]
    out_root = Path(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "paper_forward_outcome_report.json"
    md_path = out_dir / "paper_forward_outcome_summary.md"
    latest_json = out_root / "latest.json"
    latest_md = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary = render_paper_forward_outcome_summary(report)
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def audit_paper_forward_outcome_report(root: str | Path = PAPER_FORWARD_OUTCOME_REPORT_ROOT) -> dict[str, Any]:
    failures: list[str] = []
    root_path = Path(root)
    if root_path.exists():
        for path in root_path.rglob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                for result in payload.get("outcomes", []):
                    validate_paper_forward_outcome_allowed(result)
                validate_paper_forward_outcome_allowed({"authority_boundary": payload.get("authority_boundary", {})})
            except Exception as exc:
                failures.append(f"{path.as_posix()}: {exc}")
    return {"paper_forward_outcome_audit_ok": not failures, "paper_forward_outcome_audit_failures": failures}


def render_paper_forward_outcome_summary(report: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Atlas Paper-Forward Outcome Summary",
            "",
            f"Outcomes recorded: {report['outcomes_recorded']}",
            f"Pending: {report['pending']}",
            f"Active observation: {report['active_observation']}",
            f"Survived: {report['survived']}",
            f"Weakened: {report['weakened']}",
            f"Falsified: {report['falsified']}",
            f"Needs more data: {report['needs_more_data']}",
            f"Retired: {report['retired']}",
            f"Top survived mechanisms: {json.dumps(report['top_survived_mechanisms'], sort_keys=True)}",
            f"Top weakened mechanisms: {json.dumps(report['top_weakened_mechanisms'], sort_keys=True)}",
            f"Top falsified mechanisms: {json.dumps(report['top_falsified_mechanisms'], sort_keys=True)}",
            "",
            "Paper-forward outcome tracking updates memory and research effectiveness only. It does not authorize trading, capital, execution, sizing, recommendations, or promotion.",
            "",
        ]
    )


def demo_paper_forward_outcomes() -> list[dict[str, Any]]:
    survived_plan = create_paper_forward_observation_plan(
        candidate_id="paper-forward-demo-survived",
        plan_id="paper-forward-plan-demo-survived",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        regime_context={"label": "OPENING_SESSION_HIGH_VOLATILITY"},
        source_artifact_ids=["paper-candidate-demo-opening-range"],
        mechanism_tags=["OPENING_RANGE", "VOLATILITY_EXPANSION"],
        created_at="2026-06-05T00:00:00Z",
    )
    weakened_plan = create_paper_forward_observation_plan(
        candidate_id="paper-forward-demo-weakened",
        plan_id="paper-forward-plan-demo-weakened",
        observation_start="2026-06-01",
        observation_end="2026-06-05",
        regime_context={"label": "MIDDAY_SESSION_CHOP"},
        source_artifact_ids=["paper-candidate-demo-mean-reversion"],
        mechanism_tags=["MEAN_REVERSION"],
        created_at="2026-06-05T00:00:00Z",
    )
    survived_samples = [{"return": value, "regime": "OPENING_SESSION_HIGH_VOLATILITY"} for value in [0.012, 0.008, -0.003, 0.010, 0.006, 0.004]]
    weakened_samples = [{"return": value, "regime": "MIDDAY_SESSION_CHOP"} for value in [-0.009, -0.006, 0.002, -0.007, -0.004, 0.001]]
    return [
        record_paper_forward_observation_result(survived_plan, survived_samples, created_at="2026-06-05T00:00:00Z"),
        record_paper_forward_observation_result(weakened_plan, weakened_samples, created_at="2026-06-05T00:00:00Z"),
    ]


def _top_counter(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    return [{"mechanism": key, "count": value} for key, value in counter.most_common(limit)]
