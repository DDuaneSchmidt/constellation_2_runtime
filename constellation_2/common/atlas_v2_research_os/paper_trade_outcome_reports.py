from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .candidate_survival_analytics import compute_candidate_survival, survival_counts
from .paper_trade_feedback import (
    derive_feedback_from_outcome,
    update_learning_validation_from_paper_outcome,
    update_memory_from_paper_outcome,
    update_research_effectiveness_from_paper_outcome,
)
from .paper_trade_outcome_governance import validate_paper_trade_outcome_report
from .paper_trade_outcome_models import PAPER_TRADE_OUTCOME_LIMITATION
from .paper_trade_outcomes import list_paper_trade_outcomes

REPORT_ROOT = Path("reports/atlas_v2_research_os/paper_trade_outcomes")


def build_paper_trade_outcome_report(root: str | Path, outcomes: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    rows = list(outcomes if outcomes is not None else list_paper_trade_outcomes(root))
    survival = [compute_candidate_survival(row) for row in rows]
    feedback = [signal for outcome, survival_row in zip(rows, survival) for signal in derive_feedback_from_outcome(outcome, survival_row)]
    memory_updates = [update_memory_from_paper_outcome(root, outcome, survival_row) for outcome, survival_row in zip(rows, survival)]
    effectiveness_updates = [update_research_effectiveness_from_paper_outcome(root, outcome, survival_row) for outcome, survival_row in zip(rows, survival)]
    validation_updates = [update_learning_validation_from_paper_outcome(root, outcome, survival_row) for outcome, survival_row in zip(rows, survival)]
    report = {
        "schema_id": "atlas_v2_research_os_paper_trade_outcomes.v1",
        "outcome_count": len(rows),
        "outcomes": rows,
        "candidate_survival_records": survival,
        "candidate_survival_counts": survival_counts(survival),
        "falsified_candidates": [row for row in survival if row["survival_state"] == "FALSIFIED"],
        "weakened_candidates": [row for row in survival if row["survival_state"] == "WEAKENED"],
        "survived_candidates": [row for row in survival if row["survival_state"] == "SURVIVED_INITIAL_TEST"],
        "needs_more_data_candidates": [row for row in survival if row["survival_state"] == "NEEDS_MORE_DATA"],
        "feedback_signals": feedback,
        "memory_updates": memory_updates,
        "research_effectiveness_updates": effectiveness_updates,
        "learning_validation_updates": validation_updates,
        "authority_boundary": {
            "paper_only": True,
            "live_trading_allowed": False,
            "capital_authorized": False,
            "candidate_promotion_authorized": False,
            "position_sizing_authorized": False,
            "portfolio_construction_authorized": False,
            "limitation": PAPER_TRADE_OUTCOME_LIMITATION,
        },
    }
    validate_paper_trade_outcome_report(report)
    return report


def write_paper_trade_outcome_report(root: str | Path, outcomes: list[dict[str, Any]] | None = None, *, day: str | None = None) -> dict[str, Path]:
    report = build_paper_trade_outcome_report(root, outcomes)
    day_value = day or date.today().isoformat()
    day_root = REPORT_ROOT / day_value
    day_root.mkdir(parents=True, exist_ok=True)
    json_path = day_root / "paper_trade_outcome_report.json"
    summary_path = day_root / "paper_trade_outcome_summary.md"
    latest_json = REPORT_ROOT / "latest.json"
    latest_summary = REPORT_ROOT / "latest_summary.md"
    json_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary_text = _summary(report)
    json_path.write_text(json_text, encoding="utf-8")
    summary_path.write_text(summary_text, encoding="utf-8")
    latest_json.write_text(json_text, encoding="utf-8")
    latest_summary.write_text(summary_text, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def _summary(report: dict[str, Any]) -> str:
    counts = report["candidate_survival_counts"]
    lines = [
        "# Atlas V2 Research OS Paper Trade Outcomes",
        "",
        f"- outcome_count: {report['outcome_count']}",
        f"- survived_candidates: {counts.get('SURVIVED_INITIAL_TEST', 0)}",
        f"- weakened_candidates: {counts.get('WEAKENED', 0)}",
        f"- falsified_candidates: {counts.get('FALSIFIED', 0)}",
        f"- needs_more_data_candidates: {counts.get('NEEDS_MORE_DATA', 0)}",
        f"- feedback_signals: {len(report.get('feedback_signals', []))}",
        f"- memory_updates: {len(report.get('memory_updates', []))}",
        f"- research_effectiveness_updates: {len(report.get('research_effectiveness_updates', []))}",
        "- authority: paper-only research feedback; no live trading, capital, production promotion, position sizing, or portfolio construction",
        "",
    ]
    return "\n".join(lines)
