from __future__ import annotations

from typing import Any

from .paper_trade_outcome_models import FORBIDDEN_PAPER_OUTCOME_AUTHORITY_FLAGS


class PaperTradeOutcomeGovernanceError(ValueError):
    pass


def validate_paper_trade_outcome_allowed(payload: dict[str, Any]) -> bool:
    metadata = payload.get("metadata", {}) or {}
    for flag in sorted(FORBIDDEN_PAPER_OUTCOME_AUTHORITY_FLAGS):
        if payload.get(flag) is True or metadata.get(flag) is True:
            raise PaperTradeOutcomeGovernanceError(f"paper outcome cannot set authority flag: {flag}")
    target = payload.get("influence_target") or metadata.get("influence_target")
    if target and target not in {"RESEARCH_MEMORY", "CANDIDATE_QUALITY_MEASUREMENT", "RESEARCH_EFFECTIVENESS", "RESEARCH_PRIORITIZATION", "LEARNING_VALIDATION"}:
        raise PaperTradeOutcomeGovernanceError(f"paper outcome cannot influence target: {target}")
    recommendation = payload.get("recommendation") or metadata.get("recommendation")
    if recommendation in {"Trade this.", "Allocate capital.", "Promote candidate.", "Size position.", "Construct portfolio."}:
        raise PaperTradeOutcomeGovernanceError(f"forbidden paper outcome recommendation: {recommendation}")
    return True


def validate_paper_trade_outcome_report(report: dict[str, Any]) -> bool:
    validate_paper_trade_outcome_allowed({"metadata": report.get("authority_boundary", {})})
    for row in report.get("outcomes", []):
        validate_paper_trade_outcome_allowed(row)
    for row in report.get("feedback_signals", []):
        validate_paper_trade_outcome_allowed(row)
    return True
