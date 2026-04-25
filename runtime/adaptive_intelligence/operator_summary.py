from __future__ import annotations

from .schemas import content_hash
from .types import OperatorSummary


def build_summary(
    *,
    as_of: str,
    ranked_issue_refs: tuple[str, ...],
    drift_signal_refs: tuple[str, ...],
    regime_signal_refs: tuple[str, ...],
    recommendation_refs: tuple[str, ...],
    proposal_candidate_refs: tuple[str, ...],
) -> OperatorSummary:
    top_ranked_issues = tuple(ranked_issue_refs[:5])
    major_drift_signals = tuple(drift_signal_refs[:5])
    major_regime_signals = tuple(regime_signal_refs[:5])
    recommended_actions = tuple(recommendation_refs[:5])
    candidate_proposals = tuple(proposal_candidate_refs[:5])
    payload = {
        "as_of": as_of,
        "top_ranked_issues": top_ranked_issues,
        "major_drift_signals": major_drift_signals,
        "major_regime_signals": major_regime_signals,
        "recommended_actions": recommended_actions,
        "candidate_proposals": candidate_proposals,
    }
    summary_hash = content_hash(payload)
    return OperatorSummary(
        summary_id=f"operator-summary-{summary_hash[:12]}",
        as_of=as_of,
        top_ranked_issues=top_ranked_issues,
        major_drift_signals=major_drift_signals,
        major_regime_signals=major_regime_signals,
        recommended_actions=recommended_actions,
        candidate_proposals=candidate_proposals,
        summary_hash=summary_hash,
    )
