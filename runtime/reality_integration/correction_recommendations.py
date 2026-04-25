from __future__ import annotations

from .schemas import content_hash
from .types import CorrectionRecommendation, DiscrepancyClassification


def build_correction_recommendations(
    reconciliation_id: str,
    classifications: tuple[DiscrepancyClassification, ...],
) -> tuple[CorrectionRecommendation, ...]:
    recommendations: list[CorrectionRecommendation] = []
    for classification in classifications:
        if classification.mismatch_family == "executions" and classification.severity == "critical":
            action_types = (
                ("freeze_governed_execution", "p0", True, "critical execution discrepancy changes live position truth"),
                ("import_missing_execution", "p1", True, "execution truth requires explicit import or broker review"),
            )
        elif classification.mismatch_family == "taxlots" and classification.severity in {"material", "critical"}:
            action_types = (
                ("refresh_tax_lots", "p1", True, "material tax-lot discrepancy affects gain/loss truth"),
                ("broker_review_required", "p1", True, "tax-lot discrepancy requires broker-side review"),
            )
        elif classification.mismatch_family == "positions" and classification.severity in {"material", "critical"}:
            action_types = (
                ("rebuild_internal_state", "p1", True, "position truth mismatch requires explicit state rebuild review"),
            )
        elif classification.mismatch_family == "valuations" and classification.severity in {"material", "critical"}:
            action_types = (
                ("valuation_refresh", "p2", False, "valuation discrepancy requires refreshed market marks"),
            )
        elif classification.mismatch_family == "pnl" and classification.severity in {"material", "critical"}:
            action_types = (
                ("broker_review_required", "p1", True, "PnL discrepancy requires trade and fee review"),
            )
        else:
            action_types = (
                ("no_action", "p3", False, "discrepancy remains visible but does not require automated correction"),
            )
        for action_type, priority, requires_human_review, rationale in action_types:
            recommendation_id = f"recommendation-{content_hash({'reconciliation_id': reconciliation_id, 'family': classification.mismatch_family, 'action_type': action_type})[:12]}"
            recommendations.append(
                CorrectionRecommendation(
                    reconciliation_id=reconciliation_id,
                    recommendation_id=recommendation_id,
                    target_surface=classification.mismatch_family,
                    action_type=action_type,
                    action_priority=priority,
                    requires_human_review=requires_human_review,
                    rationale=rationale,
                )
            )
    if not recommendations:
        recommendation_id = f"recommendation-{content_hash({'reconciliation_id': reconciliation_id, 'action_type': 'no_action'})[:12]}"
        recommendations.append(
            CorrectionRecommendation(
                reconciliation_id=reconciliation_id,
                recommendation_id=recommendation_id,
                target_surface="global",
                action_type="no_action",
                action_priority="p3",
                requires_human_review=False,
                rationale="reconciliation completed without discrepancies",
            )
        )
    unique: dict[str, CorrectionRecommendation] = {}
    for record in recommendations:
        unique[record.recommendation_id] = record
    return tuple(unique[key] for key in sorted(unique))
