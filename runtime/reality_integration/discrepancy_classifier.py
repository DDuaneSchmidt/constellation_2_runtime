from __future__ import annotations

from .types import DiscrepancyClassification


SEVERITY_ORDER = {
    "informational": 0,
    "minor": 1,
    "material": 2,
    "critical": 3,
}


def _max_severity(values: tuple[str, ...]) -> str:
    if not values:
        return "informational"
    return max(values, key=lambda value: SEVERITY_ORDER[value])


def classify_discrepancies(reconciliation_id: str, family_mismatches: dict[str, tuple[object, ...]]) -> tuple[DiscrepancyClassification, ...]:
    classifications: list[DiscrepancyClassification] = []
    for family, mismatches in sorted(family_mismatches.items()):
        severities = tuple(getattr(mismatch, "severity") for mismatch in mismatches)
        severity = _max_severity(severities)
        if not mismatches:
            continue
        if family == "executions" and severity == "critical":
            operator_visibility = "execution_freeze_candidate"
            classification = "execution_truth_breach"
            freeze_required = True
            review_required = True
        elif family == "taxlots" and severity in {"material", "critical"}:
            operator_visibility = "broker_truth_review_required"
            classification = "tax_truth_breach"
            freeze_required = False
            review_required = True
        elif family in {"positions", "valuations", "pnl"} and severity in {"material", "critical"}:
            operator_visibility = "review_required"
            classification = f"{family[:-1]}_reconciliation_issue"
            freeze_required = severity == "critical"
            review_required = True
        elif severity == "minor":
            operator_visibility = "visible_only"
            classification = f"{family[:-1]}_minor_drift"
            freeze_required = False
            review_required = False
        else:
            operator_visibility = "visible_only"
            classification = f"{family[:-1]}_informational"
            freeze_required = False
            review_required = False
        classifications.append(
            DiscrepancyClassification(
                reconciliation_id=reconciliation_id,
                mismatch_family=family,
                classification=classification,
                severity=severity,
                operator_visibility=operator_visibility,
                freeze_required=freeze_required,
                review_required=review_required,
                explanation=f"{len(mismatches)} {family} discrepancy(s) classified as {severity}",
            )
        )
    return tuple(classifications)


def overall_severity(classifications: tuple[DiscrepancyClassification, ...]) -> str:
    if not classifications:
        return "informational"
    return max(classifications, key=lambda item: SEVERITY_ORDER[item.severity]).severity
