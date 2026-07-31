from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

FAMILY_LEARNING_STATUS_VALUES = {
    "STRENGTHENED",
    "UNCHANGED",
    "WEAKENED",
    "NEEDS_MORE_OBSERVATIONS",
    "NEEDS_DIRECT_DATA",
    "RETIRED",
}

FAMILY_LEARNING_REQUIRED_FIELDS = [
    "family_id",
    "family_name",
    "prior_classification",
    "observations_logged",
    "supporting_observations",
    "invalidating_observations",
    "neutral_observations",
    "sample_size",
    "confidence_before",
    "confidence_after",
    "confidence_delta",
    "status_after_update",
    "reason_for_update",
    "next_required_evidence",
]

ALLOWED_FAMILY_LEARNING_ACTIONS = {
    "family confidence update",
    "research memory update proposal",
    "observation recommendation",
    "retirement recommendation",
}

FORBIDDEN_FAMILY_LEARNING_ACTIONS = {
    "trade recommendation",
    "capital allocation",
    "position sizing",
    "broker execution",
    "automatic paper placement",
    "candidate production promotion",
}


@dataclass(frozen=True)
class FamilyLearningUpdate:
    family_id: str
    family_name: str
    prior_classification: str
    observations_logged: int
    supporting_observations: int
    invalidating_observations: int
    neutral_observations: int
    sample_size: int
    confidence_before: float
    confidence_after: float
    confidence_delta: float
    status_after_update: str
    reason_for_update: str
    next_required_evidence: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_family_learning_update(row)
        return row


def validate_family_learning_update(row: dict[str, Any]) -> bool:
    missing = [field_name for field_name in FAMILY_LEARNING_REQUIRED_FIELDS if field_name not in row]
    if missing:
        raise ValueError(f"missing family learning fields: {missing}")
    if row["status_after_update"] not in FAMILY_LEARNING_STATUS_VALUES:
        raise ValueError(f"invalid family learning status: {row['status_after_update']}")
    for field_name in (
        "observations_logged",
        "supporting_observations",
        "invalidating_observations",
        "neutral_observations",
        "sample_size",
    ):
        if int(row[field_name]) < 0:
            raise ValueError(f"family learning count cannot be negative: {field_name}")
    for field_name in ("confidence_before", "confidence_after"):
        value = float(row[field_name])
        if value < 0.0 or value > 1.0:
            raise ValueError(f"family learning confidence must be between 0 and 1: {field_name}")
    return True
