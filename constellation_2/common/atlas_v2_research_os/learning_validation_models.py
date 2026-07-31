from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

TIME_HORIZONS = {"7_day", "30_day", "90_day", "lifetime"}
TREND_DIRECTIONS = {"IMPROVING", "STABLE", "REGRESSING", "INSUFFICIENT_DATA"}
VALIDATION_OUTPUT_TYPES = {"LearningTrend", "LearningRegression", "LearningImprovement", "LearningPlateau"}
CONTINUOUS_LEARNING_VALIDATION_CERTIFICATION = "CONTINUOUS_LEARNING_VALIDATION_CERTIFICATION"
CERTIFICATION_RESULTS = {"IMPROVING", "STABLE", "REGRESSING", "INSUFFICIENT_DATA"}


@dataclass(frozen=True)
class LearningValidationSnapshot:
    snapshot_id: str
    observed_at: str
    repeated_failures: float
    duplicate_ideas: float
    regime_gaps: float
    evidence_maturity_score: float
    hypothesis_survival_rate: float
    candidate_quality_score: float
    source_artifact_ids: list[str] = field(default_factory=list)
    source_memory_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LearningTrend:
    horizon: str
    trend_direction: str
    metrics: dict[str, float | None]
    confidence: float
    limitations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_learning_trend(row)
        return row


@dataclass(frozen=True)
class LearningRegression:
    metric_name: str
    value: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LearningImprovement:
    metric_name: str
    value: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LearningPlateau:
    metric_name: str
    value: float | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_learning_trend(row: dict[str, Any]) -> bool:
    if row.get("horizon") not in TIME_HORIZONS:
        raise ValueError(f"invalid learning validation horizon: {row.get('horizon')}")
    if row.get("trend_direction") not in TREND_DIRECTIONS:
        raise ValueError(f"invalid learning trend direction: {row.get('trend_direction')}")
    return True
