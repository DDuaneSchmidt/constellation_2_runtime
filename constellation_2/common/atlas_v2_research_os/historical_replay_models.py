from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

HISTORICAL_REPLAY_EVIDENCE_LEVEL = "HISTORICAL_REPLAY"
FORBIDDEN_REPLAY_EVIDENCE_LEVELS = {
    "EXTERNALLY_VALIDATED",
    "OPERATOR_APPROVED",
    "LIVE_VALIDATED",
}
HISTORICAL_REPLAY_INPUT_TYPES = {
    "ResearchHypothesis",
    "CheapExperimentSpec",
    "PaperTradeCandidate",
    "MechanismVariation",
    "FailureAnalysis",
}
HISTORICAL_REPLAY_WINDOWS = {
    "30d": 30,
    "90d": 90,
    "180d": 180,
    "1y": 365,
    "3y": 1095,
    "5y": 1825,
}
HISTORICAL_REPLAY_CERTIFICATION_STATUSES = {
    "REPLAY_POSITIVE",
    "REPLAY_NEUTRAL",
    "REPLAY_NEGATIVE",
    "INSUFFICIENT_SAMPLE",
}
HISTORICAL_REPLAY_LIMITATION = (
    "Historical replay generates evidence only; it does not authorize live trading, broker execution, "
    "capital allocation, candidate promotion, portfolio construction, position sizing, trade "
    "recommendations, automatic paper trade placement, or production promotion."
)


@dataclass(frozen=True)
class HistoricalReplayRequest:
    replay_id: str
    hypothesis_id: str
    mechanism_tags: list[str]
    regime_context: dict[str, Any]
    time_window: str
    sample_size: int
    result_count: int
    evidence_level: str
    created_at: str
    source_artifact_ids: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    input_type: str = "ResearchHypothesis"

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_common(row)
        if row["input_type"] not in HISTORICAL_REPLAY_INPUT_TYPES:
            raise ValueError(f"invalid historical replay input_type: {row['input_type']}")
        if row["time_window"] not in HISTORICAL_REPLAY_WINDOWS:
            raise ValueError(f"invalid historical replay time_window: {row['time_window']}")
        return row


@dataclass(frozen=True)
class HistoricalReplayEvidence:
    replay_id: str
    hypothesis_id: str
    mechanism_tags: list[str]
    regime_context: dict[str, Any]
    time_window: str
    sample_size: int
    result_count: int
    evidence_level: str
    created_at: str
    source_artifact_ids: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    limitations: list[str] = field(default_factory=lambda: [HISTORICAL_REPLAY_LIMITATION])

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_common(row)
        return row


@dataclass(frozen=True)
class HistoricalReplayCertification:
    replay_id: str
    hypothesis_id: str
    mechanism_tags: list[str]
    regime_context: dict[str, Any]
    time_window: str
    sample_size: int
    result_count: int
    evidence_level: str
    created_at: str
    source_artifact_ids: list[str]
    status: str
    reasons: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    trading_authorized: bool = False
    capital_authorized: bool = False
    candidate_promotion_authorized: bool = False
    paper_trade_placement_authorized: bool = False
    limitations: list[str] = field(default_factory=lambda: [
        HISTORICAL_REPLAY_LIMITATION,
        "Positive replay is not trading approval.",
        "Negative replay is not automatic rejection.",
    ])

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_common(row)
        if row["status"] not in HISTORICAL_REPLAY_CERTIFICATION_STATUSES:
            raise ValueError(f"invalid historical replay certification status: {row['status']}")
        return row


@dataclass(frozen=True)
class HistoricalReplayResult:
    replay_id: str
    hypothesis_id: str
    mechanism_tags: list[str]
    regime_context: dict[str, Any]
    time_window: str
    sample_size: int
    result_count: int
    evidence_level: str
    created_at: str
    source_artifact_ids: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    certification: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_common(row)
        return row


@dataclass(frozen=True)
class HistoricalReplaySummary:
    replay_id: str
    hypothesis_id: str
    mechanism_tags: list[str]
    regime_context: dict[str, Any]
    time_window: str
    sample_size: int
    result_count: int
    evidence_level: str
    created_at: str
    source_artifact_ids: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "INSUFFICIENT_SAMPLE"
    score: float = 0.0
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        _validate_common(row)
        if row["status"] not in HISTORICAL_REPLAY_CERTIFICATION_STATUSES:
            raise ValueError(f"invalid historical replay summary status: {row['status']}")
        return row


def _validate_common(row: dict[str, Any]) -> None:
    missing = [
        field_name
        for field_name in [
            "replay_id",
            "hypothesis_id",
            "mechanism_tags",
            "regime_context",
            "time_window",
            "sample_size",
            "result_count",
            "evidence_level",
            "created_at",
            "source_artifact_ids",
            "metadata",
        ]
        if field_name not in row
    ]
    if missing:
        raise ValueError(f"missing historical replay fields: {missing}")
    if row["evidence_level"] != HISTORICAL_REPLAY_EVIDENCE_LEVEL:
        raise ValueError("historical replay evidence_level must be HISTORICAL_REPLAY")
    if row["evidence_level"] in FORBIDDEN_REPLAY_EVIDENCE_LEVELS:
        raise ValueError(f"forbidden historical replay evidence_level: {row['evidence_level']}")
    if int(row["sample_size"]) < 0 or int(row["result_count"]) < 0:
        raise ValueError("historical replay counts cannot be negative")
