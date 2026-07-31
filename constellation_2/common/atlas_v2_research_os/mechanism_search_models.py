from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class MechanismFamily(str, Enum):
    MEAN_REVERSION = "MEAN_REVERSION"
    BREAKOUT = "BREAKOUT"
    OPENING_RANGE = "OPENING_RANGE"
    VWAP_OR_AVERAGE_RECLAIM = "VWAP_OR_AVERAGE_RECLAIM"
    SESSION_TIMING = "SESSION_TIMING"
    VOLATILITY_EXPANSION = "VOLATILITY_EXPANSION"
    LIQUIDITY_SWEEP = "LIQUIDITY_SWEEP"
    TREND_CONTINUATION = "TREND_CONTINUATION"
    REVERSAL = "REVERSAL"
    EVENT_REACTION = "EVENT_REACTION"


MECHANISM_FAMILIES = [item.value for item in MechanismFamily]


@dataclass(frozen=True)
class SourceSearchConfig:
    search_family: str
    evidence_scope: str
    required_observations: list[str]
    excluded_authority: list[str] = field(
        default_factory=lambda: [
            "BROKER_SUBMIT",
            "CAPITAL_AUTHORITY",
            "AUTOMATIC_PAPER_PLACEMENT",
        ]
    )
    max_source_count: int = 12
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MechanismHypothesis:
    hypothesis_id: str
    mechanism: str
    conditions: list[str]
    regime: str
    timeframe: str
    entry_observation_rule: str
    exit_observation_rule: str
    invalidation_rule: str
    complexity_score: float
    source_search_config: dict[str, Any]
    artifact_type: str = "ResearchHypothesis"
    evidence_level: str = "GENERATED_ONLY"
    lifecycle_state: str = "NEW"
    source_artifact_ids: list[str] = field(default_factory=list)
    created_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.mechanism not in MECHANISM_FAMILIES:
            raise ValueError(f"invalid mechanism family: {self.mechanism}")
        if not self.conditions:
            raise ValueError("mechanism hypothesis requires conditions")
        if not self.regime:
            raise ValueError("mechanism hypothesis requires regime")
        if not self.timeframe:
            raise ValueError("mechanism hypothesis requires timeframe")
        for field_name in ["entry_observation_rule", "exit_observation_rule", "invalidation_rule"]:
            if not getattr(self, field_name):
                raise ValueError(f"mechanism hypothesis requires {field_name}")
        if not 0.0 <= float(self.complexity_score) <= 1.0:
            raise ValueError("complexity_score must be between 0 and 1")
        return asdict(self)


@dataclass(frozen=True)
class MechanismSearchRun:
    run_id: str
    created_at: str
    requested_limit: int
    emitted_count: int
    hypotheses: list[dict[str, Any]]
    governance_result: dict[str, Any]
    pipeline_results: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
