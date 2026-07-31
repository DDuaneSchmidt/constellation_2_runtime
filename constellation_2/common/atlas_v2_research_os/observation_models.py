from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

OBSERVATION_IMPORT_LIMITATION = (
    "Observation import may create raw observation records, observation clusters, claim seeds, and "
    "CLAIM_INVESTIGATION backlog items only; it does not authorize live trading, broker execution, "
    "capital allocation, position sizing, portfolio construction, trade recommendations, automatic "
    "paper trade placement, candidate promotion, or production promotion."
)
OBSERVATION_WORKLOAD_PROFILES = {
    "OBSERVATION_IMPORT_100": 100,
    "OBSERVATION_IMPORT_1000": 1000,
    "OBSERVATION_IMPORT_5000": 5000,
    "EXPANDED_OBSERVATION_TRIAL_5000": 5000,
    "EXPANDED_OBSERVATION_TRIAL_FOCUSED": 6400,
    "SMALL_IMPORT": 100,
    "BATCH_IMPORT": 1000,
    "DEEP_IMPORT": 10000,
}
DEFAULT_OBSERVATION_WORKLOAD_PROFILE = "OBSERVATION_IMPORT_1000"
REQUIRED_OBSERVATION_FIELDS = {
    "timestamp",
    "symbol",
    "timeframe",
    "mechanism",
    "observation",
    "source",
}
OBSERVATION_ALLOWED_OUTPUTS = {
    "ObservationRecord",
    "ObservationCluster",
    "CLAIM_INVESTIGATION",
}
MARKET_STRUCTURES = [
    "INSIDE_DAY",
    "OUTSIDE_DAY",
    "COMPRESSION",
    "RANGE_EXPANSION",
    "GAP_UP",
    "GAP_DOWN",
    "NEW_HIGH",
    "NEW_LOW",
    "FAILED_BREAKOUT",
    "FAILED_BREAKDOWN",
]
DEFAULT_MARKET_STRUCTURE = "UNKNOWN"


@dataclass(frozen=True)
class ObservationRecord:
    observation_id: str
    timestamp: str
    symbol: str
    timeframe: str
    mechanism: str
    observation_text: str
    market_structure: str
    regime: str
    source: str
    confidence: float
    metadata: dict[str, Any]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        validate_observation_record_shape(row)
        return row


@dataclass(frozen=True)
class ObservationImportBatch:
    batch_id: str
    created_at: str
    source_path: str
    input_format: str
    workload_profile: str
    max_observations: int
    records: list[dict[str, Any]] = field(default_factory=list)
    invalid_records: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if row["workload_profile"] not in OBSERVATION_WORKLOAD_PROFILES:
            raise ValueError(f"invalid observation workload profile: {row['workload_profile']}")
        if int(row["max_observations"]) <= 0:
            raise ValueError("max_observations must be positive")
        return row


@dataclass(frozen=True)
class ObservationCluster:
    cluster_id: str
    mechanism: str
    market_structure: str
    regime: str
    symbols: list[str]
    timeframes: list[str]
    source_observation_ids: list[str]
    cluster_summary: str
    confidence: float
    observation_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if not row["source_observation_ids"]:
            raise ValueError("observation cluster requires source_observation_ids")
        if not 0.0 <= float(row["confidence"]) <= 1.0:
            raise ValueError("observation cluster confidence must be between 0 and 1")
        return row


@dataclass(frozen=True)
class ObservationClaimSeed:
    claim_seed_id: str
    source_observation_ids: list[str]
    mechanism: str
    market_structure: str
    regime: str
    symbols: list[str]
    timeframes: list[str]
    cluster_summary: str
    claim_text: str
    confidence: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if not row["source_observation_ids"]:
            raise ValueError("observation claim seed requires source_observation_ids")
        if not row["claim_text"]:
            raise ValueError("observation claim seed requires claim_text")
        if not 0.0 <= float(row["confidence"]) <= 1.0:
            raise ValueError("observation claim seed confidence must be between 0 and 1")
        return row


@dataclass(frozen=True)
class ObservationImportReport:
    report_id: str
    created_at: str
    raw_observations: int
    valid_observations: int
    invalid_observations: int
    duplicates_skipped: int
    clusters_created: int
    claims_created: int
    backlog_items_created: int
    batch: dict[str, Any]
    clusters: list[dict[str, Any]]
    claim_seeds: list[dict[str, Any]]
    backlog_items: list[dict[str, Any]]
    authority_boundary: dict[str, Any] = field(default_factory=lambda: {
        "observation_records_allowed": True,
        "observation_clusters_allowed": True,
        "claim_investigation_backlog_allowed": True,
        "trading_authorized": False,
        "broker_execution_authorized": False,
        "capital_authorized": False,
        "position_sizing_authorized": False,
        "portfolio_construction_authorized": False,
        "trade_recommendation_authorized": False,
        "automatic_paper_trade_placement_authorized": False,
        "candidate_promotion_authorized": False,
        "production_promotion_authorized": False,
    })
    limitations: list[str] = field(default_factory=lambda: [OBSERVATION_IMPORT_LIMITATION])
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_observation_record_shape(row: dict[str, Any]) -> bool:
    missing = [field for field in ["observation_id", "timestamp", "symbol", "timeframe", "mechanism", "observation_text", "market_structure", "regime", "source", "confidence", "metadata", "created_at"] if field not in row]
    if missing:
        raise ValueError(f"missing observation record fields: {missing}")
    if not str(row["observation_text"]).strip():
        raise ValueError("observation_text cannot be empty")
    if not 0.0 <= float(row["confidence"]) <= 1.0:
        raise ValueError("observation confidence must be between 0 and 1")
    return True
