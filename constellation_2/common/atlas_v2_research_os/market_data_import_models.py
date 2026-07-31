from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


AUTHORITY_BOUNDARY = {
    "local_market_data_import_only": True,
    "schema_validation_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}


@dataclass(frozen=True)
class LocalMarketDataFile:
    path: str
    symbol: str
    timeframe: str
    filename: str
    source_root: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MarketDataSchemaValidationResult:
    path: str
    symbol: str
    timeframe: str
    status: str
    row_count: int = 0
    date_start: str = ""
    date_end: str = ""
    normalized_columns: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateMarketDataCoverage:
    candidate_id: str
    required_symbols: list[str]
    required_timeframes: list[str]
    available_symbols: list[str]
    missing_symbols: list[str]
    available_timeframes: list[str]
    missing_timeframes: list[str]
    date_start: str
    date_end: str
    row_count: int
    coverage_status: str
    local_files: list[str]
    blockers: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
