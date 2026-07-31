from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


MODE_DRY_RUN_ONLY = "DRY_RUN_ONLY"
MODE_EXECUTE = "EXECUTE"
STATUS_PLANNED = "PLANNED_NOT_FETCHED"
STATUS_DOWNLOADED = "DOWNLOADED"
STATUS_BLOCKED = "BLOCKED"
STATUS_FAILED = "FAILED"

ALLOWLISTED_PROVIDERS = {"tiingo", "alpha_vantage"}
PRIORITY_1_REQUESTS = {("DIA", "30m"), ("DIA", "5m"), ("QQQ", "30m"), ("QQQ", "5m"), ("SPY", "30m"), ("SPY", "5m")}

AUTHORITY_BOUNDARY = {
    "historical_data_only": True,
    "dry_run_default": True,
    "external_api_called": False,
    "broker_endpoint_allowed": False,
    "real_time_trading_data_allowed": False,
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
class IntradayProviderStatus:
    provider: str
    allowlisted: bool
    credentials_present: bool
    supports_historical_intraday: bool
    safe_to_execute_if_explicit: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IntradayDownloadPlanItem:
    symbol: str
    timeframe: str
    start_date: str
    end_date: str
    required_for_candidates: list[str]
    target_csv_path: str
    provider: str
    priority: int
    status: str = STATUS_PLANNED
    blocker: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IntradayDownloadResult:
    symbol: str
    timeframe: str
    provider: str
    target_csv_path: str
    status: str
    rows_written: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
