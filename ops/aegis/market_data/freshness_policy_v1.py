from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PENDING_VENDOR_DATA = "PENDING_VENDOR_DATA"
PARTIAL_DATA_AVAILABLE = "PARTIAL_DATA_AVAILABLE"
VALIDATED_CURRENT_DAY = "VALIDATED_CURRENT_DAY"
READ_ONLY_PRIOR_DAY_FALLBACK = "READ_ONLY_PRIOR_DAY_FALLBACK"
PROVISIONAL_INTRADAY = "PROVISIONAL_INTRADAY"
STALE = "STALE"
CERTIFICATION_PENDING = "CERTIFICATION_PENDING"
CERTIFIED = "CERTIFIED"
INVALID = "INVALID"

FRESHNESS_STATES = {
    PENDING_VENDOR_DATA,
    PARTIAL_DATA_AVAILABLE,
    VALIDATED_CURRENT_DAY,
    READ_ONLY_PRIOR_DAY_FALLBACK,
    PROVISIONAL_INTRADAY,
    STALE,
    CERTIFICATION_PENDING,
    CERTIFIED,
    INVALID,
}

NY_TZ = ZoneInfo("America/New_York")
DEFAULT_VENDOR_LAG_MINUTES = 120

BOND_RATE_SYMBOLS = {
    "AGG",
    "BIL",
    "BND",
    "HYG",
    "IEF",
    "LQD",
    "MBB",
    "SHY",
    "TLT",
    "TIP",
    "US02Y",
    "US10Y",
    "US30Y",
}

# NYSE/Nasdaq planned 13:00 ET equity early closes for the current governed
# operating horizon. Full open/closed truth still comes from market_calendar_v1.
EQUITY_EARLY_CLOSES = {
    "2026-07-02",
    "2026-11-27",
    "2026-12-24",
}

# SIFMA recommended 14:00 ET fixed-income early closes relevant to the same
# horizon. This is only used when bond/rate symbols are part of the refresh.
SIFMA_EARLY_CLOSES = {
    "2026-04-02",
    "2026-05-22",
    "2026-07-02",
    "2026-11-27",
    "2026-12-24",
    "2026-12-31",
}


@dataclass(frozen=True)
class MarketFreshnessDecisionV1:
    freshness_state: str
    validation_status: str
    certification_state: str
    usable_for_candidate_generation: bool
    usable_for_candidate_visibility: bool
    usable_for_execution_candidate_generation: bool
    market_calendar: dict[str, Any]
    expected_current_data_after_utc: str
    next_retry_utc: str
    last_attempt: dict[str, Any]
    message: str
    reason: str


def vendor_lag_minutes_from_env_v1() -> int:
    raw = str(os.environ.get("AEGIS_MARKET_DATA_VENDOR_LAG_MINUTES") or "").strip()
    if not raw:
        return DEFAULT_VENDOR_LAG_MINUTES
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_VENDOR_LAG_MINUTES


def parse_utc_dt_v1(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).replace(microsecond=0)
    except ValueError:
        return None


def _parse_day(day_utc: str) -> date:
    return datetime.strptime(str(day_utc), "%Y-%m-%d").date()


def _calendar_record_from_truth(truth_root: Path, day_utc: str, exchange: str) -> tuple[bool | None, str, str]:
    path = Path(truth_root).expanduser().resolve() / "market_calendar_v1" / exchange / f"{day_utc[:4]}.jsonl"
    if not path.exists():
        return None, "CALENDAR_FILE_MISSING", str(path)
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if str(row.get("day_utc") or "") == day_utc:
                return bool(row.get("is_trading_session") is True), "CALENDAR_EXPLICIT", str(path)
    except Exception:
        return None, "CALENDAR_READ_FAILED", str(path)
    return None, "CALENDAR_DAY_MISSING", str(path)


def _us_equity_holiday_fallback(day: date) -> bool:
    # Conservative fallback for common US market holidays when the governed
    # calendar artifact is absent. The artifact remains authoritative when present.
    fixed = {(1, 1), (6, 19), (7, 4), (12, 25)}
    if (day.month, day.day) in fixed:
        return True
    # Memorial Day: last Monday in May.
    if day.month == 5 and day.weekday() == 0 and day.day >= 25:
        return True
    # Labor Day: first Monday in September.
    if day.month == 9 and day.weekday() == 0 and day.day <= 7:
        return True
    # Thanksgiving: fourth Thursday in November.
    if day.month == 11 and day.weekday() == 3 and 22 <= day.day <= 28:
        return True
    return False


def resolve_market_session_v1(*, truth_root: Path, day_utc: str, required_symbols: list[str] | tuple[str, ...] | None = None, vendor_lag_minutes: int | None = None) -> dict[str, Any]:
    day = _parse_day(day_utc)
    symbols = {str(symbol or "").upper() for symbol in (required_symbols or []) if str(symbol or "").strip()}
    nyse_open, source_reason, source_path = _calendar_record_from_truth(Path(truth_root), day_utc, "NYSE")
    nasdaq_open, nasdaq_reason, nasdaq_path = _calendar_record_from_truth(Path(truth_root), day_utc, "NASDAQ")
    if nyse_open is None:
        nyse_open = day.weekday() < 5 and not _us_equity_holiday_fallback(day)
    if nasdaq_open is None:
        nasdaq_open = nyse_open
    equity_trading_session = bool(nyse_open and nasdaq_open)
    equity_close = time(13, 0) if day_utc in EQUITY_EARLY_CLOSES else time(16, 0)
    bond_rate_required = bool(symbols & BOND_RATE_SYMBOLS)
    sifma_close = time(14, 0) if day_utc in SIFMA_EARLY_CLOSES else time(17, 0)
    close_candidates = [datetime.combine(day, equity_close, tzinfo=NY_TZ)]
    if bond_rate_required:
        close_candidates.append(datetime.combine(day, sifma_close, tzinfo=NY_TZ))
    official_close_local = max(close_candidates)
    lag_minutes = vendor_lag_minutes if vendor_lag_minutes is not None else vendor_lag_minutes_from_env_v1()
    expected_after = official_close_local + timedelta(minutes=max(0, int(lag_minutes)))
    next_morning = datetime.combine(day + timedelta(days=1), time(8, 15), tzinfo=NY_TZ)
    return {
        "timezone": "America/New_York",
        "day_utc": day_utc,
        "equity_trading_session": equity_trading_session,
        "nyse_trading_session": bool(nyse_open),
        "nasdaq_trading_session": bool(nasdaq_open),
        "calendar_source_reason": source_reason,
        "calendar_source_path": source_path,
        "nasdaq_calendar_source_reason": nasdaq_reason,
        "nasdaq_calendar_source_path": nasdaq_path,
        "equity_close_local": datetime.combine(day, equity_close, tzinfo=NY_TZ).replace(microsecond=0).isoformat(),
        "equity_early_close": day_utc in EQUITY_EARLY_CLOSES,
        "bond_rate_data_required": bond_rate_required,
        "sifma_close_local": datetime.combine(day, sifma_close, tzinfo=NY_TZ).replace(microsecond=0).isoformat() if bond_rate_required else "",
        "sifma_early_close": bool(bond_rate_required and day_utc in SIFMA_EARLY_CLOSES),
        "official_close_local": official_close_local.replace(microsecond=0).isoformat(),
        "vendor_lag_minutes": int(lag_minutes),
        "expected_current_data_after_utc": expected_after.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "next_morning_repair_utc": next_morning.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }


def classify_market_data_freshness_v1(
    *,
    truth_root: Path,
    day_utc: str,
    required_symbols: list[str] | tuple[str, ...] | None,
    fetched_symbols: list[str] | tuple[str, ...] | None,
    missing_symbols: list[str] | tuple[str, ...] | None,
    stale_symbols: list[str] | tuple[str, ...] | None,
    current_session_symbols: list[str] | tuple[str, ...] | None = None,
    as_of_utc: str | datetime | None = None,
    provider_status: str = "",
    provider_error: str = "",
    vendor_lag_minutes: int | None = None,
) -> MarketFreshnessDecisionV1:
    required = {str(symbol or "").upper() for symbol in (required_symbols or []) if str(symbol or "").strip()}
    fetched = {str(symbol or "").upper() for symbol in (fetched_symbols or []) if str(symbol or "").strip()}
    missing = {str(symbol or "").upper() for symbol in (missing_symbols or []) if str(symbol or "").strip()}
    stale = {str(symbol or "").upper() for symbol in (stale_symbols or []) if str(symbol or "").strip()}
    current = {str(symbol or "").upper() for symbol in (current_session_symbols or []) if str(symbol or "").strip()}
    if not current:
        current = fetched - stale
    now = as_of_utc if isinstance(as_of_utc, datetime) else parse_utc_dt_v1(str(as_of_utc or ""))
    now = (now or datetime.now(UTC)).astimezone(UTC).replace(microsecond=0)
    session = resolve_market_session_v1(truth_root=truth_root, day_utc=day_utc, required_symbols=sorted(required), vendor_lag_minutes=vendor_lag_minutes)
    expected_after = parse_utc_dt_v1(str(session.get("expected_current_data_after_utc") or "")) or now
    next_retry_dt = min(expected_after, now + timedelta(minutes=15)) if now < expected_after else now + timedelta(minutes=30)
    next_retry = next_retry_dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    status = str(provider_status or "").upper()
    if not session.get("equity_trading_session"):
        state = READ_ONLY_PRIOR_DAY_FALLBACK
        certification_state = STALE
        reason = "MARKET_HOLIDAY_OR_NON_TRADING_SESSION"
        message = "No current-day equity session is scheduled. Prior-day fallback is read-only."
    elif required and required <= current and not (missing & required) and not (stale & required):
        state = VALIDATED_CURRENT_DAY
        certification_state = CERTIFICATION_PENDING
        reason = "ALL_REQUIRED_SYMBOLS_CURRENT"
        message = "Current-day market data is available for read-only candidates; final EOD certification is pending."
    elif now < expected_after:
        state = PARTIAL_DATA_AVAILABLE if current else PENDING_VENDOR_DATA
        certification_state = PARTIAL_DATA_AVAILABLE if current else CERTIFICATION_PENDING
        reason = "BEFORE_MARKET_CLOSE_VENDOR_LAG_ELAPSED"
        message = "Current-day vendor data is still inside the market-close/vendor-lag window."
    elif current:
        state = PARTIAL_DATA_AVAILABLE
        certification_state = PARTIAL_DATA_AVAILABLE
        reason = "PARTIAL_CURRENT_DAY_VENDOR_RESPONSE"
        message = "Partial current-day market data is available for read-only provisional candidates; execution/ranking finalization is held until certification."
    else:
        state = READ_ONLY_PRIOR_DAY_FALLBACK
        certification_state = STALE
        reason = "CURRENT_DAY_VENDOR_DATA_NOT_VALIDATED"
        if status in {"FAILED", "STALE"} or stale or missing:
            reason = "STALE_OR_MISSING_CURRENT_DAY_VENDOR_DATA"
        message = "Current-day data is not validated. Prior-day fallback is read-only."
    visibility_states = {VALIDATED_CURRENT_DAY, PARTIAL_DATA_AVAILABLE}
    return MarketFreshnessDecisionV1(
        freshness_state=state,
        validation_status=state,
        certification_state=certification_state,
        usable_for_candidate_generation=state in visibility_states,
        usable_for_candidate_visibility=state in visibility_states,
        usable_for_execution_candidate_generation=certification_state == CERTIFIED,
        market_calendar=session,
        expected_current_data_after_utc=str(session.get("expected_current_data_after_utc") or ""),
        next_retry_utc=next_retry,
        last_attempt={
            "attempted_at_utc": now.isoformat().replace("+00:00", "Z"),
            "provider_status": status,
            "error": str(provider_error or ""),
        },
        message=message,
        reason=reason,
    )


def decision_to_dict_v1(decision: MarketFreshnessDecisionV1) -> dict[str, Any]:
    return {
        "freshness_state": decision.freshness_state,
        "validation_status": decision.validation_status,
        "certification_state": decision.certification_state,
        "usable_for_candidate_generation": decision.usable_for_candidate_generation,
        "usable_for_candidate_visibility": decision.usable_for_candidate_visibility,
        "usable_for_execution_candidate_generation": decision.usable_for_execution_candidate_generation,
        "market_calendar": decision.market_calendar,
        "expected_current_data_after_utc": decision.expected_current_data_after_utc,
        "next_retry_utc": decision.next_retry_utc,
        "last_attempt": decision.last_attempt,
        "message": decision.message,
        "reason": decision.reason,
    }
