from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from typing import Any

NY_TZ = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
US_EQUITIES_DOMAINS = {"US_EQUITIES_EOD", "US_EQUITIES_INTRADAY"}
RATES_BONDS_DOMAINS = {"RATES_BONDS"}
# SIFMA recommended early close before Memorial Day 2026. This is fixed-income only.
SIFMA_FIXED_INCOME_EARLY_CLOSES_V1 = {"2026-05-22": "14:00:00"}
US_EQUITIES_EARLY_CLOSES_V1: dict[str, str] = {}


def _parse_day(day_utc: str) -> date:
    return datetime.strptime(str(day_utc), "%Y-%m-%d").date()


# NYSE full-market holidays for 2026. This is intentionally small and explicit
# until Aegis has a governed exchange-calendar artifact for this path.
US_EQUITIES_FULL_MARKET_HOLIDAYS_V1 = {
    "2026-01-01",
    "2026-01-19",
    "2026-02-16",
    "2026-04-03",
    "2026-05-25",
    "2026-06-19",
    "2026-07-03",
    "2026-09-07",
    "2026-11-26",
    "2026-12-25",
}


def is_us_equities_trading_day_v1(day_utc: str) -> bool:
    day = _parse_day(day_utc)
    day_text = day.isoformat()
    return day.weekday() < 5 and day_text not in US_EQUITIES_FULL_MARKET_HOLIDAYS_V1


def latest_us_equities_trading_day_on_or_before_v1(day_utc: str) -> str:
    day = _parse_day(day_utc)
    for offset in range(0, 14):
        candidate = day - timedelta(days=offset)
        if is_us_equities_trading_day_v1(candidate.isoformat()):
            return candidate.isoformat()
    raise ValueError(f"NO_US_EQUITIES_TRADING_DAY_FOUND_ON_OR_BEFORE:{day_utc}")

def session_close_time_v1(*, domain_id: str, day_utc: str) -> dt_time:
    domain = str(domain_id or "").strip().upper()
    day = str(day_utc or "")[:10]
    if domain in RATES_BONDS_DOMAINS and day in SIFMA_FIXED_INCOME_EARLY_CLOSES_V1:
        return dt_time.fromisoformat(SIFMA_FIXED_INCOME_EARLY_CLOSES_V1[day])
    if domain in US_EQUITIES_DOMAINS and day in US_EQUITIES_EARLY_CLOSES_V1:
        return dt_time.fromisoformat(US_EQUITIES_EARLY_CLOSES_V1[day])
    return dt_time(16, 0)


def session_classification_v1(*, domain_id: str, day_utc: str) -> dict[str, Any]:
    close_time = session_close_time_v1(domain_id=domain_id, day_utc=day_utc)
    local_close = datetime.combine(_parse_day(day_utc), close_time, tzinfo=NY_TZ).replace(microsecond=0)
    is_early_close = close_time != dt_time(16, 0)
    return {
        "schema_id": "aegis_market_session_classification",
        "schema_version": "v1",
        "domain_id": str(domain_id or "").strip().upper(),
        "day_utc": str(day_utc)[:10],
        "calendar": "NYSE/NASDAQ" if str(domain_id or "").strip().upper() in US_EQUITIES_DOMAINS else ("SIFMA_FIXED_INCOME" if str(domain_id or "").strip().upper() in RATES_BONDS_DOMAINS else "DOMAIN_DEFAULT"),
        "session_type": "EARLY_CLOSE" if is_early_close else "REGULAR",
        "close_time_local": close_time.isoformat(),
        "close_at_local": local_close.isoformat(),
        "close_at_utc": local_close.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "sifma_fixed_income_early_close_applied": bool(str(domain_id or "").strip().upper() in RATES_BONDS_DOMAINS and is_early_close),
        "equity_early_close_applied": bool(str(domain_id or "").strip().upper() in US_EQUITIES_DOMAINS and is_early_close),
    }


def finalization_window_for_domain_v1(*, domain_id: str, day_utc: str) -> dict[str, str]:
    close = datetime.fromisoformat(session_classification_v1(domain_id=domain_id, day_utc=day_utc)["close_at_local"])
    start = close.replace(minute=15 if close.minute == 0 else close.minute, second=0)
    if close.minute == 0:
        start = close.replace(hour=close.hour, minute=15)
    end = close.replace(hour=close.hour + 2 if close.hour <= 21 else close.hour, minute=0, second=0)
    if end <= start:
        end = start.replace(hour=start.hour + 1)
    return {
        "timezone": "America/New_York",
        "start_local": start.isoformat(),
        "end_local": end.isoformat(),
        "start_utc": start.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "end_utc": end.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "session_close_local": close.isoformat(),
        "session_close_utc": close.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }
