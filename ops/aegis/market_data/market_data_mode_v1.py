from __future__ import annotations

from typing import Any

INTRADAY_OPERATIONAL = "INTRADAY_OPERATIONAL"
FINAL_EOD_CERTIFIED = "FINAL_EOD_CERTIFIED"
PROVISIONAL_INTRADAY = "PROVISIONAL_INTRADAY"
STALE_PRIOR_DAY = "STALE_PRIOR_DAY"
UNAVAILABLE = "UNAVAILABLE"

DATA_MODES = {
    INTRADAY_OPERATIONAL,
    FINAL_EOD_CERTIFIED,
}


def normalize_market_data_mode_v1(value: Any, *, default: str = INTRADAY_OPERATIONAL) -> str:
    text = str(value or "").strip().upper()
    if text in DATA_MODES:
        return text
    return default


def finalization_status_for_record_v1(*, session_date: str, day_utc: str, data_finality: str) -> str:
    if not session_date or session_date != day_utc:
        return "FINAL_UNAVAILABLE"
    if str(data_finality or "").upper() == "FINAL_EOD":
        return "FINAL"
    return "NOT_FINAL_YET"


def market_data_mode_for_record_v1(*, session_date: str, day_utc: str, data_finality: str) -> str:
    if not session_date:
        return UNAVAILABLE
    if session_date != day_utc:
        return STALE_PRIOR_DAY
    if str(data_finality or "").upper() == "FINAL_EOD":
        return FINAL_EOD_CERTIFIED
    return PROVISIONAL_INTRADAY


def usable_for_v1(*, record_mode: str, requested_mode: str) -> dict[str, bool]:
    record_mode = str(record_mode or "").upper()
    requested_mode = normalize_market_data_mode_v1(requested_mode)
    intraday_ok = record_mode in {PROVISIONAL_INTRADAY, FINAL_EOD_CERTIFIED}
    final_ok = record_mode == FINAL_EOD_CERTIFIED
    return {
        "sleeve_intraday_generation": intraday_ok and requested_mode == INTRADAY_OPERATIONAL,
        "manual_capture": intraday_ok and requested_mode == INTRADAY_OPERATIONAL,
        "final_eod_certification": final_ok,
        "research_backtest": final_ok,
    }
