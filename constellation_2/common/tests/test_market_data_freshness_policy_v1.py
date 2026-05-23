from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.market_data.freshness_policy_v1 import (
    PARTIAL_DATA_AVAILABLE,
    PENDING_VENDOR_DATA,
    READ_ONLY_PRIOR_DAY_FALLBACK,
    VALIDATED_CURRENT_DAY,
    classify_market_data_freshness_v1,
    resolve_market_session_v1,
)


def _calendar(root: Path, rows: dict[str, bool]) -> None:
    out = root / "market_calendar_v1" / "NYSE"
    out.mkdir(parents=True, exist_ok=True)
    with (out / "2026.jsonl").open("w", encoding="utf-8") as handle:
        for day, is_open in sorted(rows.items()):
            handle.write(json.dumps({"day_utc": day, "exchange": "NYSE", "is_trading_session": is_open}) + "\n")


def test_normal_trading_day_before_close_is_pending_vendor_data(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-05-21": True})
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        required_symbols=["SPY", "QQQ"],
        fetched_symbols=[],
        missing_symbols=["SPY", "QQQ"],
        stale_symbols=[],
        as_of_utc="2026-05-21T18:00:00Z",
        provider_status="FAILED",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == PENDING_VENDOR_DATA
    assert decision.usable_for_candidate_generation is False
    assert decision.usable_for_candidate_visibility is False
    assert decision.usable_for_execution_candidate_generation is False


def test_after_close_inside_vendor_lag_is_still_pending(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-05-21": True})
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        required_symbols=["SPY"],
        fetched_symbols=[],
        missing_symbols=["SPY"],
        stale_symbols=[],
        as_of_utc="2026-05-21T21:00:00Z",
        provider_status="FAILED",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == PENDING_VENDOR_DATA


def test_early_close_day_uses_early_close_plus_lag(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-11-27": True})
    session = resolve_market_session_v1(truth_root=tmp_path, day_utc="2026-11-27", required_symbols=["SPY"], vendor_lag_minutes=120)
    assert session["equity_early_close"] is True
    assert session["official_close_local"].endswith("13:00:00-05:00")
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-11-27",
        required_symbols=["SPY"],
        fetched_symbols=[],
        missing_symbols=["SPY"],
        stale_symbols=[],
        as_of_utc="2026-11-27T18:30:00Z",
        provider_status="FAILED",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == PENDING_VENDOR_DATA


def test_market_holiday_uses_read_only_prior_day_fallback(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-05-25": False})
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-05-25",
        required_symbols=["SPY"],
        fetched_symbols=[],
        missing_symbols=["SPY"],
        stale_symbols=[],
        as_of_utc="2026-05-25T20:30:00Z",
        provider_status="FAILED",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == READ_ONLY_PRIOR_DAY_FALLBACK


def test_partial_vendor_response_is_not_candidate_generation_ready(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-05-21": True})
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        required_symbols=["SPY", "QQQ"],
        fetched_symbols=["SPY"],
        missing_symbols=["QQQ"],
        stale_symbols=[],
        current_session_symbols=["SPY"],
        as_of_utc="2026-05-21T22:30:00Z",
        provider_status="PARTIAL",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == PARTIAL_DATA_AVAILABLE
    assert decision.usable_for_candidate_generation is True
    assert decision.usable_for_candidate_visibility is True
    assert decision.usable_for_execution_candidate_generation is False


def test_stale_prior_day_rows_for_current_date_are_read_only(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-05-21": True})
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        required_symbols=["SPY"],
        fetched_symbols=["SPY"],
        missing_symbols=[],
        stale_symbols=["SPY"],
        current_session_symbols=[],
        as_of_utc="2026-05-21T22:30:00Z",
        provider_status="STALE",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == READ_ONLY_PRIOR_DAY_FALLBACK
    assert decision.usable_for_candidate_generation is False


def test_all_required_current_symbols_validate_current_day(tmp_path: Path) -> None:
    _calendar(tmp_path, {"2026-05-21": True})
    decision = classify_market_data_freshness_v1(
        truth_root=tmp_path,
        day_utc="2026-05-21",
        required_symbols=["SPY", "QQQ"],
        fetched_symbols=["SPY", "QQQ"],
        missing_symbols=[],
        stale_symbols=[],
        current_session_symbols=["SPY", "QQQ"],
        as_of_utc="2026-05-21T22:30:00Z",
        provider_status="CURRENT",
        vendor_lag_minutes=120,
    )
    assert decision.freshness_state == VALIDATED_CURRENT_DAY
    assert decision.usable_for_candidate_generation is True
