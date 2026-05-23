from __future__ import annotations

from research_lab.longitudinal.batch_scheduler import scheduled_as_of_dates


def _rows(days: list[str]):
    return [{"date": day, "symbol": "SPY"} for day in days]


def test_weekly_scheduler_picks_deterministic_trading_dates() -> None:
    rows = _rows(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-08", "2024-01-09", "2024-01-12"])

    assert scheduled_as_of_dates(rows, start="2024-01-01", end="2024-01-31", frequency="weekly") == ["2024-01-04", "2024-01-12"]


def test_monthly_scheduler_picks_month_end_trading_dates() -> None:
    rows = _rows(["2024-01-30", "2024-01-31", "2024-02-27", "2024-02-29"])

    assert scheduled_as_of_dates(rows, start="2024-01-01", end="2024-02-29", frequency="monthly") == ["2024-01-31", "2024-02-29"]

