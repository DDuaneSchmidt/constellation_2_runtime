from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any


def _date_text(value: Any) -> str:
    return str(value)[:10]


def available_trading_dates(rows: list[dict[str, Any]], *, start: str, end: str) -> list[str]:
    dates = sorted({_date_text(row["date"]) for row in rows if start <= _date_text(row["date"]) <= end})
    return dates


def scheduled_as_of_dates(rows: list[dict[str, Any]], *, start: str, end: str, frequency: str) -> list[str]:
    dates = available_trading_dates(rows, start=start, end=end)
    if frequency not in {"daily", "weekly", "monthly"}:
        raise RuntimeError(f"Unsupported frequency: {frequency}")
    if frequency == "daily":
        return dates
    parsed = [(date.fromisoformat(day), day) for day in dates]
    if frequency == "weekly":
        by_week: dict[tuple[int, int], list[tuple[date, str]]] = defaultdict(list)
        for item in parsed:
            iso = item[0].isocalendar()
            by_week[(iso.year, iso.week)].append(item)
        result: list[str] = []
        for key in sorted(by_week):
            week_days = sorted(by_week[key])
            friday_or_before = [item for item in week_days if item[0].weekday() <= 4]
            result.append((friday_or_before or week_days)[-1][1])
        return result
    by_month: dict[tuple[int, int], list[tuple[date, str]]] = defaultdict(list)
    for item in parsed:
        by_month[(item[0].year, item[0].month)].append(item)
    return [sorted(by_month[key])[-1][1] for key in sorted(by_month)]

