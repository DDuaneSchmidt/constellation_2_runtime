from __future__ import annotations

import math
from typing import Any


SUPPORTED_EVENT_TYPES = {
    "daily_return_below_threshold",
    "daily_return_above_threshold",
    "daily_range_percentile_above",
    "daily_range_percentile_below",
}


def validate_event_definition(event_definition: dict[str, Any]) -> None:
    event_type = event_definition.get("type")
    params = event_definition.get("params", {})
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise ValueError(f"Unsupported event definition type: {event_type}")
    if not isinstance(params, dict):
        raise ValueError("event_definition.params must be an object")
    if event_type in {"daily_return_below_threshold", "daily_return_above_threshold"}:
        if "threshold" not in params:
            raise ValueError(f"{event_type} requires params.threshold")
        float(params["threshold"])
        return_column = params.get("return_column", "adj_close")
        if return_column not in {"close", "adj_close"}:
            raise ValueError("return_column must be close or adj_close")
    if event_type in {"daily_range_percentile_above", "daily_range_percentile_below"}:
        percentile = float(params.get("percentile", 90 if event_type.endswith("above") else 10))
        if percentile < 0 or percentile > 100:
            raise ValueError("percentile must be between 0 and 100")


def date_text(value: Any) -> str:
    return str(value)[:10]


def daily_return_events(symbol_rows: list[dict[str, Any]], event_definition: dict[str, Any]) -> list[dict[str, Any]]:
    validate_event_definition(event_definition)
    event_type = event_definition["type"]
    params = event_definition.get("params", {})
    return_column = params.get("return_column", "adj_close")
    threshold = float(params["threshold"])
    events: list[dict[str, Any]] = []
    prior: dict[str, Any] | None = None
    for row in sorted(symbol_rows, key=lambda item: date_text(item["date"])):
        if prior is None:
            prior = row
            continue
        previous = float(prior[return_column])
        current = float(row[return_column])
        if previous == 0:
            prior = row
            continue
        event_value = current / previous - 1.0
        matched = event_value < threshold if event_type.endswith("below_threshold") else event_value > threshold
        if matched:
            events.append(
                {
                    "symbol": row["symbol"],
                    "event_date": date_text(row["date"]),
                    "event_type": event_type,
                    "event_value": event_value,
                    "threshold": threshold,
                    "return_column": return_column,
                }
            )
        prior = row
    return events


def _percentile(sorted_values: list[float], percentile: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot compute percentile from empty values")
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (percentile / 100.0) * (len(sorted_values) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[int(rank)]
    weight = rank - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def daily_range_percentile_events(symbol_rows: list[dict[str, Any]], event_definition: dict[str, Any]) -> list[dict[str, Any]]:
    validate_event_definition(event_definition)
    event_type = event_definition["type"]
    params = event_definition.get("params", {})
    default_percentile = 90 if event_type.endswith("above") else 10
    percentile = float(params.get("percentile", default_percentile))
    enriched: list[tuple[dict[str, Any], float]] = []
    for row in sorted(symbol_rows, key=lambda item: date_text(item["date"])):
        close = float(row["close"])
        if close <= 0:
            continue
        daily_range = (float(row["high"]) - float(row["low"])) / close
        enriched.append((row, daily_range))
    threshold = _percentile(sorted(value for _, value in enriched), percentile)
    events: list[dict[str, Any]] = []
    for row, event_value in enriched:
        matched = event_value > threshold if event_type.endswith("above") else event_value < threshold
        if matched:
            events.append(
                {
                    "symbol": row["symbol"],
                    "event_date": date_text(row["date"]),
                    "event_type": event_type,
                    "event_value": event_value,
                    "percentile": percentile,
                    "percentile_threshold": threshold,
                    "return_column": "adj_close",
                }
            )
    return events

