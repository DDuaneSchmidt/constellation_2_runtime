from __future__ import annotations

from collections import defaultdict
from typing import Any

from research_lab.events.event_definitions import (
    daily_range_percentile_events,
    daily_return_events,
    date_text,
    validate_event_definition,
)


def extract_events(rows: list[dict[str, Any]], research_plan: dict[str, Any]) -> list[dict[str, Any]]:
    event_definition = research_plan["event_definition"]
    validate_event_definition(event_definition)
    start = date_text(research_plan["date_range"]["start"])
    end = date_text(research_plan["date_range"]["end"])
    requested_symbols = {symbol.upper() for symbol in research_plan["symbols"]}
    available_symbols = {str(row["symbol"]).upper() for row in rows}
    missing_symbols = sorted(requested_symbols - available_symbols)
    if missing_symbols:
        raise RuntimeError(f"ResearchPlan symbols missing from dataset: {', '.join(missing_symbols)}")

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row["symbol"]).upper()
        row_date = date_text(row["date"])
        if symbol in requested_symbols and start <= row_date <= end:
            normalized = dict(row)
            normalized["symbol"] = symbol
            normalized["date"] = row_date
            grouped[symbol].append(normalized)

    if not any(grouped.values()):
        raise RuntimeError("Date range has no dataset rows for requested symbols")

    events: list[dict[str, Any]] = []
    for symbol in sorted(grouped):
        symbol_rows = sorted(grouped[symbol], key=lambda item: item["date"])
        event_type = event_definition["type"]
        if event_type in {"daily_return_below_threshold", "daily_return_above_threshold"}:
            symbol_events = daily_return_events(symbol_rows, event_definition)
        elif event_type in {"daily_range_percentile_above", "daily_range_percentile_below"}:
            symbol_events = daily_range_percentile_events(symbol_rows, event_definition)
        else:
            raise RuntimeError(f"Unsupported event definition type: {event_type}")
        for event in symbol_events:
            event.update(
                {
                    "research_plan_id": research_plan["research_plan_id"],
                    "dataset_snapshot_id": research_plan["dataset_snapshot_id"],
                    "universe_snapshot_id": research_plan["universe_snapshot_id"],
                }
            )
        events.extend(symbol_events)

    events.sort(key=lambda item: (item["symbol"], item["event_date"]))
    return events
