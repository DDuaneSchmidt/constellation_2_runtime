from __future__ import annotations

from collections import defaultdict
from typing import Any

from research_lab.events.event_definitions import date_text


def calculate_forward_returns(
    *,
    events: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    windows: list[int],
    research_plan_id: str,
    dataset_snapshot_id: str,
    return_column: str = "adj_close",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not events:
        raise RuntimeError("Cannot calculate forward returns without events")

    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        normalized = dict(row)
        normalized["symbol"] = str(row["symbol"]).upper()
        normalized["date"] = date_text(row["date"])
        rows_by_symbol[normalized["symbol"]].append(normalized)
    for symbol in rows_by_symbol:
        rows_by_symbol[symbol].sort(key=lambda item: item["date"])

    unavailable_counts = {str(window): 0 for window in sorted(windows)}
    forward_rows: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda item: (item["symbol"], item["event_date"])):
        symbol = event["symbol"]
        symbol_rows = rows_by_symbol.get(symbol, [])
        index_by_date = {row["date"]: idx for idx, row in enumerate(symbol_rows)}
        event_index = index_by_date.get(event["event_date"])
        if event_index is None:
            continue
        start_price = float(symbol_rows[event_index][return_column])
        if start_price == 0:
            continue
        for window in sorted(windows):
            future_index = event_index + int(window)
            if future_index >= len(symbol_rows):
                unavailable_counts[str(window)] += 1
                continue
            end_row = symbol_rows[future_index]
            end_price = float(end_row[return_column])
            forward_rows.append(
                {
                    "symbol": symbol,
                    "event_date": event["event_date"],
                    "forward_window": int(window),
                    "forward_date": end_row["date"],
                    "start_adj_close": start_price,
                    "end_adj_close": end_price,
                    "gross_forward_return": end_price / start_price - 1.0,
                    "forward_return": end_price / start_price - 1.0,
                    "research_plan_id": research_plan_id,
                    "dataset_snapshot_id": dataset_snapshot_id,
                    "return_column": return_column,
                }
            )

    forward_rows.sort(key=lambda item: (item["symbol"], item["event_date"], item["forward_window"]))
    return forward_rows, unavailable_counts
