from __future__ import annotations

from typing import Any

from research_lab.events.event_definitions import validate_event_definition
from research_lab.events.event_extractor import extract_events


def signal_rank_value(event: dict[str, Any]) -> float:
    return abs(float(event.get("event_value", 0.0)))


def generate_signals(rows: list[dict[str, Any]], plan: dict[str, Any]) -> list[dict[str, Any]]:
    signal_rule = plan["signal_rule"]
    validate_event_definition(signal_rule)
    research_like_plan = {
        "research_plan_id": plan["backtest_plan_id"],
        "dataset_snapshot_id": plan["dataset_snapshot_id"],
        "universe_snapshot_id": plan["universe_snapshot_id"],
        "symbols": plan["symbols"],
        "date_range": plan["date_range"],
        "event_definition": signal_rule,
    }
    events = extract_events(rows, research_like_plan)
    for event in events:
        event["backtest_plan_id"] = plan["backtest_plan_id"]
        event["signal_date"] = event["event_date"]
        event["signal_value"] = float(event["event_value"])
        event["signal_rank"] = signal_rank_value(event)
    events.sort(key=lambda item: (item["signal_date"], -float(item["signal_rank"]), item["symbol"]))
    return events

