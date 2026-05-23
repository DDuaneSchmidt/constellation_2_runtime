from __future__ import annotations

from research_lab.events.event_extractor import extract_events
from research_lab.research.research_plan import build_research_plan


ROWS = [
    {"date": "2024-01-01", "symbol": "SPY", "open": 100, "high": 101, "low": 99, "close": 100, "adj_close": 100, "volume": 1000},
    {"date": "2024-01-02", "symbol": "SPY", "open": 100, "high": 101, "low": 97, "close": 98, "adj_close": 98, "volume": 1000},
    {"date": "2024-01-03", "symbol": "SPY", "open": 98, "high": 104, "low": 98, "close": 103, "adj_close": 103, "volume": 1000},
    {"date": "2024-01-04", "symbol": "SPY", "open": 103, "high": 103, "low": 101, "close": 102, "adj_close": 102, "volume": 1000},
]


def _plan(event_type: str, params: dict) -> dict:
    return build_research_plan(
        hypothesis_id="hyp_events",
        title="events",
        dataset_snapshot_id="ds_fixture",
        universe_snapshot_id="us_fixture",
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-04",
        event_definition={"type": event_type, "params": params},
        forward_return_windows=[1],
        created_at="2024-01-01T00:00:00Z",
    )


def test_daily_return_below_threshold_extracts_expected_fixture_events() -> None:
    events = extract_events(ROWS, _plan("daily_return_below_threshold", {"return_column": "adj_close", "threshold": -0.01}))

    assert [event["event_date"] for event in events] == ["2024-01-02"]


def test_daily_return_above_threshold_extracts_expected_fixture_events() -> None:
    events = extract_events(ROWS, _plan("daily_return_above_threshold", {"return_column": "adj_close", "threshold": 0.03}))

    assert [event["event_date"] for event in events] == ["2024-01-03"]


def test_daily_range_percentile_above_extracts_deterministic_events() -> None:
    events = extract_events(ROWS, _plan("daily_range_percentile_above", {"percentile": 75}))

    assert [event["event_date"] for event in events] == ["2024-01-03"]
    assert events[0]["event_type"] == "daily_range_percentile_above"

