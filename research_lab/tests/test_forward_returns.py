from __future__ import annotations

from pytest import approx

from research_lab.runners.forward_returns import calculate_forward_returns


def test_forward_return_calculator_computes_exact_fixture_returns() -> None:
    rows = [
        {"date": "2024-01-01", "symbol": "SPY", "adj_close": 100},
        {"date": "2024-01-02", "symbol": "SPY", "adj_close": 105},
        {"date": "2024-01-03", "symbol": "SPY", "adj_close": 110},
    ]
    events = [{"symbol": "SPY", "event_date": "2024-01-01"}]

    forward_rows, unavailable = calculate_forward_returns(
        events=events,
        rows=rows,
        windows=[1, 2, 5],
        research_plan_id="rp_fixture",
        dataset_snapshot_id="ds_fixture",
    )

    assert [row["forward_window"] for row in forward_rows] == [1, 2]
    assert forward_rows[0]["forward_return"] == approx(0.05)
    assert forward_rows[1]["forward_return"] == approx(0.10)
    assert unavailable["5"] == 1


def test_events_lacking_future_data_are_excluded_per_window() -> None:
    rows = [
        {"date": "2024-01-01", "symbol": "SPY", "adj_close": 100},
        {"date": "2024-01-02", "symbol": "SPY", "adj_close": 101},
    ]
    events = [{"symbol": "SPY", "event_date": "2024-01-02"}]

    forward_rows, unavailable = calculate_forward_returns(
        events=events,
        rows=rows,
        windows=[1],
        research_plan_id="rp_fixture",
        dataset_snapshot_id="ds_fixture",
    )

    assert forward_rows == []
    assert unavailable["1"] == 1

