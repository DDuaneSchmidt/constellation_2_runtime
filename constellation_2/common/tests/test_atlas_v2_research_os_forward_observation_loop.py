from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.forward_observation_loop import (
    MEASURED_COLUMNS,
    NEW_SIGNAL_COLUMNS,
    PENDING_COLUMNS,
    SCOREBOARD_COLUMNS,
    TARGET_FAMILY_ID,
    build_forward_observation_loop,
    write_forward_observation_loop,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _bar_rows(*, latest_signal: bool = True, include_exit: bool = False) -> list[dict[str, str]]:
    start = datetime(2026, 6, 6, 13, 0, tzinfo=UTC)
    rows: list[dict[str, str]] = []
    price = 100.0
    for index in range(55):
        ts = start + timedelta(minutes=30 * index)
        open_price = price
        close = price + 0.2
        if index in {51, 52, 53}:
            open_price = price + 0.1
            close = price - 0.1
        if index == 54:
            if latest_signal:
                open_price = price - 0.1
                close = price + 0.2
            else:
                open_price = price + 0.1
                close = price - 0.1
        rows.append(
            {
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "open": f"{open_price:.2f}",
                "high": f"{max(open_price, close) + 0.1:.2f}",
                "low": f"{min(open_price, close) - 0.1:.2f}",
                "close": f"{close:.2f}",
                "volume": "1000",
            }
        )
        price += 0.2
    if include_exit:
        ts = start + timedelta(minutes=30 * 55)
        rows.append({"timestamp": ts.isoformat().replace("+00:00", "Z"), "open": "111.00", "high": "111.40", "low": "110.90", "close": "111.30", "volume": "1000"})
    return rows


def _seed_rule(root: Path, data_file: Path) -> None:
    fields = ["candidate_id", "family_id", "symbol", "timeframe", "data_file", "sample_size", "expectancy", "profit_factor", "max_drawdown", "classification", "fallback_used", "regime_original", "regime_bridged", "notes"]
    rows = [
        {
            "candidate_id": "candidate_alpha",
            "family_id": TARGET_FAMILY_ID,
            "symbol": "BAC",
            "timeframe": "30m",
            "data_file": str(data_file),
            "sample_size": "237",
            "expectancy": "0.0006",
            "profit_factor": "1.4",
            "max_drawdown": "-0.04",
            "classification": "EXACT_CONFIRMED_STRONG",
            "fallback_used": "false",
            "regime_original": "TRENDING",
            "regime_bridged": "TRENDING",
            "notes": "seed",
        }
    ]
    _write_csv(root / "exact_replay_without_fallback" / "exact_replay_results.csv", fields, rows)


def test_loop_reports_not_started_when_latest_bar_has_no_signal(tmp_path: Path) -> None:
    data_file = tmp_path / "BAC_30m.csv"
    rows = _bar_rows(latest_signal=False)
    _write_csv(data_file, ["timestamp", "open", "high", "low", "close", "volume"], rows)
    _seed_rule(tmp_path, data_file)

    report = build_forward_observation_loop(tmp_path, created_at="2026-06-07T00:00:00Z", now="2026-06-07T00:00:00Z")

    assert report["summary"]["new_signals"] == 0
    assert report["summary"]["pending_observations"] == 0
    assert report["summary"]["forward_classification"] == "FORWARD_NOT_STARTED"


def test_loop_creates_pending_observation_for_latest_signal(tmp_path: Path) -> None:
    data_file = tmp_path / "BAC_30m.csv"
    rows = _bar_rows(latest_signal=True)
    _write_csv(data_file, ["timestamp", "open", "high", "low", "close", "volume"], rows)
    _seed_rule(tmp_path, data_file)

    report = build_forward_observation_loop(tmp_path, created_at="2026-06-07T00:00:00Z", now=rows[-1]["timestamp"])

    assert report["summary"]["new_signals"] == 1
    assert report["summary"]["pending_observations"] == 1
    assert report["summary"]["measured_outcomes"] == 0
    assert report["summary"]["forward_classification"] == "FORWARD_PENDING"
    assert report["pending_observations"][0]["blocker"] == "PENDING_RETURN_WINDOW"


def test_loop_measures_prior_pending_observation_after_exit_bar_available(tmp_path: Path) -> None:
    data_file = tmp_path / "BAC_30m.csv"
    first_rows = _bar_rows(latest_signal=True, include_exit=False)
    _write_csv(data_file, ["timestamp", "open", "high", "low", "close", "volume"], first_rows)
    _seed_rule(tmp_path, data_file)
    first = build_forward_observation_loop(tmp_path, created_at="2026-06-07T00:00:00Z", now=first_rows[-1]["timestamp"])
    write_forward_observation_loop(first, tmp_path)

    second_rows = _bar_rows(latest_signal=True, include_exit=True)
    _write_csv(data_file, ["timestamp", "open", "high", "low", "close", "volume"], second_rows)
    second = build_forward_observation_loop(tmp_path, created_at="2026-06-07T00:30:00Z", now=second_rows[-1]["timestamp"])

    assert second["summary"]["new_signals"] == 0
    assert second["summary"]["pending_observations"] == 0
    assert second["summary"]["measured_outcomes"] == 1
    assert second["summary"]["forward_classification"] == "FORWARD_INSUFFICIENT_SAMPLE"
    assert second["measured_forward_outcomes"][0]["outcome_classification"] == "POSITIVE_RETURN"


def test_loop_writes_required_files_and_authority_boundary(tmp_path: Path) -> None:
    data_file = tmp_path / "BAC_30m.csv"
    rows = _bar_rows(latest_signal=True)
    _write_csv(data_file, ["timestamp", "open", "high", "low", "close", "volume"], rows)
    _seed_rule(tmp_path, data_file)
    report = build_forward_observation_loop(tmp_path, created_at="2026-06-07T00:00:00Z", now=rows[-1]["timestamp"])
    paths = write_forward_observation_loop(report, tmp_path)

    expected_columns = {
        "new_forward_signals": NEW_SIGNAL_COLUMNS,
        "pending_observations": PENDING_COLUMNS,
        "measured_forward_outcomes": MEASURED_COLUMNS,
        "forward_observation_scoreboard": SCOREBOARD_COLUMNS,
    }
    for key in ["latest_json", "latest_summary", *expected_columns]:
        assert paths[key].exists(), key
    for key, columns in expected_columns.items():
        with paths[key].open(newline="", encoding="utf-8") as handle:
            assert csv.DictReader(handle).fieldnames == columns
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["authority_boundary"]["observation_only"] is True
    assert payload["authority_boundary"]["trade_recommendations"] is False
    assert payload["authority_boundary"]["broker_execution"] is False
