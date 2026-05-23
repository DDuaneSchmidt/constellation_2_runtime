from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.workflows.first_dataset_workflow import build_from_present_csvs, first_dataset_status


def _write_csv(root: Path, symbol: str, *, years: int = 4) -> None:
    root.mkdir(parents=True, exist_ok=True)
    rows = ["date,open,high,low,close,adj_close,volume"]
    price = 100.0
    row_index = 0
    for year in range(2020, 2020 + years):
        for month in range(1, 13):
            row_index += 1
            if row_index % 8 == 0:
                price *= 0.97
            else:
                price *= 1.002
            rows.append(f"{year}-{month:02d}-02,{price:.2f},{price + 1:.2f},{price - 1:.2f},{price:.2f},{price:.2f},1000")
    (root / f"{symbol}.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_first_dataset_status_reports_missing_csvs(tmp_path: Path) -> None:
    status = first_dataset_status(csv_root=tmp_path / "csv", store_root=tmp_path / "store")

    assert status["can_build_minimum_viable_dataset"] is False
    assert set(status["missing_minimum_viable_symbols"]) == {"SPY", "QQQ", "IWM", "TLT", "GLD"}
    assert "validate-local-csvs" in status["commands"]["validate_csvs"]


def test_first_dataset_status_reports_ready_when_3_valid_symbols_exist(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    for symbol in ["SPY", "QQQ", "IWM"]:
        _write_csv(csv_root, symbol)

    status = first_dataset_status(csv_root=csv_root, store_root=tmp_path / "store")

    assert status["can_build_minimum_viable_dataset"] is True
    assert status["present_symbol_count"] == 3


def test_build_from_present_csvs_fails_with_fewer_than_3_valid_symbols(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    _write_csv(csv_root, "SPY")
    _write_csv(csv_root, "QQQ")

    with pytest.raises(RuntimeError, match="Fewer than 3"):
        build_from_present_csvs(
            universe="local_etf_minimum_viable_v1",
            csv_root=csv_root,
            start="2020-01-01",
            end="2024-12-31",
            store_root=tmp_path / "store",
            allow_test_parquet_fallback=True,
        )


def test_build_from_present_csvs_succeeds_with_3_mocked_valid_csvs(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    for symbol in ["SPY", "QQQ", "IWM"]:
        _write_csv(csv_root, symbol)

    result = build_from_present_csvs(
        universe="local_etf_minimum_viable_v1",
        csv_root=csv_root,
        start="2020-01-01",
        end="2024-12-31",
        store_root=tmp_path / "store",
        allow_test_parquet_fallback=True,
    )

    dataset = result["dataset_build"]["dataset_snapshot"]
    assert dataset["symbol_count"] == 3
    assert result["csv_readiness_report"]["ready_to_build"] is True
    assert sorted(dataset["symbols"]) == ["IWM", "QQQ", "SPY"]

