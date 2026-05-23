from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from research_lab.datasets.dataset_registry import list_dataset_snapshots
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.providers.base import ProviderFetchError
from research_lab.providers.local_csv_daily import (
    LOCAL_CSV_FALLBACK_ADJUSTMENT_POLICY,
    LocalCSVDailyProvider,
    normalize_local_csv_rows,
)
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


def _write_lowercase_csv(root: Path, symbol: str, *, include_adj_close: bool = True) -> None:
    root.mkdir(parents=True, exist_ok=True)
    header = "date,open,high,low,close"
    values = [
        "2024-01-02,100,102,99,101",
        "2024-01-03,101,103,100,102",
        "2024-02-02,102,104,101,103",
    ]
    if include_adj_close:
        header += ",adj_close"
        values = [f"{row},{row.split(',')[4]}" for row in values]
    header += ",volume"
    values = [f"{row},1000" for row in values]
    (root / f"{symbol}.csv").write_text(header + "\n" + "\n".join(values) + "\n", encoding="utf-8")


def _write_yahoo_csv(root: Path, symbol: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{symbol}.csv").write_text(
        "Date,Open,High,Low,Close,Adj Close,Volume\n"
        "2024-01-02,200,202,199,201,200.5,2000\n"
        "2024-01-03,201,203,200,202,201.5,2100\n",
        encoding="utf-8",
    )


def _store_universe(tmp_path: Path) -> str:
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "test",
                "source_notes": "test",
                "symbols": [
                    {"symbol": "SPY", "asset_type": "ETF", "category": "equity", "active": True, "min_start_date": "2005-01-01", "notes": ""},
                    {"symbol": "QQQ", "asset_type": "ETF", "category": "equity", "active": True, "min_start_date": "2005-01-01", "notes": ""},
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="local_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_local_csv_provider_reads_fixture_csv(tmp_path: Path) -> None:
    root = tmp_path / "ohlcv"
    _write_lowercase_csv(root, "SPY")
    provider = LocalCSVDailyProvider(root=root)

    rows = provider.fetch_daily_ohlcv("SPY", date(2024, 1, 2), date(2024, 2, 1))

    assert len(rows) == 2
    assert rows[0]["symbol"] == "SPY"
    assert rows[0]["provider"] == "local_csv"


def test_local_csv_column_normalization_lowercase_and_yahoo_style(tmp_path: Path) -> None:
    root = tmp_path / "ohlcv"
    _write_lowercase_csv(root, "SPY")
    _write_yahoo_csv(root, "QQQ")
    provider = LocalCSVDailyProvider(root=root)

    spy = provider.fetch_daily_ohlcv("SPY", date(2024, 1, 1), date(2024, 1, 31))
    qqq = provider.fetch_daily_ohlcv("QQQ", date(2024, 1, 1), date(2024, 1, 31))

    assert spy[0]["adj_close"] == "101"
    assert qqq[0]["adj_close"] == "200.5"


def test_local_csv_missing_adj_close_is_warned_and_filled_from_close() -> None:
    rows = normalize_local_csv_rows(
        symbol="SPY",
        csv_rows=[{"date": "2024-01-02", "open": "1", "high": "2", "low": "1", "close": "1.5", "volume": "100"}],
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
    )

    assert rows[0]["adj_close"] == "1.5"
    assert rows[0]["adjustment_policy"] == LOCAL_CSV_FALLBACK_ADJUSTMENT_POLICY


def test_local_csv_date_filtering(tmp_path: Path) -> None:
    root = tmp_path / "ohlcv"
    _write_lowercase_csv(root, "SPY")
    provider = LocalCSVDailyProvider(root=root)

    rows = provider.fetch_daily_ohlcv("SPY", date(2024, 1, 3), date(2024, 1, 3))

    assert [row["date"] for row in rows] == ["2024-01-03"]


def test_local_csv_missing_file_fails_symbol_explicitly(tmp_path: Path) -> None:
    provider = LocalCSVDailyProvider(root=tmp_path / "ohlcv")

    with pytest.raises(ProviderFetchError) as exc:
        provider.fetch_daily_ohlcv("SPY", date(2024, 1, 1), date(2024, 1, 31))

    assert "Local CSV file not found" in str(exc.value)
    assert exc.value.diagnostic["symbol"] == "SPY"


def test_dataset_builder_creates_raw_and_canonical_parquet_from_local_csv(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "ohlcv"
    _write_lowercase_csv(root, "SPY", include_adj_close=False)
    _write_yahoo_csv(root, "QQQ")
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    universe_id = _store_universe(tmp_path)

    result = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="local_csv",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_local_csv_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-02",
        end_date="2024-02-01",
        store_root=tmp_path / "store",
        command="local csv fixture build",
        allow_test_parquet_fallback=True,
    )

    dataset_id = result["dataset_snapshot"]["dataset_snapshot_id"]
    dataset_root = tmp_path / "store" / "datasets" / dataset_id
    canonical = dataset_root / "data" / "canonical" / "daily_ohlcv.parquet"

    assert result["manifest"]["provider"] == "local_csv"
    assert result["manifest"]["adjustment_policy"] == "unadjusted_close_as_adj_close"
    assert result["quality_report"]["adjustment_policy"] == "unadjusted_close_as_adj_close"
    assert "adj_close_derived_from_close" in result["quality_report"]["warning_reasons"]
    assert (dataset_root / "data" / "raw" / "provider=local_csv" / "interval=1d" / "symbol=SPY.parquet").exists()
    assert canonical.exists()
    assert read_parquet_records(canonical)
    assert list_dataset_snapshots(store_root=tmp_path / "store")[-1]["dataset_snapshot_id"] == dataset_id


def test_local_csv_failed_build_does_not_append_dataset_registry(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "ohlcv"
    root.mkdir()
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    universe_id = _store_universe(tmp_path)

    with pytest.raises(RuntimeError):
        build_ohlcv_dataset_snapshot(
            dataset_type="ohlcv",
            provider_name="local_csv",
            interval="1d",
            bar_policy_version="bp_daily_ohlcv_local_csv_v1",
            universe_snapshot_id=universe_id,
            start_date="2024-01-02",
            end_date="2024-02-01",
            store_root=tmp_path / "store",
            command="local csv missing build",
            allow_test_parquet_fallback=True,
        )

    assert list_dataset_snapshots(store_root=tmp_path / "store") == []
