from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from research_lab.datasets.bulk_local_csv_import import discover_local_csv_symbols, missing_local_csv_symbols
from research_lab.datasets.dataset_registry import list_dataset_snapshots
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


def _write_csv(root: Path, symbol: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    prices = [100, 97, 98, 96, 99, 100]
    lines = ["date,open,high,low,close,adj_close,volume"]
    for index, close in enumerate(prices, start=1):
        lines.append(f"2024-01-0{index},{close},{close + 1},{close - 1},{close},{close},1000")
    (root / f"{symbol}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _store_universe(tmp_path: Path, symbols: list[str], store_root: Path | None = None) -> str:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "fixture",
                "source_notes": "fixture",
                "symbols": [
                    {"symbol": symbol, "asset_type": "ETF", "category": "fixture", "active": True, "min_start_date": "2024-01-01", "notes": ""}
                    for symbol in symbols
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="bulk_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=store_root or tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_bulk_local_csv_import_discovers_multiple_symbol_csvs(tmp_path: Path) -> None:
    root = tmp_path / "ohlcv"
    _write_csv(root, "QQQ")
    _write_csv(root, "SPY")

    assert discover_local_csv_symbols(root) == ["QQQ", "SPY"]
    assert missing_local_csv_symbols(["SPY", "IWM"], root) == ["IWM"]


def test_bulk_import_is_deterministic_regardless_of_file_listing_order(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "ohlcv"
    _write_csv(root, "QQQ")
    _write_csv(root, "SPY")
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    store_a = tmp_path / "store_a"
    universe_id = _store_universe(tmp_path, ["SPY", "QQQ"], store_a)

    first = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="local_csv",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_local_csv_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-01",
        end_date="2024-01-06",
        store_root=store_a,
        allow_test_parquet_fallback=True,
    )
    store_b = tmp_path / "store_b"
    universe_id_b = _store_universe(tmp_path / "b", ["QQQ", "SPY"], store_b)
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    second = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="local_csv",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_local_csv_v1",
        universe_snapshot_id=universe_id_b,
        start_date="2024-01-01",
        end_date="2024-01-06",
        store_root=store_b,
        allow_test_parquet_fallback=True,
    )

    assert first["manifest"]["canonical_file_hash"] == second["manifest"]["canonical_file_hash"]


def test_missing_csv_fails_when_allow_missing_symbols_false(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "ohlcv"
    _write_csv(root, "SPY")
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    universe_id = _store_universe(tmp_path, ["SPY", "QQQ"])

    with pytest.raises(RuntimeError, match="allow_missing_symbols=false"):
        build_ohlcv_dataset_snapshot(
            dataset_type="ohlcv",
            provider_name="local_csv",
            interval="1d",
            bar_policy_version="bp_daily_ohlcv_local_csv_v1",
            universe_snapshot_id=universe_id,
            start_date="2024-01-01",
            end_date="2024-01-06",
            store_root=tmp_path / "store",
            allow_test_parquet_fallback=True,
        )

    assert list_dataset_snapshots(store_root=tmp_path / "store") == []


def test_missing_csv_warns_when_allow_missing_symbols_true(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "ohlcv"
    _write_csv(root, "SPY")
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    universe_id = _store_universe(tmp_path, ["SPY", "QQQ"])

    result = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="local_csv",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_local_csv_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-01",
        end_date="2024-01-06",
        store_root=tmp_path / "store",
        allow_test_parquet_fallback=True,
        allow_missing_symbols=True,
    )

    assert result["dataset_snapshot"]["quality_status"] == "pass_with_warnings"
    assert result["manifest"]["symbols_missing"] == ["QQQ"]
    assert result["coverage_report"]["symbols_missing"] == ["QQQ"]
