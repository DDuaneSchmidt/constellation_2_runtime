from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from research_lab.audit.audit_log import audit_events
from research_lab.datasets.dataset_registry import list_dataset_snapshots
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.providers.base import DailyOHLCVProvider
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import list_universe_snapshots, store_universe_snapshot


class FixtureProvider(DailyOHLCVProvider):
    provider_name = "fixture"
    provider_version = "fixture_v1"

    def __init__(self, missing: set[str] | None = None) -> None:
        self.missing = missing or set()

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date):
        if symbol in self.missing:
            raise RuntimeError("fixture missing symbol")
        return [
            {"date": "2026-05-14", "symbol": symbol, "open": 100, "high": 102, "low": 99, "close": 101, "adj_close": 101, "volume": 1000, "provider": "fixture", "provider_version": "fixture_v1", "fetched_at": "2026-05-18T00:00:00Z"},
            {"date": "2026-05-15", "symbol": symbol, "open": 101, "high": 103, "low": 100, "close": 102, "adj_close": 102, "volume": 1100, "provider": "fixture", "provider_version": "fixture_v1", "fetched_at": "2026-05-18T00:00:00Z"},
        ]


def _store_universe(tmp_path: Path) -> str:
    existing = list_universe_snapshots(store_root=tmp_path / "store")
    if existing:
        return existing[0]["universe_snapshot_id"]
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
    snapshot = build_universe_snapshot(name="core", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def _build(tmp_path: Path, provider: DailyOHLCVProvider | None = None, *, allow_missing_symbols: bool = False):
    universe_id = _store_universe(tmp_path)
    return build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="fixture",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_v1",
        universe_snapshot_id=universe_id,
        start_date="2026-05-14",
        end_date="2026-05-18",
        provider=provider or FixtureProvider(),
        store_root=tmp_path / "store",
        command="fixture build",
        allow_test_parquet_fallback=True,
        allow_missing_symbols=allow_missing_symbols,
    )


def test_dataset_builder_writes_raw_and_canonical_files(tmp_path: Path) -> None:
    result = _build(tmp_path)
    dataset_id = result["dataset_snapshot"]["dataset_snapshot_id"]
    root = tmp_path / "store" / "datasets" / dataset_id

    assert (root / "data" / "raw" / "provider=fixture" / "interval=1d" / "symbol=SPY.parquet").exists()
    assert (root / "data" / "raw" / "provider=fixture" / "interval=1d" / "symbol=QQQ.parquet").exists()
    assert (root / "data" / "canonical" / "daily_ohlcv.parquet").exists()
    assert (root / "dataset_snapshot.json").exists()
    assert (root / "quality_report.json").exists()
    assert (root / "manifest.json").exists()

    rows = read_parquet_records(root / "data" / "canonical" / "daily_ohlcv.parquet")
    assert [(row["symbol"], row["date"]) for row in rows] == [
        ("QQQ", "2026-05-14"),
        ("QQQ", "2026-05-15"),
        ("SPY", "2026-05-14"),
        ("SPY", "2026-05-15"),
    ]


def test_dataset_builder_registry_and_audit_append(tmp_path: Path) -> None:
    result = _build(tmp_path)
    dataset_id = result["dataset_snapshot"]["dataset_snapshot_id"]

    assert list_dataset_snapshots(store_root=tmp_path / "store")[-1]["dataset_snapshot_id"] == dataset_id
    actions = [event["action"] for event in audit_events(store_root=tmp_path / "store")]
    assert "ohlcv_dataset_build_started" in actions
    assert "raw_symbol_fetch_completed" in actions
    assert "canonical_dataset_written" in actions
    assert "quality_validation_completed" in actions
    assert "ohlcv_dataset_build_completed" in actions


def test_dataset_builder_records_missing_symbol_warning(tmp_path: Path) -> None:
    result = _build(tmp_path, provider=FixtureProvider(missing={"QQQ"}), allow_missing_symbols=True)

    assert result["quality_report"]["quality_status"] == "pass_with_warnings"
    assert result["quality_report"]["symbols_missing"] == ["QQQ"]
    assert result["manifest"]["symbols_missing"] == ["QQQ"]


def test_dataset_builder_snapshot_directory_cannot_be_overwritten(tmp_path: Path) -> None:
    _build(tmp_path)
    try:
        _build(tmp_path)
    except FileExistsError as exc:
        assert "Refusing to overwrite immutable dataset snapshot" in str(exc)
    else:
        raise AssertionError("Expected immutable snapshot overwrite to fail")


def test_same_fixture_input_produces_same_canonical_hash(tmp_path: Path) -> None:
    first = _build(tmp_path / "a")
    second = _build(tmp_path / "b")

    assert first["manifest"]["canonical_file_hash"] == second["manifest"]["canonical_file_hash"]
