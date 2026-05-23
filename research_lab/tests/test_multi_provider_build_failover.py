from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.providers.base import DailyOHLCVProvider
from research_lab.providers.stooq_daily import STOOQ_ADJUSTMENT_POLICY
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


class MockStooqProvider(DailyOHLCVProvider):
    provider_name = "stooq"
    provider_version = "stooq_csv_daily_v1"
    adjustment_policy = STOOQ_ADJUSTMENT_POLICY

    def fetch_daily_ohlcv(self, symbol: str, start: date, end: date):
        return [
            {
                "date": "2024-01-02",
                "symbol": symbol,
                "open": 100,
                "high": 102,
                "low": 99,
                "close": 101,
                "adj_close": 101,
                "volume": 1000,
                "provider": "stooq",
                "provider_version": "stooq_csv_daily_v1",
                "fetched_at": "2026-05-18T00:00:00Z",
                "adjustment_policy": STOOQ_ADJUSTMENT_POLICY,
            },
            {
                "date": "2024-01-03",
                "symbol": symbol,
                "open": 101,
                "high": 103,
                "low": 100,
                "close": 102,
                "adj_close": 102,
                "volume": 1200,
                "provider": "stooq",
                "provider_version": "stooq_csv_daily_v1",
                "fetched_at": "2026-05-18T00:00:00Z",
                "adjustment_policy": STOOQ_ADJUSTMENT_POLICY,
            },
        ]


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
    snapshot = build_universe_snapshot(name="core", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_dataset_builder_can_build_from_mocked_stooq_provider(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    result = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="stooq",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_stooq_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-02",
        end_date="2024-01-03",
        provider=MockStooqProvider(),
        store_root=tmp_path / "store",
        command="mock stooq build",
        allow_test_parquet_fallback=True,
    )

    manifest = result["manifest"]
    quality_report = result["quality_report"]
    snapshot = result["dataset_snapshot"]
    root = tmp_path / "store" / "datasets" / snapshot["dataset_snapshot_id"]

    assert manifest["provider"] == "stooq"
    assert manifest["adjustment_policy"] == STOOQ_ADJUSTMENT_POLICY
    assert snapshot["adjustment_policy"] == STOOQ_ADJUSTMENT_POLICY
    assert quality_report["adjustment_policy"] == STOOQ_ADJUSTMENT_POLICY
    assert "adj_close_derived_from_close" in quality_report["warning_reasons"]
    assert (root / "data" / "raw" / "provider=stooq" / "interval=1d" / "symbol=SPY.parquet").exists()
    assert (root / "data" / "canonical" / "daily_ohlcv.parquet").exists()
