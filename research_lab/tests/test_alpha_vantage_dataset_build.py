from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml

from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.providers.alpha_vantage_daily import AlphaVantageDailyProvider
from research_lab.providers.rate_limit import RateLimitPolicy
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


ALPHA_FIXTURE = (
    "timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient\n"
    "2024-01-03,100,102,99,101,100.5,1000,0.0000,1.0\n"
    "2024-01-02,99,101,98,100,99.5,900,0.0000,1.0\n"
)


def _store_universe(tmp_path: Path) -> str:
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "fixture",
                "source_notes": "fixture",
                "symbols": [
                    {"symbol": "SPY", "asset_type": "ETF", "category": "core_index", "active": True, "min_start_date": "2020-01-01", "notes": ""},
                    {"symbol": "QQQ", "asset_type": "ETF", "category": "growth_index", "active": True, "min_start_date": "2020-01-01", "notes": ""},
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="alpha_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_dataset_builder_works_with_mocked_alpha_vantage_provider(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    provider = AlphaVantageDailyProvider(
        api_key="test-key",
        transport=lambda url: ALPHA_FIXTURE,
        rate_limit_policy=RateLimitPolicy(sleep_seconds=0, max_retries=1),
    )

    result = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="alpha_vantage",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_alpha_vantage_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-02",
        end_date="2024-01-03",
        provider=provider,
        store_root=tmp_path / "store",
        command="alpha fixture build",
        allow_test_parquet_fallback=True,
    )

    manifest = result["manifest"]
    assert manifest["provider"] == "alpha_vantage"
    assert manifest["bar_policy_version"] == "bp_daily_ohlcv_alpha_vantage_v1"
    dataset_id = result["dataset_snapshot"]["dataset_snapshot_id"]
    raw_path = tmp_path / "store" / "datasets" / dataset_id / "data" / "raw" / "provider=alpha_vantage" / "interval=1d" / "symbol=SPY.parquet"
    raw_rows = read_parquet_records(raw_path)
    assert "dividend_amount" in raw_rows[0]
    assert "split_coefficient" in raw_rows[0]

