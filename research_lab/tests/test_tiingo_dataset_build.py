from __future__ import annotations

from pathlib import Path

import yaml

from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.providers.rate_limit import RateLimitPolicy
from research_lab.providers.tiingo_daily import TiingoDailyProvider
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


TIINGO_FIXTURE = """[
  {
    "date": "2024-01-03T00:00:00.000Z",
    "open": 100.0,
    "high": 102.0,
    "low": 99.0,
    "close": 101.0,
    "adjOpen": 99.5,
    "adjHigh": 101.5,
    "adjLow": 98.5,
    "adjClose": 100.5,
    "volume": 1000,
    "adjVolume": 1000,
    "divCash": 0.0,
    "splitFactor": 1.0
  },
  {
    "date": "2024-01-02T00:00:00.000Z",
    "open": 99.0,
    "high": 101.0,
    "low": 98.0,
    "close": 100.0,
    "adjOpen": 98.5,
    "adjHigh": 100.5,
    "adjLow": 97.5,
    "adjClose": 99.5,
    "volume": 900,
    "adjVolume": 900,
    "divCash": 0.0,
    "splitFactor": 1.0
  }
]"""


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
    snapshot = build_universe_snapshot(name="tiingo_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_dataset_builder_works_with_mocked_tiingo_provider(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    provider = TiingoDailyProvider(
        api_key="test-key",
        transport=lambda url: TIINGO_FIXTURE,
        rate_limit_policy=RateLimitPolicy(sleep_seconds=0, max_retries=1),
    )

    result = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="tiingo",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_tiingo_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-02",
        end_date="2024-01-03",
        provider=provider,
        store_root=tmp_path / "store",
        command="tiingo fixture build",
        allow_test_parquet_fallback=True,
    )

    manifest = result["manifest"]
    assert manifest["provider"] == "tiingo"
    assert manifest["bar_policy_version"] == "bp_daily_ohlcv_tiingo_v1"
    dataset_id = result["dataset_snapshot"]["dataset_snapshot_id"]
    raw_path = tmp_path / "store" / "datasets" / dataset_id / "data" / "raw" / "provider=tiingo" / "interval=1d" / "symbol=SPY.parquet"
    raw_rows = read_parquet_records(raw_path)
    assert "dividend_amount" in raw_rows[0]
    assert "split_coefficient" in raw_rows[0]
    assert "adj_open" in raw_rows[0]
    assert manifest["adjustment_policy"] == "provider_adj_close"
