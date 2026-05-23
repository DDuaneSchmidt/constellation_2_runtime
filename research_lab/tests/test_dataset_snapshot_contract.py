from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from research_lab.contracts.schemas import validate_contract
from research_lab.datasets.dataset_snapshot import build_empty_dataset_snapshot
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


def _store_universe(tmp_path: Path) -> str:
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "test",
                "source_notes": "test",
                "symbols": [
                    {
                        "symbol": "SPY",
                        "asset_type": "ETF",
                        "category": "equity",
                        "active": True,
                        "min_start_date": "2005-01-01",
                        "notes": "",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="core", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_dataset_snapshot_schema_validation(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    snapshot = build_empty_dataset_snapshot(
        dataset_type="ohlcv",
        provider="placeholder",
        interval="1d",
        bar_policy_version="bp_v1",
        universe_snapshot_id=universe_id,
        start_date="2005-01-01",
        end_date="2026-05-18",
        store_root=tmp_path / "store",
    )

    validate_contract("dataset_snapshot", snapshot)
    assert snapshot["quality_status"] == "pending"
    assert snapshot["row_count"] == 0
    assert snapshot["dataset_snapshot_id"].startswith("ds_ohlcv_1d_core_20050101_20260518_")


def test_dataset_hash_changes_when_date_range_changes(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    common = {
        "dataset_type": "ohlcv",
        "provider": "placeholder",
        "interval": "1d",
        "bar_policy_version": "bp_v1",
        "universe_snapshot_id": universe_id,
        "start_date": "2005-01-01",
        "store_root": tmp_path / "store",
    }

    first = build_empty_dataset_snapshot(end_date="2026-05-18", **common)
    second = build_empty_dataset_snapshot(end_date="2026-05-19", **common)

    assert first["content_hash"] != second["content_hash"]


def test_invalid_quality_status_is_rejected(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    snapshot = build_empty_dataset_snapshot(
        dataset_type="ohlcv",
        provider="placeholder",
        interval="1d",
        bar_policy_version="bp_v1",
        universe_snapshot_id=universe_id,
        start_date="2005-01-01",
        end_date="2026-05-18",
        store_root=tmp_path / "store",
    )
    snapshot["quality_status"] = "green"

    with pytest.raises(Exception):
        validate_contract("dataset_snapshot", snapshot)


def test_missing_required_dataset_field_fails_validation(tmp_path: Path) -> None:
    universe_id = _store_universe(tmp_path)
    snapshot = build_empty_dataset_snapshot(
        dataset_type="ohlcv",
        provider="placeholder",
        interval="1d",
        bar_policy_version="bp_v1",
        universe_snapshot_id=universe_id,
        start_date="2005-01-01",
        end_date="2026-05-18",
        store_root=tmp_path / "store",
    )
    del snapshot["provider"]

    with pytest.raises(Exception):
        validate_contract("dataset_snapshot", snapshot)
