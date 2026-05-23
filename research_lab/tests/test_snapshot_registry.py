from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from research_lab.audit.audit_log import audit_events
from research_lab.cli import main
from research_lab.datasets.dataset_registry import list_dataset_snapshots, store_dataset_snapshot
from research_lab.datasets.dataset_snapshot import build_empty_dataset_snapshot
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import list_universe_snapshots, store_universe_snapshot


def _source(path: Path) -> Path:
    path.write_text(
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
    return path


def test_registry_append_and_list_behavior(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    universe = build_universe_snapshot(name="core", version="v1", input_path=_source(tmp_path / "u.yaml"))
    store_universe_snapshot(universe, store_root=store_root)
    dataset = build_empty_dataset_snapshot(
        dataset_type="ohlcv",
        provider="placeholder",
        interval="1d",
        bar_policy_version="bp_v1",
        universe_snapshot_id=universe["universe_snapshot_id"],
        start_date="2005-01-01",
        end_date="2026-05-18",
        store_root=store_root,
    )
    store_dataset_snapshot(dataset, store_root=store_root)

    assert list_universe_snapshots(store_root=store_root)[0]["universe_snapshot_id"] == universe["universe_snapshot_id"]
    assert list_dataset_snapshots(store_root=store_root)[0]["dataset_snapshot_id"] == dataset["dataset_snapshot_id"]


def test_dataset_snapshot_cannot_be_overwritten(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    universe = build_universe_snapshot(name="core", version="v1", input_path=_source(tmp_path / "u.yaml"))
    store_universe_snapshot(universe, store_root=store_root)
    dataset = build_empty_dataset_snapshot(
        dataset_type="ohlcv",
        provider="placeholder",
        interval="1d",
        bar_policy_version="bp_v1",
        universe_snapshot_id=universe["universe_snapshot_id"],
        start_date="2005-01-01",
        end_date="2026-05-18",
        store_root=store_root,
    )
    store_dataset_snapshot(dataset, store_root=store_root)

    with pytest.raises(FileExistsError):
        store_dataset_snapshot(dataset, store_root=store_root)


def test_cli_create_and_validate_append_audit_events(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    source = _source(tmp_path / "u.yaml")

    assert main(["--store-root", str(store_root), "create-universe-snapshot", "--name", "core", "--version", "v1", "--input", str(source)]) == 0
    universe_id = list_universe_snapshots(store_root=store_root)[0]["universe_snapshot_id"]
    assert main(["--store-root", str(store_root), "validate-universe-snapshot", "--universe-snapshot-id", universe_id]) == 0
    assert main(
        [
            "--store-root",
            str(store_root),
            "create-empty-dataset-snapshot",
            "--dataset-type",
            "ohlcv",
            "--provider",
            "placeholder",
            "--interval",
            "1d",
            "--bar-policy",
            "bp_v1",
            "--universe-snapshot-id",
            universe_id,
            "--start",
            "2005-01-01",
            "--end",
            "2026-05-18",
        ]
    ) == 0
    dataset_id = list_dataset_snapshots(store_root=store_root)[0]["dataset_snapshot_id"]
    assert main(["--store-root", str(store_root), "validate-dataset-snapshot", "--dataset-snapshot-id", dataset_id]) == 0

    actions = [event["action"] for event in audit_events(store_root=store_root)]
    assert actions == [
        "universe_snapshot_created",
        "universe_snapshot_validated",
        "dataset_snapshot_created",
        "dataset_snapshot_validated",
    ]
