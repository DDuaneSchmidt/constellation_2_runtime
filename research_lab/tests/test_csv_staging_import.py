from __future__ import annotations

from pathlib import Path

import yaml

from research_lab.acquisition.csv_staging import import_local_csvs
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


def _universe(tmp_path: Path) -> str:
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "fixture",
                "source_notes": "fixture",
                "symbols": [
                    {"symbol": "SPY", "asset_type": "ETF", "category": "equity", "active": True, "min_start_date": "2020-01-01", "notes": ""},
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="import_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_import_local_csvs_dry_run_does_not_copy_files(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "SPY.csv").write_text("date,open,high,low,close,adj_close,volume\n", encoding="utf-8")
    csv_root = tmp_path / "csv"

    report = import_local_csvs(source_dir=source, csv_root=csv_root, universe_snapshot_id=_universe(tmp_path), mode="dry-run", store_root=tmp_path / "store")

    assert report["copied"][0]["would_copy"] is True
    assert not (csv_root / "SPY.csv").exists()


def test_import_local_csvs_copy_normalizes_filenames_and_does_not_overwrite(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "spy.csv").write_text("new\n", encoding="utf-8")
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    (csv_root / "SPY.csv").write_text("old\n", encoding="utf-8")
    universe_id = _universe(tmp_path)

    no_overwrite = import_local_csvs(source_dir=source, csv_root=csv_root, universe_snapshot_id=universe_id, mode="copy", store_root=tmp_path / "store")
    assert no_overwrite["skipped_existing"][0]["symbol"] == "SPY"
    assert (csv_root / "SPY.csv").read_text(encoding="utf-8") == "old\n"

    overwrite = import_local_csvs(source_dir=source, csv_root=csv_root, universe_snapshot_id=universe_id, mode="copy", overwrite=True, store_root=tmp_path / "store")
    assert overwrite["copied"][0]["destination"].endswith("SPY.csv")
    assert (csv_root / "SPY.csv").read_text(encoding="utf-8") == "new\n"

