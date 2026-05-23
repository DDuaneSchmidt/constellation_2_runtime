from __future__ import annotations

from pathlib import Path

import yaml

from research_lab.acquisition.csv_template import create_csv_template
from research_lab.acquisition.expected_files import expected_csv_files_report
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
                    {"symbol": "QQQ", "asset_type": "ETF", "category": "equity", "active": True, "min_start_date": "2020-01-01", "notes": ""},
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="expected_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_expected_csv_files_reports_present_missing_extra_and_templates(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    (csv_root / "SPY.csv").write_text("date,open,high,low,close,adj_close,volume\n", encoding="utf-8")
    (csv_root / "EXTRA.csv").write_text("date,open,high,low,close,adj_close,volume\n", encoding="utf-8")
    create_csv_template(symbol="QQQ", output_path=csv_root / "QQQ_template.csv")

    report = expected_csv_files_report(universe_snapshot_id=_universe(tmp_path), csv_root=csv_root, store_root=tmp_path / "store")

    assert report["present_symbols"] == ["SPY"]
    assert report["missing_symbols"] == ["QQQ"]
    assert report["extra_files"] == ["EXTRA.csv"]
    assert report["template_files_ignored"] == ["QQQ_template.csv"]

