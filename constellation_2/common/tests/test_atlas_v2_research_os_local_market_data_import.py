from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.local_market_data_import import build_market_data_import_report, discover_local_market_data_files


def _write_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-01-01T09:30:00,100,101,99,100.5,1000\n"
        "2026-01-01T10:00:00,100.5,102,100,101,1200\n",
        encoding="utf-8",
    )


def test_discover_local_market_data_files_supports_expected_paths(tmp_path: Path) -> None:
    _write_csv(tmp_path / "data" / "cache" / "QQQ_30m.csv")
    _write_csv(tmp_path / "data" / "historical" / "TLT_daily.csv")

    files = discover_local_market_data_files(tmp_path)

    assert [(row.symbol, row.timeframe) for row in files] == [("QQQ", "30m"), ("TLT", "daily")]


def test_market_data_import_report_validates_discovered_csvs(tmp_path: Path) -> None:
    _write_csv(tmp_path / "data" / "cache" / "DIA_5m.csv")

    report = build_market_data_import_report(root=tmp_path, base_dir=tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["files_discovered"] == 1
    assert report["summary"]["schema_valid_files"] == 1
    assert report["summary"]["symbols_available"] == ["DIA"]
    assert report["authority_boundary"]["live_trading_authorized"] is False
