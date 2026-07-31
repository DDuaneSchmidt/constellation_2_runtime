from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.manual_intraday_csv_intake import (
    discover_manual_intraday_csvs,
    normalize_manual_intraday_csv,
    resample_1m_to_5m_and_30m,
    run_manual_intraday_csv_intake,
    validate_manual_intraday_schema,
    write_normalized_priority1_files,
)


def _write_intraday_csv(path: Path, *, rows: int = 40, start: datetime | None = None, symbol: str | None = None) -> None:
    start_dt = start or datetime(2026, 1, 2, 9, 30)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = ["timestamp", "open", "high", "low", "close", "volume"]
        if symbol is not None:
            fieldnames.append("symbol")
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        price = 100.0
        for index in range(rows):
            timestamp = start_dt + timedelta(minutes=index)
            row = {
                "timestamp": timestamp.isoformat(),
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price + 0.25,
                "volume": 1000 + index,
            }
            if symbol is not None:
                row["symbol"] = symbol
            writer.writerow(row)
            price += 0.1


def test_discover_manual_intraday_csvs_creates_drop_folder(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    files = discover_manual_intraday_csvs()

    assert files == []
    assert (tmp_path / "data" / "manual_intraday_import").is_dir()


def test_validate_and_write_direct_priority1_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manual = tmp_path / "data" / "manual_intraday_import" / "DIA_30m.csv"
    _write_intraday_csv(manual, rows=35, symbol="DIA")

    validation = validate_manual_intraday_schema(manual)
    written, resampled = write_normalized_priority1_files([validation])

    assert validation["status"] == "PASS"
    assert resampled == []
    assert written[0]["target_csv_path"].endswith("data/cache/DIA_30m.csv")
    with (tmp_path / "data" / "cache" / "DIA_30m.csv").open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == ["timestamp", "open", "high", "low", "close", "volume", "adjusted_close", "source_file"]
        assert len(list(reader)) == 35


def test_resample_1m_to_5m_and_30m_writes_priority_outputs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manual = tmp_path / "data" / "manual_intraday_import" / "QQQ_1m.csv"
    _write_intraday_csv(manual, rows=60, symbol="QQQ")
    rows, _warnings = normalize_manual_intraday_csv(manual, symbol="QQQ", timeframe="1m")

    resampled_rows = resample_1m_to_5m_and_30m(rows)
    validation = validate_manual_intraday_schema(manual)
    written, resampled = write_normalized_priority1_files([validation])

    assert len(resampled_rows["5m"]) == 12
    assert len(resampled_rows["30m"]) == 2
    assert {row["timeframe"] for row in written} == {"5m", "30m"}
    assert {Path(row["target_csv_path"]).name for row in resampled} == {"QQQ_5m.csv", "QQQ_30m.csv"}


def test_strict_validation_rejects_bad_ohlc_and_short_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bad = tmp_path / "data" / "manual_intraday_import" / "SPY_5m.csv"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-01-02T09:30:00,100,99,101,100,1000\n",
        encoding="utf-8",
    )

    validation = validate_manual_intraday_schema(bad)

    assert validation["status"] == "REJECTED"
    assert any("high < low" in error for error in validation["errors"])


def test_run_manual_intraday_csv_intake_without_files_writes_report(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    report = run_manual_intraday_csv_intake(root=tmp_path / "reports" / "atlas_v2_research_os", created_at="2026-06-05T00:00:00Z", base_dir=tmp_path)

    assert report["summary"]["files_discovered"] == 0
    assert report["summary"]["normalized_files_written"] == 0
    assert (tmp_path / "reports" / "atlas_v2_research_os" / "manual_intraday_csv_intake" / "latest.json").exists()
    assert report["authority_boundary"]["external_api_calls_authorized"] is False
