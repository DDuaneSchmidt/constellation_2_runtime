from __future__ import annotations

from pathlib import Path

from research_lab.acquisition.download_guide import csv_download_guide, verify_downloaded_csvs
from research_lab.acquisition.manual_sources import MINIMUM_VIABLE_SYMBOLS


def _write_csv(root: Path, symbol: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    root.joinpath(f"{symbol}.csv").write_text(
        "date,open,high,low,close,adj_close,volume\n"
        "2020-01-02,100,101,99,100.5,100.5,1000\n",
        encoding="utf-8",
    )


def test_csv_download_guide_lists_all_minimum_viable_symbols(tmp_path: Path) -> None:
    guide = csv_download_guide(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    assert guide["preferred_full_set"] == MINIMUM_VIABLE_SYMBOLS
    assert guide["expected_filenames"] == [f"{symbol}.csv" for symbol in MINIMUM_VIABLE_SYMBOLS]
    assert "stooq" in guide["manual_acquisition_guidance"]
    assert guide["no_automated_scraping"] is True
    assert guide["no_fake_data_generated"] is True


def test_csv_download_guide_marks_present_and_missing_files_correctly(tmp_path: Path) -> None:
    _write_csv(tmp_path / "csv", "SPY")

    guide = csv_download_guide(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    status = {row["symbol"]: row["present"] for row in guide["current_status"]}
    assert status["SPY"] is True
    assert status["QQQ"] is False
    assert "QQQ" in guide["missing_symbols"]


def test_download_checklist_is_generated(tmp_path: Path) -> None:
    guide = csv_download_guide(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    checklist = Path(guide["download_checklist_path"])
    assert checklist.exists()
    text = checklist.read_text(encoding="utf-8")
    assert "# Minimum Viable ETF CSV Checklist" in text
    assert "- [ ] SPY.csv" in text


def test_verify_downloaded_csvs_blocks_when_fewer_than_3_valid_files_exist(tmp_path: Path) -> None:
    _write_csv(tmp_path / "csv", "SPY")
    _write_csv(tmp_path / "csv", "QQQ")

    result = verify_downloaded_csvs(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    assert result["ready_for_dataset_build"] is False
    assert result["valid_symbol_count"] == 2
    assert "build-from-present-csvs" not in result["next_command"]


def test_verify_downloaded_csvs_passes_when_3_valid_fixture_files_exist(tmp_path: Path) -> None:
    for symbol in ["SPY", "QQQ", "IWM"]:
        _write_csv(tmp_path / "csv", symbol)

    result = verify_downloaded_csvs(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    assert result["ready_for_dataset_build"] is True
    assert result["valid_symbol_count"] == 3
    assert "build-from-present-csvs" in result["next_command"]


def test_download_guide_commands_do_not_generate_fake_ohlcv_data(tmp_path: Path) -> None:
    csv_download_guide(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    assert not list((tmp_path / "csv").glob("*.csv"))


def test_download_guide_does_not_bypass_captcha_or_api_key_restrictions(tmp_path: Path) -> None:
    guide = csv_download_guide(
        universe="local_etf_minimum_viable_v1",
        csv_root=tmp_path / "csv",
        store_root=tmp_path / "store",
    )

    source_text = " ".join(" ".join(lines) for lines in guide["manual_acquisition_guidance"].values())
    assert "does not bypass CAPTCHA" in source_text
    assert "does not implement automated Yahoo scraping" in source_text

