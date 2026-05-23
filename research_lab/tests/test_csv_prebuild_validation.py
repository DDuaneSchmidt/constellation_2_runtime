from __future__ import annotations

from pathlib import Path

from research_lab.datasets.csv_validation import validate_local_csv_file


def _write(path: Path, rows: list[str], header: str = "date,open,high,low,close,adj_close,volume") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_validate_local_csvs_passes_clean_fixture_csv(tmp_path: Path) -> None:
    path = tmp_path / "SPY.csv"
    _write(path, ["2024-01-02,100,101,99,100.5,100.5,1000"])

    result = validate_local_csv_file(symbol="SPY", path=path, start="2024-01-01", end="2024-01-31")

    assert result["valid"] is True
    assert result["row_count"] == 1


def test_validate_local_csvs_blocks_duplicate_dates(tmp_path: Path) -> None:
    path = tmp_path / "SPY.csv"
    _write(path, ["2024-01-02,100,101,99,100.5,100.5,1000", "2024-01-02,100,101,99,100.5,100.5,1000"])

    result = validate_local_csv_file(symbol="SPY", path=path, start="2024-01-01", end="2024-01-31")

    assert result["valid"] is False
    assert "duplicate_dates" in result["errors"]


def test_validate_local_csvs_blocks_invalid_ohlc(tmp_path: Path) -> None:
    path = tmp_path / "SPY.csv"
    _write(path, ["2024-01-02,100,99,101,100.5,100.5,1000"])

    result = validate_local_csv_file(symbol="SPY", path=path, start="2024-01-01", end="2024-01-31")

    assert result["valid"] is False
    assert "invalid_ohlc_structure" in result["errors"]


def test_validate_local_csvs_warns_when_adj_close_missing(tmp_path: Path) -> None:
    path = tmp_path / "SPY.csv"
    _write(path, ["2024-01-02,100,101,99,100.5,1000"], header="timestamp,open,high,low,close,volume")

    result = validate_local_csv_file(symbol="SPY", path=path, start="2024-01-01", end="2024-01-31")

    assert result["valid"] is True
    assert "adj_close_missing_will_use_close" in result["warnings"]

