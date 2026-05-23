from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.acquisition.csv_template import CSV_TEMPLATE_COLUMNS, create_csv_template
from research_lab.datasets.bulk_local_csv_import import discover_local_csv_symbols


def test_csv_template_generator_writes_expected_columns(tmp_path: Path) -> None:
    output = tmp_path / "SPY_template.csv"

    result = create_csv_template(symbol="SPY", output_path=output)

    assert output.read_text(encoding="utf-8").splitlines()[0].split(",") == CSV_TEMPLATE_COLUMNS
    assert result["do_not_import_template"] is True


def test_dataset_discovery_ignores_template_files(tmp_path: Path) -> None:
    create_csv_template(symbol="SPY", output_path=tmp_path / "SPY_template.csv")
    (tmp_path / "SPY.csv").write_text("date,open,high,low,close,adj_close,volume\n", encoding="utf-8")

    assert discover_local_csv_symbols(tmp_path) == ["SPY"]


def test_csv_template_requires_template_suffix(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="template"):
        create_csv_template(symbol="SPY", output_path=tmp_path / "SPY.csv")

