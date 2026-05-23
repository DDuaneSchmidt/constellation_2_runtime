from __future__ import annotations

from pathlib import Path

from research_lab.acquisition.expected_files import is_template_file
from research_lab.providers.local_csv_daily import local_csv_root


def discover_local_csv_symbols(root: Path | None = None) -> list[str]:
    base = (root or local_csv_root()).resolve()
    if not base.exists():
        return []
    return sorted(path.stem.upper() for path in base.glob("*.csv") if path.is_file() and not is_template_file(path))


def local_csv_file_map(root: Path | None = None) -> dict[str, str]:
    base = (root or local_csv_root()).resolve()
    return {symbol: str(base / f"{symbol}.csv") for symbol in discover_local_csv_symbols(base)}


def missing_local_csv_symbols(symbols: list[str], root: Path | None = None) -> list[str]:
    available = set(discover_local_csv_symbols(root))
    return sorted({symbol.upper() for symbol in symbols} - available)
