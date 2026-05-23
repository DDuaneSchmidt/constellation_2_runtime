from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.storage.hashing import utc_now_iso
from research_lab.universes.universe_registry import load_universe_snapshot


def is_template_file(path: Path) -> bool:
    return path.name.lower().endswith("_template.csv")


def expected_csv_files_report(
    *,
    universe_snapshot_id: str,
    csv_root: Path,
    store_root: Path | None = None,
) -> dict[str, Any]:
    universe = load_universe_snapshot(universe_snapshot_id, store_root=store_root)
    csv_root = csv_root.resolve()
    expected_symbols = sorted(str(row["symbol"]).upper() for row in universe["symbols"])
    template_files = sorted(path.name for path in csv_root.glob("*.csv") if is_template_file(path)) if csv_root.exists() else []
    real_csvs = sorted(path for path in csv_root.glob("*.csv") if not is_template_file(path)) if csv_root.exists() else []
    real_symbols = {path.stem.upper(): path for path in real_csvs}
    rows = [
        {
            "symbol": symbol,
            "expected_filename": f"{symbol}.csv",
            "exists": symbol in real_symbols,
            "path": str(csv_root / f"{symbol}.csv"),
        }
        for symbol in expected_symbols
    ]
    extra_files = sorted(path.name for path in real_csvs if path.stem.upper() not in expected_symbols)
    present_symbols = [row["symbol"] for row in rows if row["exists"]]
    missing_symbols = [row["symbol"] for row in rows if not row["exists"]]
    return {
        "universe_snapshot_id": universe_snapshot_id,
        "csv_root": str(csv_root),
        "expected_files": rows,
        "present_symbols": present_symbols,
        "missing_symbols": missing_symbols,
        "extra_files": extra_files,
        "template_files_ignored": template_files,
        "created_at": utc_now_iso(),
        "schema_version": "expected_csv_files.v1",
    }

