from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from research_lab.acquisition.expected_files import is_template_file
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.manifest_io import write_json
from research_lab.universes.universe_registry import load_universe_snapshot


def import_local_csvs(
    *,
    source_dir: Path,
    csv_root: Path,
    universe_snapshot_id: str,
    mode: str,
    overwrite: bool = False,
    store_root: Path | None = None,
) -> dict[str, Any]:
    if mode not in {"copy", "dry-run"}:
        raise ValueError("mode must be copy or dry-run")
    universe = load_universe_snapshot(universe_snapshot_id, store_root=store_root)
    expected_symbols = sorted(str(row["symbol"]).upper() for row in universe["symbols"])
    expected = set(expected_symbols)
    csv_root.mkdir(parents=True, exist_ok=True)
    copied: list[dict[str, Any]] = []
    skipped_existing: list[dict[str, Any]] = []
    unmatched_source_files: list[str] = []
    template_files_ignored: list[str] = []
    for path in sorted(source_dir.glob("*.csv")):
        if is_template_file(path):
            template_files_ignored.append(path.name)
            continue
        symbol = path.stem.upper()
        if symbol not in expected:
            unmatched_source_files.append(path.name)
            continue
        destination = csv_root / f"{symbol}.csv"
        if destination.exists() and not overwrite:
            skipped_existing.append({"symbol": symbol, "source": str(path), "destination": str(destination), "reason": "destination_exists"})
            continue
        copied.append({"symbol": symbol, "source": str(path), "destination": str(destination), "would_copy": mode == "dry-run"})
        if mode == "copy":
            shutil.copy2(path, destination)
    present_after = sorted(path.stem.upper() for path in csv_root.glob("*.csv") if not is_template_file(path))
    missing_expected_symbols = sorted(expected - set(present_after))
    report = {
        "source_dir": str(source_dir.resolve()),
        "csv_root": str(csv_root.resolve()),
        "universe_snapshot_id": universe_snapshot_id,
        "mode": mode,
        "overwrite": overwrite,
        "copied": copied,
        "skipped_existing": skipped_existing,
        "unmatched_source_files": unmatched_source_files,
        "template_files_ignored": template_files_ignored,
        "missing_expected_symbols_after_import": missing_expected_symbols,
        "created_at": utc_now_iso(),
        "schema_version": "local_csv_import_report.v1",
    }
    write_json(csv_root / "import_report.json", report, overwrite=True)
    return report

