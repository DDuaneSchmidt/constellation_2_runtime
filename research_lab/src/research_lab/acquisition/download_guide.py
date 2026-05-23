from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.acquisition.csv_readiness import build_csv_readiness_report
from research_lab.acquisition.expected_files import expected_csv_files_report
from research_lab.acquisition.manual_sources import (
    MANUAL_SOURCE_GUIDANCE,
    MINIMUM_REQUIRED_COUNT,
    MINIMUM_VIABLE_SYMBOLS,
    SYMBOL_SOURCE_HINTS,
)
from research_lab.workflows.first_dataset_workflow import create_or_reuse_minimum_viable_universe


def checklist_markdown(*, csv_root: Path) -> str:
    rows = [
        "# Minimum Viable ETF CSV Checklist",
        "",
        "Need at least 3 real CSV files.",
        "",
    ]
    rows.extend(f"- [ ] {symbol}.csv" for symbol in MINIMUM_VIABLE_SYMBOLS)
    rows.extend(
        [
            "",
            "Place files in:",
            str(csv_root),
            "",
            "Then run:",
            f"research_lab/.venv/bin/python -m research_lab.cli first-dataset-status --csv-root {csv_root}",
            "",
        ]
    )
    return "\n".join(rows)


def write_download_checklist(*, csv_root: Path) -> Path:
    csv_root.mkdir(parents=True, exist_ok=True)
    path = csv_root / "download_checklist.md"
    path.write_text(checklist_markdown(csv_root=csv_root), encoding="utf-8")
    return path


def csv_download_guide(*, universe: str, csv_root: Path, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    if universe != "local_etf_minimum_viable_v1":
        raise ValueError("Packet 4D supports only --universe local_etf_minimum_viable_v1")
    snapshot = create_or_reuse_minimum_viable_universe(store_root=store_root, actor=actor)
    expected = expected_csv_files_report(
        universe_snapshot_id=snapshot["universe_snapshot_id"],
        csv_root=csv_root,
        store_root=store_root,
    )
    checklist_path = write_download_checklist(csv_root=csv_root)
    symbol_rows = []
    present = set(expected["present_symbols"])
    for symbol in MINIMUM_VIABLE_SYMBOLS:
        hints = SYMBOL_SOURCE_HINTS[symbol]
        symbol_rows.append(
            {
                "symbol": symbol,
                "expected_file": hints["expected_file"],
                "present": symbol in present,
                "path": str(csv_root / hints["expected_file"]),
                "stooq_symbol_hint": hints["stooq_symbol_hint"],
                "yahoo_symbol_hint": hints["yahoo_symbol_hint"],
            }
        )
    return {
        "universe": universe,
        "universe_snapshot_id": snapshot["universe_snapshot_id"],
        "csv_root": str(csv_root.resolve()),
        "required_minimum": f"At least {MINIMUM_REQUIRED_COUNT} of: {', '.join(MINIMUM_VIABLE_SYMBOLS)}",
        "preferred_full_set": MINIMUM_VIABLE_SYMBOLS,
        "expected_filenames": [f"{symbol}.csv" for symbol in MINIMUM_VIABLE_SYMBOLS],
        "current_status": symbol_rows,
        "missing_symbols": [row["symbol"] for row in symbol_rows if not row["present"]],
        "manual_acquisition_guidance": MANUAL_SOURCE_GUIDANCE,
        "after_download_commands": [
            f"research_lab/.venv/bin/python -m research_lab.cli first-dataset-status --csv-root {csv_root}",
            f"research_lab/.venv/bin/python -m research_lab.cli validate-local-csvs --universe-snapshot-id {snapshot['universe_snapshot_id']} --csv-root {csv_root} --start 2015-01-01 --end 2025-12-31 --allow-missing-symbols true",
            f"research_lab/.venv/bin/python -m research_lab.cli build-from-present-csvs --universe local_etf_minimum_viable_v1 --csv-root {csv_root} --start 2015-01-01 --end 2025-12-31",
        ],
        "download_checklist_path": str(checklist_path),
        "no_automated_scraping": True,
        "no_fake_data_generated": True,
        "schema_version": "csv_download_guide.v1",
    }


def verify_downloaded_csvs(*, universe: str, csv_root: Path, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    if universe != "local_etf_minimum_viable_v1":
        raise ValueError("Packet 4D supports only --universe local_etf_minimum_viable_v1")
    snapshot = create_or_reuse_minimum_viable_universe(store_root=store_root, actor=actor)
    readiness = build_csv_readiness_report(
        universe_snapshot_id=snapshot["universe_snapshot_id"],
        csv_root=csv_root,
        start="2015-01-01",
        end="2025-12-31",
        allow_missing_symbols=True,
        store_root=store_root,
    )
    invalid_files = [
        {"symbol": row["symbol"], "path": row["path"], "errors": row.get("errors", [])}
        for row in readiness["per_symbol"]
        if row.get("exists") and not row.get("valid")
    ]
    valid_symbol_count = len(readiness["symbols_present"])
    ready = valid_symbol_count >= MINIMUM_REQUIRED_COUNT and not invalid_files
    next_command = (
        f"research_lab/.venv/bin/python -m research_lab.cli build-from-present-csvs --universe local_etf_minimum_viable_v1 --csv-root {csv_root} --start 2015-01-01 --end 2025-12-31"
        if ready
        else f"research_lab/.venv/bin/python -m research_lab.cli csv-download-guide --universe local_etf_minimum_viable_v1 --csv-root {csv_root}"
    )
    return {
        "universe": universe,
        "universe_snapshot_id": snapshot["universe_snapshot_id"],
        "csv_root": str(csv_root.resolve()),
        "ready_for_dataset_build": ready,
        "valid_symbol_count": valid_symbol_count,
        "minimum_required_count": MINIMUM_REQUIRED_COUNT,
        "valid_symbols": readiness["symbols_present"],
        "missing_symbols": readiness["symbols_missing"],
        "invalid_files": invalid_files,
        "next_command": next_command,
        "csv_readiness_report": readiness,
        "no_automated_scraping": True,
        "no_fake_data_generated": True,
        "schema_version": "downloaded_csv_verification.v1",
    }

