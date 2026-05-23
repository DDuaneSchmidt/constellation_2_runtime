from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from research_lab.macro_events.macro_event_calendar import build_macro_event_calendar_snapshot, normalize_macro_event

REQUIRED_COLUMNS = ["event_date", "event_type", "event_name", "importance", "source", "notes"]


def load_macro_event_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [column for column in REQUIRED_COLUMNS if column not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"macro event CSV missing columns: {missing}")
        return [normalize_macro_event(dict(row)) for row in reader]


def import_macro_event_calendar(input_path: Path, *, created_by: str = "Aegis") -> tuple[dict[str, Any], list[dict[str, Any]]]:
    events = load_macro_event_csv(input_path)
    snapshot = build_macro_event_calendar_snapshot(events=events, source_path=str(input_path), created_by=created_by)
    return snapshot, sorted(events, key=lambda row: (row["event_date"], row["event_type"], row["event_name"]))
