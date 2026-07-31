from __future__ import annotations

import csv
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.family_observation_log import CSV_FIELDS
from constellation_2.common.atlas_v2_research_os.family_observation_review import (
    build_weekly_family_observation_review,
    render_weekly_family_observation_review_summary,
)


def _write_log(path: Path) -> None:
    rows = [
        {
            "date": "2026-06-01",
            "family_id": "family-a",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "timeframe": "30M",
            "symbol_or_universe": "SPY",
            "observation_condition_met": "true",
            "invalidation_condition_met": "false",
            "paper_outcome": "supporting",
            "return_observed": "0.4%",
            "notes": "",
            "reviewer": "David",
        },
        {
            "date": "2026-06-02",
            "family_id": "family-b",
            "mechanism": "MEAN_REVERSION",
            "regime": "VOL_COMPRESSION",
            "timeframe": "1H",
            "symbol_or_universe": "QQQ",
            "observation_condition_met": "false",
            "invalidation_condition_met": "true",
            "paper_outcome": "invalidating",
            "return_observed": "",
            "notes": "",
            "reviewer": "David",
        },
        {
            "date": "2026-06-03",
            "family_id": "family-c",
            "mechanism": "BREAKOUT",
            "regime": "TREND",
            "timeframe": "D",
            "symbol_or_universe": "SPY",
            "observation_condition_met": "",
            "invalidation_condition_met": "",
            "paper_outcome": "needs_data",
            "return_observed": "",
            "notes": "Needs data before confidence increase.",
            "reviewer": "David",
        },
        {field: "" for field in CSV_FIELDS} | {"family_id": "family-d"},
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def test_builds_weekly_family_observation_review_from_logged_rows(tmp_path: Path) -> None:
    path = tmp_path / "family_log.csv"
    _write_log(path)

    report = build_weekly_family_observation_review(path, week_ending="2026-06-05")

    assert report["observations_logged"] == 3
    assert report["sample_size_by_family"]["family-a"]["observations"] == 1
    assert report["sample_size_by_family"]["family-a"]["supporting"] == 1
    assert report["sample_size_by_family"]["family-b"]["invalidating"] == 1
    assert report["supporting_observations"] == 1
    assert report["invalidating_observations"] == 1
    assert report["families_strengthened"] == ["family-a"]
    assert report["families_weakened"] == ["family-b"]
    assert report["families_needing_data"] == ["family-c"]
    assert report["authority_boundary"]["trade_execution_authorized"] is False


def test_renders_required_weekly_review_sections(tmp_path: Path) -> None:
    path = tmp_path / "family_log.csv"
    _write_log(path)
    report = build_weekly_family_observation_review(path, week_ending="2026-06-05")

    summary = render_weekly_family_observation_review_summary(report)

    for required in (
        "## Observations Logged",
        "## Sample Size By Family",
        "## Supporting Observations",
        "## Invalidating Observations",
        "## Families Strengthened",
        "## Families Weakened",
        "## Families Needing Data",
        "## Families To Retire",
        "No trade execution",
    ):
        assert required in summary
