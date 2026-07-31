from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.family_observation_log import (
    CSV_FIELDS,
    WEEKLY_REVIEW_FIELDS,
    audit_family_observation_log_templates,
    build_family_observation_log_template,
    write_family_observation_log_template,
)


def _write_family_robustness_review(root: Path) -> None:
    (root / "family_robustness_review").mkdir(parents=True)
    rows = [
        {
            "best_rank": 2,
            "classification": "PROMISING_BUT_DATA_BLOCKED",
            "family_id": "family-b",
            "family_name": "MEAN_REVERSION / VOL_COMPRESSION / 1H / JOURNAL_EXTRACT",
            "timeframe_concentration": {"timeframes": ["1H"]},
            "symbol_universe_concentration": {"symbols": ["QQQ"]},
        },
        {
            "best_rank": 1,
            "classification": "ROBUST_ENOUGH_TO_OBSERVE",
            "family_id": "family-a",
            "family_name": "BREAKOUT / CHOP / 30M / JOURNAL_EXTRACT",
            "timeframe_concentration": {"timeframes": ["30M"]},
            "symbol_universe_concentration": {"symbols": ["SPY", "IWM"]},
        },
    ]
    (root / "family_robustness_review" / "latest.json").write_text(
        json.dumps({"day": "2026-06-05", "family_reviews": rows}),
        encoding="utf-8",
    )


def test_builds_family_observation_log_template_from_robustness_review(tmp_path: Path) -> None:
    _write_family_robustness_review(tmp_path)
    payload = build_family_observation_log_template(tmp_path, day="2026-06-05")

    assert payload["fields"] == CSV_FIELDS
    assert payload["weekly_review_fields"] == WEEKLY_REVIEW_FIELDS
    assert payload["family_count"] == 2
    assert payload["rows"][0]["family_id"] == "family-a"
    assert payload["rows"][0]["mechanism"] == "BREAKOUT"
    assert payload["rows"][0]["regime"] == "CHOP"
    assert payload["rows"][0]["timeframe"] == "30M"
    assert payload["rows"][0]["symbol_or_universe"] == "SPY, IWM"
    assert payload["authority_boundary"]["manual_observation_only"] is True
    assert payload["authority_boundary"]["trade_execution_authorized"] is False
    assert payload["authority_boundary"]["broker_execution_authorized"] is False
    assert payload["authority_boundary"]["capital_authorized"] is False
    assert payload["authority_boundary"]["position_sizing_authorized"] is False


def test_writes_family_observation_outputs_and_latest_files(tmp_path: Path) -> None:
    _write_family_robustness_review(tmp_path)
    paths = write_family_observation_log_template(tmp_path, day="2026-06-05")

    assert paths["csv"].exists()
    assert paths["instructions"].exists()
    assert paths["weekly_review_template"].exists()
    assert paths["latest_csv"].exists()
    assert paths["latest_instructions"].exists()
    with paths["csv"].open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == CSV_FIELDS
        rows = list(reader)
    assert rows[0]["family_id"] == "family-a"
    instructions = paths["instructions"].read_text(encoding="utf-8")
    assert "Manual observation only." in instructions
    assert "No trade execution." in instructions
    assert "No broker actions." in instructions
    weekly = paths["weekly_review_template"].read_text(encoding="utf-8")
    for required in (
        "## Observations Logged",
        "## Sample Size By Family",
        "## Supporting Observations",
        "## Invalidating Observations",
        "## Families Strengthened",
        "## Families Weakened",
        "## Families Needing Data",
        "## Families To Retire",
    ):
        assert required in weekly
    assert audit_family_observation_log_templates(tmp_path)["family_observation_log_audit_ok"] is True
