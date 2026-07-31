from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.paper_forward_observation_log import (
    DEFAULT_FIELDS,
    audit_observation_log_templates,
    build_observation_log_template,
    write_observation_log_template,
)


def _write_candidate_review(root: Path) -> None:
    (root / "candidate_review").mkdir(parents=True)
    rows = [
        {"candidate_id": "ptc-1", "mechanism": "BREAKOUT"},
        {"candidate_id": "ptc-2", "mechanism": "MEAN_REVERSION"},
    ]
    (root / "candidate_review" / "latest.json").write_text(
        json.dumps({"day": "2026-06-05", "top_candidates": rows}),
        encoding="utf-8",
    )


def test_builds_manual_observation_log_template_from_candidate_review(tmp_path: Path) -> None:
    _write_candidate_review(tmp_path)
    payload = build_observation_log_template(tmp_path, day="2026-06-05")

    assert payload["fields"] == DEFAULT_FIELDS
    assert payload["candidate_count"] == 2
    assert payload["rows"][0]["candidate_id"] == "ptc-1"
    assert payload["rows"][0]["ticker_or_symbol"] == ""
    assert payload["authority_boundary"]["broker_execution_authorized"] is False
    assert payload["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False
    assert payload["authority_boundary"]["live_trading_authorized"] is False
    assert payload["authority_boundary"]["capital_authorized"] is False


def test_writes_csv_json_and_instructions(tmp_path: Path) -> None:
    _write_candidate_review(tmp_path)
    paths = write_observation_log_template(tmp_path, day="2026-06-05")

    assert paths["csv"].exists()
    assert paths["json"].exists()
    assert paths["instructions"].exists()
    assert paths["latest_csv"].exists()
    assert paths["latest_instructions"].exists()
    with paths["csv"].open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == DEFAULT_FIELDS
        rows = list(reader)
    assert rows[1]["candidate_id"] == "ptc-2"
    assert "No broker execution." in paths["instructions"].read_text(encoding="utf-8")
    assert audit_observation_log_templates(tmp_path)["paper_forward_observation_log_audit_ok"] is True
