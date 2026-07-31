from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_event_row_materializer import (
    CANONICAL_COLUMNS,
    INCOMPLETE_COLUMNS,
    MATERIALIZATION_MATRIX_COLUMNS,
    SOURCE_SCAN_COLUMNS,
    build_holdout_event_row_materializer,
    deterministic_event_id,
    write_holdout_event_row_materializer,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_build_096(root: Path) -> None:
    source = root / "holdout_event_row_builder_design"
    source.mkdir(parents=True, exist_ok=True)
    _write_csv(
        source / "holdout_event_schema.csv",
        ["field_name", "required", "type", "description", "allowed_values", "example"],
        [{"field_name": field, "required": "yes", "type": "string", "description": "", "allowed_values": "", "example": ""} for field in CANONICAL_COLUMNS],
    )
    _write_csv(
        source / "holdout_event_row_requirements.csv",
        [
            "family_id",
            "candidate_id",
            "required_symbol",
            "required_timeframe",
            "required_regime",
            "required_mechanism",
            "required_source",
            "required_start",
            "required_end",
            "required_return_window",
            "row_status",
            "missing_fields",
            "priority",
        ],
        [
            {
                "family_id": "family_55443d63b32328bd",
                "candidate_id": "ptc_backtest_final_4df2e8e80685a054",
                "required_symbol": "FAMILY_OR_CANDIDATE_SYMBOL_REQUIRED",
                "required_timeframe": "1H",
                "required_regime": "TRENDING",
                "required_mechanism": "REVERSAL",
                "required_source": "SCREEN_REPLAY",
                "required_start": "UNSPECIFIED_EVENT_IMPORT_START",
                "required_end": "UNSPECIFIED_EVENT_IMPORT_END",
                "required_return_window": "FROZEN_HOLDOUT_RETURN_WINDOW_REQUIRED",
                "row_status": "BLOCKED",
                "missing_fields": "event_id,return_observed,split_date",
                "priority": "P0",
            }
        ],
    )
    _write_csv(
        source / "holdout_import_validation_matrix.csv",
        ["source_file", "rows_found", "rows_valid", "rows_invalid", "families_matched", "candidates_matched", "missing_required_fields", "validation_status", "notes"],
        [],
    )
    (source / "latest.json").write_text(json.dumps({"summary": {"target_families": 1, "blocked_requirements": 1}}), encoding="utf-8")


def _seed_source_artifacts(root: Path) -> None:
    directory = root / "direct_candidate_data_validation"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "latest.json").write_text(
        json.dumps(
            {
                "candidate_validations": [
                    {
                        "candidate_id": "ptc_backtest_final_4df2e8e80685a054",
                        "timestamp": "2024-03-15T14:30:00+00:00",
                        "symbol": "BAC",
                        "trigger_observed": True,
                    },
                    {
                        "candidate_id": "not_a_target",
                        "timestamp": "2024-03-16T14:30:00+00:00",
                        "symbol": "SPY",
                        "return_observed": "0.01",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    historical = root / "historical_replay"
    historical.mkdir(parents=True, exist_ok=True)
    (historical / "latest.json").write_text(
        json.dumps(
            {
                "events": [
                    {
                        "event_id": "source_evt_1",
                        "family_id": "family_55443d63b32328bd",
                        "timestamp": "2024-04-01T14:30:00+00:00",
                        "symbol": "BAC",
                        "regime": "TRENDING",
                        "return_observed": "0.012",
                        "return_window": "next_5_bars",
                        "split_date": "2024-01-01",
                        "trigger_observed": True,
                    },
                    {
                        "timestamp": "2024-04-02T14:30:00+00:00",
                        "return_observed": "0.02",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


def test_materializer_loads_build_096_and_target_families(tmp_path: Path) -> None:
    _seed_build_096(tmp_path)
    _seed_source_artifacts(tmp_path)

    report = build_holdout_event_row_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["schema_fields_loaded"] == CANONICAL_COLUMNS
    assert report["target_family_ids"] == ["family_55443d63b32328bd"]
    assert report["target_candidate_ids"] == ["ptc_backtest_final_4df2e8e80685a054"]
    assert report["summary"]["families_targeted"] == 1
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["confidence_impact"] == "NONE"


def test_materializer_only_keys_rows_and_does_not_fabricate_missing_fields(tmp_path: Path) -> None:
    _seed_build_096(tmp_path)
    _seed_source_artifacts(tmp_path)

    report = build_holdout_event_row_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")
    rows = report["materialized_holdout_event_rows"]

    assert len(rows) == 2
    assert all(row["candidate_id"] or row["family_id"] for row in rows)
    assert not any(row["candidate_id"] == "not_a_target" for row in rows)
    incomplete = next(row for row in rows if row["candidate_id"] == "ptc_backtest_final_4df2e8e80685a054")
    assert incomplete["return_observed"] == ""
    assert incomplete["split_date"] == ""
    assert incomplete["date"] == "2024-03-15"
    assert "return_observed" in next(row for row in report["incomplete_event_rows"] if row["event_id"] == incomplete["event_id"])["missing_fields"]
    complete = next(row for row in rows if row["event_id"] == "source_evt_1")
    assert complete["evidence_status"] == "MATERIALIZED_COMPLETE"


def test_materializer_event_id_is_stable_and_no_replay_authority(tmp_path: Path) -> None:
    _seed_build_096(tmp_path)
    _seed_source_artifacts(tmp_path)

    first = build_holdout_event_row_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")
    second = build_holdout_event_row_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")
    first_ids = [row["event_id"] for row in first["materialized_holdout_event_rows"]]
    second_ids = [row["event_id"] for row in second["materialized_holdout_event_rows"]]

    assert first_ids == second_ids
    assert deterministic_event_id("family", "candidate", "2024-01-01", "SPY", "1H", "SCREEN") == deterministic_event_id(
        "family", "candidate", "2024-01-01", "SPY", "1H", "SCREEN"
    )
    assert not any("uuid" in event_id.lower() for event_id in first_ids)
    assert first["authority_boundary"]["research_only"] is True
    assert not any(key.endswith("_authorized") for key in first["authority_boundary"])
    assert "holdout replay execution" in first["authority_boundary"]["forbidden_actions"]
    assert "holdout validation execution" in first["authority_boundary"]["forbidden_actions"]


def test_materializer_writes_required_outputs_and_columns(tmp_path: Path) -> None:
    _seed_build_096(tmp_path)
    _seed_source_artifacts(tmp_path)
    report = build_holdout_event_row_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_holdout_event_row_materializer(report, root=tmp_path)

    for key in ["latest_json", "latest_summary", "materialized_rows", "materialization_matrix", "incomplete_rows", "source_artifact_scan"]:
        assert paths[key].exists()
    with paths["materialized_rows"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == CANONICAL_COLUMNS
    with paths["materialization_matrix"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == MATERIALIZATION_MATRIX_COLUMNS
    with paths["incomplete_rows"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == INCOMPLETE_COLUMNS
    with paths["source_artifact_scan"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == SOURCE_SCAN_COLUMNS

    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "# Build 100 — Holdout Event Row Materializer" in summary
    assert "## What This Build Does Not Do" in summary
