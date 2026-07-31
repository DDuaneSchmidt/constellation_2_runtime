from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.holdout_event_row_builder_design import (
    CANONICAL_SCHEMA,
    build_holdout_event_row_builder_design,
    run_holdout_event_row_builder_design,
)

NOW = "2026-06-06T00:00:00Z"
REQUIRED_FIELDS = {
    "event_id",
    "family_id",
    "candidate_id",
    "timestamp",
    "date",
    "symbol",
    "timeframe",
    "mechanism",
    "regime",
    "source",
    "trigger_observed",
    "return_observed",
    "return_window",
    "split_date",
    "data_source",
    "created_by",
    "evidence_status",
    "notes",
}


def test_canonical_schema_includes_all_required_fields() -> None:
    fields = {row["field_name"] for row in CANONICAL_SCHEMA}

    assert REQUIRED_FIELDS == fields
    status_row = next(row for row in CANONICAL_SCHEMA if row["field_name"] == "evidence_status")
    assert "MISSING_RETURN_OBSERVED" in status_row["allowed_values"]
    assert "MISSING_SPLIT_DATE" in status_row["allowed_values"]


def test_targeted_families_loaded_from_build_092_and_missing_rows_emit_p0(tmp_path: Path) -> None:
    _seed_build_092(tmp_path)

    report = build_holdout_event_row_builder_design(root=tmp_path, created_at=NOW)

    assert report["summary"]["target_families"] == 9
    assert len(report["target_families"]) == 9
    assert report["summary"]["candidate_requirements"] == 9
    assert report["summary"]["blocked_requirements"] == 9
    assert {row["priority"] for row in report["holdout_event_row_requirements"]} == {"P0"}
    assert all("return_observed" in row["missing_fields"] for row in report["holdout_event_row_requirements"])


def test_compatible_import_rows_are_detected_when_present(tmp_path: Path) -> None:
    _seed_build_092(tmp_path)
    _write_json(
        tmp_path / "holdout_event_rows" / "latest.json",
        {"holdout_event_rows": [_valid_row("family_00", "candidate_00")]},
    )

    report = build_holdout_event_row_builder_design(root=tmp_path, created_at=NOW)
    matrix = {Path(row["source_file"]).parent.name: row for row in report["import_validation_matrix"]}

    assert matrix["holdout_event_rows"]["validation_status"] == "VALID_IMPORT_AVAILABLE"
    assert matrix["holdout_event_rows"]["rows_valid"] == 1
    assert report["summary"]["compatible_import_rows"] == 1
    assert report["summary"]["blocked_requirements"] == 8


def test_rows_missing_return_observed_are_rejected(tmp_path: Path) -> None:
    _seed_build_092(tmp_path)
    row = _valid_row("family_00", "candidate_00")
    row.pop("return_observed")
    _write_json(tmp_path / "holdout_event_rows" / "latest.json", {"rows": [row]})

    report = build_holdout_event_row_builder_design(root=tmp_path, created_at=NOW)
    matrix = {Path(row["source_file"]).parent.name: row for row in report["import_validation_matrix"]}

    assert matrix["holdout_event_rows"]["validation_status"] == "SCHEMA_MISMATCH"
    assert matrix["holdout_event_rows"]["rows_valid"] == 0
    assert "return_observed" in matrix["holdout_event_rows"]["missing_required_fields"]


def test_rows_missing_split_date_are_flagged(tmp_path: Path) -> None:
    _seed_build_092(tmp_path)
    row = _valid_row("family_00", "candidate_00")
    row.pop("split_date")
    _write_json(tmp_path / "holdout_event_rows" / "latest.json", {"rows": [row]})

    report = build_holdout_event_row_builder_design(root=tmp_path, created_at=NOW)
    matrix = {Path(row["source_file"]).parent.name: row for row in report["import_validation_matrix"]}

    assert matrix["holdout_event_rows"]["validation_status"] == "SCHEMA_MISMATCH"
    assert "split_date" in matrix["holdout_event_rows"]["missing_required_fields"]


def test_no_replay_confidence_or_forbidden_authority_is_emitted(tmp_path: Path) -> None:
    _seed_build_092(tmp_path)

    report = build_holdout_event_row_builder_design(root=tmp_path, created_at=NOW)

    assert report["execution_boundary"]["holdout_validation_executed"] is False
    assert report["execution_boundary"]["holdout_replay_called"] is False
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["confidence_impact"] == "NONE"
    assert not _has_forbidden_true(report)


def test_required_reports_are_written_and_cli_prints_summary(tmp_path: Path, capsys) -> None:
    _seed_build_092(tmp_path)

    report = run_holdout_event_row_builder_design(root=tmp_path, created_at=NOW)
    rc = main(["--root", str(tmp_path), "--holdout-event-row-builder-design"])
    captured = capsys.readouterr()
    printed = json.loads(captured.out.strip().splitlines()[-1])

    assert report["summary"]["target_families"] == 9
    assert rc == 0
    assert printed["target_families"] == 9
    assert printed["confidence_impact"] == "NONE"
    for name in (
        "latest.json",
        "latest_summary.md",
        "holdout_event_schema.csv",
        "holdout_event_builder_plan.md",
        "holdout_event_row_requirements.csv",
        "holdout_import_validation_matrix.csv",
    ):
        assert (tmp_path / "holdout_event_row_builder_design" / name).exists(), name
    rows = list(csv.DictReader((tmp_path / "holdout_event_row_builder_design" / "holdout_event_schema.csv").open()))
    assert {row["field_name"] for row in rows} == REQUIRED_FIELDS


def _seed_build_092(root: Path) -> None:
    matrix = []
    for index in range(9):
        matrix.append(
            {
                "family_id": f"family_{index:02d}",
                "family_name": f"BREAKOUT / CHOP / 30M / TEST {index}",
                "candidate_count": 1,
                "candidate_ids": [f"candidate_{index:02d}"],
                "mechanism": "BREAKOUT",
                "regime": "CHOP",
                "timeframes": ["30M"],
                "source_types": ["TEST"],
                "target_reasons": ["BUILD_092_TARGET"],
                "readiness_status": "BLOCKED_DATA",
            }
        )
    _write_json(
        root / "holdout_readiness_audit" / "latest.json",
        {
            "report_type": "HOLDOUT_READINESS_AUDIT",
            "summary": {"families_audited": 9, "confidence_impact": "NONE"},
            "readiness_matrix": matrix,
        },
    )


def _valid_row(family_id: str, candidate_id: str) -> dict[str, object]:
    return {
        "event_id": "event-001",
        "family_id": family_id,
        "candidate_id": candidate_id,
        "timestamp": "2024-03-15T14:30:00Z",
        "date": "2024-03-15",
        "symbol": "SPY",
        "timeframe": "30M",
        "mechanism": "BREAKOUT",
        "regime": "CHOP",
        "source": "TEST",
        "trigger_observed": True,
        "return_observed": 0.0125,
        "return_window": "next_5_bars",
        "split_date": "2024-01-01",
        "data_source": "unit_test",
        "created_by": "unit_test",
        "evidence_status": "IMPORT_READY",
        "notes": "test row",
    }


def _has_forbidden_true(report: dict[str, object]) -> bool:
    text = json.dumps(report, sort_keys=True)
    forbidden_terms = [
        "live_trading_authorized",
        "broker_execution_authorized",
        "capital_allocation_authorized",
        "position_sizing_authorized",
        "trade_recommendation_authorized",
        "automatic_paper_placement_authorized",
        "candidate_promotion_authorized",
        "production_promotion_authorized",
    ]
    authority = report["authority_boundary"]
    assert all(term in text for term in forbidden_terms)
    return any(authority.get(term) is True for term in forbidden_terms)  # type: ignore[union-attr]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
