from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_event_row_readiness_recheck import (
    CANDIDATE_GAP_COLUMNS,
    FAMILY_GAP_COLUMNS,
    MISSING_FIELD_COLUMNS,
    READINESS_MATRIX_COLUMNS,
    build_holdout_event_row_readiness_recheck,
    classify_readiness,
    write_holdout_event_row_readiness_recheck,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_inputs(root: Path) -> None:
    materializer = root / "holdout_event_row_materializer"
    materializer.mkdir(parents=True, exist_ok=True)
    event_columns = [
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
    ]
    _write_csv(
        materializer / "materialized_holdout_event_rows.csv",
        event_columns,
        [
            {
                "event_id": "evt_complete",
                "family_id": "family_ready",
                "candidate_id": "candidate_ready",
                "timestamp": "2024-03-01T14:30:00+00:00",
                "date": "2024-03-01",
                "symbol": "SPY",
                "timeframe": "30M",
                "mechanism": "BREAKOUT",
                "regime": "CHOP",
                "source": "JOURNAL_EXTRACT",
                "trigger_observed": "true",
                "return_observed": "0.01",
                "return_window": "next_5_bars",
                "split_date": "2024-01-01",
                "data_source": "fixture",
                "created_by": "test",
                "evidence_status": "MATERIALIZED_COMPLETE",
                "notes": "",
            },
            {
                "event_id": "evt_incomplete",
                "family_id": "",
                "candidate_id": "candidate_blocked",
                "timestamp": "",
                "date": "",
                "symbol": "",
                "timeframe": "1H",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "source": "SCREEN_REPLAY",
                "trigger_observed": "true",
                "return_observed": "",
                "return_window": "",
                "split_date": "",
                "data_source": "fixture",
                "created_by": "test",
                "evidence_status": "MATERIALIZED_INCOMPLETE",
                "notes": "missing_fields=timestamp,date,symbol,return_observed,return_window,split_date",
            },
        ],
    )
    matrix_columns = [
        "family_id",
        "candidate_id",
        "source_artifact",
        "rows_found",
        "rows_materialized",
        "complete_rows",
        "incomplete_rows",
        "missing_return_observed",
        "missing_split_date",
        "missing_timestamp",
        "missing_symbol",
        "missing_regime",
        "materialization_status",
    ]
    _write_csv(
        materializer / "materialization_matrix.csv",
        matrix_columns,
        [
            {
                "family_id": "family_ready",
                "candidate_id": "candidate_ready",
                "source_artifact": "fixture",
                "rows_found": "1",
                "rows_materialized": "1",
                "complete_rows": "1",
                "incomplete_rows": "0",
                "missing_return_observed": "0",
                "missing_split_date": "0",
                "missing_timestamp": "0",
                "missing_symbol": "0",
                "missing_regime": "0",
                "materialization_status": "MATERIALIZED_COMPLETE",
            },
            {
                "family_id": "family_blocked",
                "candidate_id": "candidate_blocked",
                "source_artifact": "fixture",
                "rows_found": "1",
                "rows_materialized": "1",
                "complete_rows": "0",
                "incomplete_rows": "1",
                "missing_return_observed": "1",
                "missing_split_date": "1",
                "missing_timestamp": "1",
                "missing_symbol": "1",
                "missing_regime": "0",
                "materialization_status": "MATERIALIZED_PARTIAL",
            },
            {
                "family_id": "family_no_rows",
                "candidate_id": "candidate_no_rows",
                "source_artifact": "fixture",
                "rows_found": "0",
                "rows_materialized": "0",
                "complete_rows": "0",
                "incomplete_rows": "0",
                "missing_return_observed": "0",
                "missing_split_date": "0",
                "missing_timestamp": "0",
                "missing_symbol": "0",
                "missing_regime": "0",
                "materialization_status": "BLOCKED_NO_SOURCE_ROWS",
            },
        ],
    )
    _write_csv(
        materializer / "incomplete_event_rows.csv",
        ["event_id", "family_id", "candidate_id", "missing_fields", "evidence_status", "source_artifact", "notes"],
        [
            {
                "event_id": "evt_incomplete",
                "family_id": "",
                "candidate_id": "candidate_blocked",
                "missing_fields": "timestamp,date,symbol,return_observed,return_window,split_date",
                "evidence_status": "MATERIALIZED_INCOMPLETE",
                "source_artifact": "fixture",
                "notes": "",
            }
        ],
    )
    audit = root / "holdout_readiness_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "latest.json").write_text(json.dumps({"summary": {"ready_count": 0, "blocked_count": 3}}), encoding="utf-8")
    _write_csv(
        audit / "holdout_readiness_matrix.csv",
        ["family_id", "candidate_count", "holdout_rows_present", "return_observed_present", "timestamp_present", "symbol_present", "regime_present", "event_id_present", "split_date_present", "parser_supported", "readiness_status"],
        [
            {"family_id": "family_ready", "candidate_count": "1", "holdout_rows_present": "False", "return_observed_present": "False", "timestamp_present": "False", "symbol_present": "False", "regime_present": "False", "event_id_present": "False", "split_date_present": "False", "parser_supported": "True", "readiness_status": "BLOCKED_DATA"},
            {"family_id": "family_blocked", "candidate_count": "1", "holdout_rows_present": "False", "return_observed_present": "False", "timestamp_present": "False", "symbol_present": "False", "regime_present": "False", "event_id_present": "False", "split_date_present": "False", "parser_supported": "True", "readiness_status": "BLOCKED_DATA"},
            {"family_id": "family_no_rows", "candidate_count": "1", "holdout_rows_present": "False", "return_observed_present": "False", "timestamp_present": "False", "symbol_present": "False", "regime_present": "False", "event_id_present": "False", "split_date_present": "False", "parser_supported": "True", "readiness_status": "BLOCKED_DATA"},
        ],
    )


def test_readiness_recheck_loads_build_100_outputs_and_classifies_deterministically(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_holdout_event_row_readiness_recheck(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["summary"]["families_reviewed"] == 3
    assert report["summary"]["candidates_reviewed"] == 3
    assert report["summary"]["rows_reviewed"] == 2
    statuses = {row["candidate_id"]: row["readiness_status"] for row in report["readiness_matrix"]}
    assert statuses["candidate_ready"] == "READY"
    assert statuses["candidate_blocked"] == "BLOCKED_MULTIPLE_FIELDS"
    assert statuses["candidate_no_rows"] == "BLOCKED_NO_ROWS"
    assert classify_readiness(1, 0, 1, 0, 1, 0, 0) == "BLOCKED_MISSING_RETURN"
    assert report["confidence_impact"] == "NONE"


def test_readiness_recheck_missing_field_counts_and_no_replay_authority(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_holdout_event_row_readiness_recheck(tmp_path, created_at="2026-06-06T00:00:00Z")

    fields = {row["field_name"]: row for row in report["missing_field_summary"]}
    assert fields["timestamp"]["missing_count"] == "1"
    assert fields["date"]["missing_count"] == "1"
    assert fields["return_observed"]["missing_count"] == "1"
    assert fields["split_date"]["priority"] == "P0"
    no_rows = next(row for row in report["candidate_gap_matrix"] if row["candidate_id"] == "candidate_no_rows")
    assert no_rows["missing_fields"] == "event_rows"
    assert no_rows["blocking_reason"] == "no materialized event rows for candidate requirement"
    assert report["authority_boundary"]["research_only"] is True
    assert not any(key.endswith("_authorized") for key in report["authority_boundary"])
    assert "holdout replay" in report["authority_boundary"]["forbidden_actions"]
    assert "holdout validation" in report["authority_boundary"]["forbidden_actions"]


def test_readiness_recheck_writes_required_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_holdout_event_row_readiness_recheck(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_holdout_event_row_readiness_recheck(report, root=tmp_path)

    for key in ["latest_json", "latest_summary", "readiness_matrix", "family_gap_matrix", "candidate_gap_matrix", "missing_field_summary"]:
        assert paths[key].exists()
    with paths["readiness_matrix"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == READINESS_MATRIX_COLUMNS
    with paths["family_gap_matrix"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == FAMILY_GAP_COLUMNS
    with paths["candidate_gap_matrix"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == CANDIDATE_GAP_COLUMNS
    with paths["missing_field_summary"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == MISSING_FIELD_COLUMNS
    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "# Build 101 — Holdout Event Row Readiness Recheck" in summary
    assert "## Authority Boundary" in summary
