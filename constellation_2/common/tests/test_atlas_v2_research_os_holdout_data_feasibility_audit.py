from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_data_feasibility_audit import (
    build_holdout_data_feasibility_audit,
    run_holdout_data_feasibility_audit,
    write_holdout_data_feasibility_audit,
)

NOW = "2026-06-06T00:00:00Z"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _seed_inputs(root: Path) -> None:
    _write_csv(
        root / "governed_holdout_outcome_data_acquisition_manifest" / "holdout_outcome_acquisition_manifest.csv",
        [
            "priority",
            "family_id",
            "candidate_id",
            "missing_fields",
            "required_symbol",
            "required_timeframe",
            "required_regime",
            "required_date_range",
            "required_return_window",
            "split_date_requirement",
            "reason_blocked",
            "acceptable_source",
            "not_acceptable_source",
            "post_import_validation_command",
        ],
        [
            {
                "priority": "P0",
                "family_id": "fam_now",
                "candidate_id": "cand_now",
                "missing_fields": "timestamp",
                "required_symbol": "SPY",
                "required_timeframe": "30M",
                "required_regime": "CHOP",
                "required_date_range": "POST_SPLIT_HOLDOUT_OBSERVATION_PERIOD",
                "required_return_window": "1D",
                "split_date_requirement": "2024-01-01",
                "reason_blocked": "missing timestamp",
                "acceptable_source": "source",
                "not_acceptable_source": "synthetic",
                "post_import_validation_command": "validate",
            },
            {
                "priority": "P0",
                "family_id": "fam_work",
                "candidate_id": "cand_work",
                "missing_fields": "return_observed,return_window,split_date",
                "required_symbol": "SPY",
                "required_timeframe": "30M",
                "required_regime": "TRENDING",
                "required_date_range": "POST_SPLIT_HOLDOUT_OBSERVATION_PERIOD",
                "required_return_window": "EVENT_DEFINED_RETURN_WINDOW_REQUIRED",
                "split_date_requirement": "SOURCE_BACKED_SPLIT_DATE_PRECEDING_HOLDOUT_OBSERVATION_PERIOD",
                "reason_blocked": "needs outcomes",
                "acceptable_source": "source",
                "not_acceptable_source": "synthetic",
                "post_import_validation_command": "validate",
            },
            {
                "priority": "P0",
                "family_id": "fam_unknown",
                "candidate_id": "cand_unknown",
                "missing_fields": "timestamp,return_observed",
                "required_symbol": "SOURCE_REQUIRED",
                "required_timeframe": "FROZEN_FAMILY_OR_CANDIDATE_TIMEFRAME",
                "required_regime": "CHOP",
                "required_date_range": "POST_SPLIT_HOLDOUT_OBSERVATION_PERIOD",
                "required_return_window": "EVENT_DEFINED_RETURN_WINDOW_REQUIRED",
                "split_date_requirement": "SOURCE_BACKED_SPLIT_DATE_PRECEDING_HOLDOUT_OBSERVATION_PERIOD",
                "reason_blocked": "source required",
                "acceptable_source": "source",
                "not_acceptable_source": "synthetic",
                "post_import_validation_command": "validate",
            },
            {
                "priority": "P0",
                "family_id": "fam_only",
                "candidate_id": "",
                "missing_fields": "split_date",
                "required_symbol": "SOURCE_REQUIRED",
                "required_timeframe": "FROZEN_FAMILY_OR_CANDIDATE_TIMEFRAME",
                "required_regime": "CHOP",
                "required_date_range": "POST_SPLIT_HOLDOUT_OBSERVATION_PERIOD",
                "required_return_window": "EVENT_DEFINED_RETURN_WINDOW_REQUIRED",
                "split_date_requirement": "SOURCE_BACKED_SPLIT_DATE_PRECEDING_HOLDOUT_OBSERVATION_PERIOD",
                "reason_blocked": "family only split missing",
                "acceptable_source": "source",
                "not_acceptable_source": "synthetic",
                "post_import_validation_command": "validate",
            },
        ],
    )
    _write_csv(
        root / "holdout_event_outcome_backfill_plan" / "backfill_candidates.csv",
        ["family_id", "candidate_id", "field_name", "rows_missing", "recoverable", "recovery_confidence", "source_artifact", "recovery_method"],
        [
            {
                "family_id": "fam_now",
                "candidate_id": "cand_now",
                "field_name": "timestamp",
                "rows_missing": "1",
                "recoverable": "DIRECT_RECOVERY",
                "recovery_confidence": "HIGH",
                "source_artifact": "reports/source.json",
                "recovery_method": "timestamp alias present",
            },
            {
                "family_id": "fam_work",
                "candidate_id": "cand_work",
                "field_name": "return_window",
                "rows_missing": "1",
                "recoverable": "INDIRECT_RECOVERY",
                "recovery_confidence": "MEDIUM",
                "source_artifact": "reports/spec.json",
                "recovery_method": "methodology alias present",
            },
        ],
    )
    _write_csv(
        root / "holdout_event_outcome_backfill_plan" / "backfill_sources.csv",
        ["source_artifact", "field_name", "rows_available", "candidate_keys_present", "family_keys_present", "usable_for_backfill", "notes"],
        [
            {
                "source_artifact": "reports/source.json",
                "field_name": "timestamp",
                "rows_available": "1",
                "candidate_keys_present": "true",
                "family_keys_present": "true",
                "usable_for_backfill": "true",
                "notes": "fixture",
            }
        ],
    )
    _write_csv(
        root / "holdout_event_row_materializer" / "incomplete_event_rows.csv",
        ["event_id", "family_id", "candidate_id", "missing_fields", "evidence_status", "source_artifact", "notes"],
        [
            {"event_id": "evt1", "family_id": "fam_now", "candidate_id": "cand_now", "missing_fields": "timestamp", "evidence_status": "MATERIALIZED_INCOMPLETE", "source_artifact": "fixture", "notes": ""},
            {"event_id": "evt2", "family_id": "fam_work", "candidate_id": "cand_work", "missing_fields": "return_observed", "evidence_status": "MATERIALIZED_INCOMPLETE", "source_artifact": "fixture", "notes": ""},
        ],
    )
    _write_json(root / "governed_holdout_outcome_data_acquisition_manifest" / "latest.json", {})
    _write_json(root / "holdout_event_outcome_backfill_plan" / "latest.json", {})
    _write_json(root / "holdout_event_row_materializer" / "latest.json", {})


def test_build_117_classifies_holdout_field_feasibility(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_holdout_data_feasibility_audit(tmp_path, created_at=NOW)

    by_key = {(row["family_id"], row["candidate_id"], row["field_name"]): row for row in report["feasibility_matrix"]}
    assert by_key[("fam_now", "cand_now", "timestamp")]["classification"] == "OBTAINABLE_NOW"
    assert by_key[("fam_work", "cand_work", "return_observed")]["classification"] == "OBTAINABLE_WITH_WORK"
    assert by_key[("fam_work", "cand_work", "return_window")]["classification"] == "OBTAINABLE_WITH_WORK"
    assert by_key[("fam_work", "cand_work", "split_date")]["classification"] == "OBTAINABLE_WITH_WORK"
    assert by_key[("fam_unknown", "cand_unknown", "timestamp")]["classification"] == "UNKNOWN"
    assert by_key[("fam_only", "", "split_date")]["classification"] == "LIKELY_UNOBTAINABLE"
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["holdout_replay_run"] is False


def test_build_117_writes_required_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = run_holdout_data_feasibility_audit(tmp_path, created_at=NOW)
    paths = write_holdout_data_feasibility_audit(report, tmp_path)

    for key in ["latest_json", "latest_summary", "holdout_data_sources", "feasibility_matrix", "acquisition_effort"]:
        assert paths[key].exists()
    assert (tmp_path / "holdout_data_feasibility_audit" / "latest.json").exists()
    assert report["summary"]["holdout_feasibility"] == "PARTIAL_FEASIBILITY_WITH_LIKELY_UNOBTAINABLE_ROWS"
    assert report["holdout_data_sources"]
