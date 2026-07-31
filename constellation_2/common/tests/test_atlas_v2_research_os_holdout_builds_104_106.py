from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_readiness_after_backfill import (
    build_holdout_readiness_after_backfill,
    write_holdout_readiness_after_backfill,
)
from constellation_2.common.atlas_v2_research_os.holdout_replay_dry_run import (
    build_holdout_replay_dry_run,
    write_holdout_replay_dry_run,
)
from constellation_2.common.atlas_v2_research_os.holdout_replay import build_holdout_replay, write_holdout_replay

NOW = "2026-06-06T00:00:00Z"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_inputs(root: Path) -> None:
    materializer = root / "holdout_event_row_materializer"
    columns = [
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
    complete_rows = [
        {
            "event_id": f"evt_{idx}",
            "family_id": "family_ready",
            "candidate_id": "candidate_ready",
            "timestamp": f"2025-01-{idx + 2:02d}T14:30:00+00:00",
            "date": f"2025-01-{idx + 2:02d}",
            "symbol": "SPY",
            "timeframe": "30M",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "source": "fixture",
            "trigger_observed": "true",
            "return_observed": value,
            "return_window": "next_5_bars",
            "split_date": "2025-01-01",
            "data_source": "fixture",
            "created_by": "test",
            "evidence_status": "MATERIALIZED_COMPLETE",
            "notes": "",
        }
        for idx, value in enumerate(["0.02", "-0.005", "0.018", "0.011"])
    ]
    blocked_row = {
        "event_id": "evt_blocked",
        "family_id": "family_blocked",
        "candidate_id": "candidate_blocked",
        "timestamp": "",
        "date": "",
        "symbol": "",
        "timeframe": "1H",
        "mechanism": "REVERSAL",
        "regime": "TRENDING",
        "source": "fixture",
        "trigger_observed": "true",
        "return_observed": "",
        "return_window": "",
        "split_date": "",
        "data_source": "fixture",
        "created_by": "test",
        "evidence_status": "MATERIALIZED_INCOMPLETE",
        "notes": "missing_fields=timestamp,date,symbol,return_observed,return_window,split_date",
    }
    _write_csv(materializer / "materialized_holdout_event_rows.csv", columns, complete_rows + [blocked_row])
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
                "rows_found": "4",
                "rows_materialized": "4",
                "complete_rows": "4",
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
                "materialization_status": "MATERIALIZED_INCOMPLETE",
            },
        ],
    )
    _write_csv(
        materializer / "incomplete_event_rows.csv",
        ["event_id", "family_id", "candidate_id", "missing_fields", "evidence_status", "source_artifact", "notes"],
        [
            {
                "event_id": "evt_blocked",
                "family_id": "family_blocked",
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
    (audit / "latest.json").write_text(json.dumps({"summary": {"families_audited": 2}}), encoding="utf-8")
    _write_csv(audit / "holdout_readiness_matrix.csv", ["family_id", "candidate_count"], [{"family_id": "family_ready", "candidate_count": "1"}])


def test_build_104_rechecks_after_backfill_and_writes_required_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_holdout_readiness_after_backfill(tmp_path, created_at=NOW)

    statuses = {row["candidate_id"]: row["classification"] for row in report["readiness_matrix"]}
    assert statuses["candidate_ready"] == "READY"
    assert statuses["candidate_blocked"] == "BLOCKED_MULTIPLE_FIELDS"
    assert report["summary"]["ready_family_count"] == 1
    assert report["summary"]["blocked_family_count"] == 1
    assert report["summary"]["complete_rows"] == 4
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["holdout_replay_run"] is False

    paths = write_holdout_readiness_after_backfill(report, tmp_path)
    for key in ["latest_json", "latest_summary", "readiness_matrix", "candidate_holdout_status", "remaining_holdout_blockers"]:
        assert paths[key].exists()


def test_build_105_dry_run_uses_ready_rows_without_scoring(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    write_holdout_readiness_after_backfill(build_holdout_readiness_after_backfill(tmp_path, created_at=NOW), tmp_path)

    report = build_holdout_replay_dry_run(tmp_path, created_at=NOW)

    assert report["summary"]["rows_tested"] == 4
    assert report["summary"]["parser_ready_count"] == 4
    assert report["summary"]["blocked_count"] == 0
    assert report["dry_run_matrix"][0]["dry_run_classification"] == "DRY_RUN_READY"
    assert report["authority_boundary"]["market_outcome_scoring"] is False
    assert report["authority_boundary"]["edge_classification_emitted"] is False

    paths = write_holdout_replay_dry_run(report, tmp_path)
    for key in ["latest_json", "latest_summary", "dry_run_matrix", "replay_schema_checks", "dry_run_blockers"]:
        assert paths[key].exists()


def test_build_106_replays_only_dry_run_ready_rows(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    write_holdout_readiness_after_backfill(build_holdout_readiness_after_backfill(tmp_path, created_at=NOW), tmp_path)
    write_holdout_replay_dry_run(build_holdout_replay_dry_run(tmp_path, created_at=NOW), tmp_path)

    report = build_holdout_replay(tmp_path, created_at=NOW)

    assert report["summary"]["families_tested"] == 1
    assert report["summary"]["survived"] == 1
    assert report["summary"]["confidence_impact"] == "SMALL_INCREASE"
    result = report["holdout_replay_results"][0]
    assert result["candidate_id"] == "candidate_ready"
    assert result["classification"] == "HOLDOUT_SURVIVED"
    assert result["confidence_impact"] == "SMALL_INCREASE"
    assert report["authority_boundary"]["trading_authority"] is False
    assert report["authority_boundary"]["promotion_authority_emitted"] is False

    paths = write_holdout_replay(report, tmp_path)
    for key in ["latest_json", "latest_summary", "holdout_replay_results", "family_holdout_results", "failed_holdout_rows"]:
        assert paths[key].exists()
