from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_event_row_materializer import CANONICAL_COLUMNS
from constellation_2.common.atlas_v2_research_os.holdout_reconstruction_from_databento import (
    AUDIT_COLUMNS,
    LOOKAHEAD_COLUMNS,
    RECONSTRUCTED_COLUMNS,
    UNRECONSTRUCTED_COLUMNS,
    build_holdout_reconstruction_from_databento,
    write_holdout_reconstruction_from_databento,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_required_dirs(root: Path) -> None:
    (root / "exact_replay_without_fallback").mkdir(parents=True, exist_ok=True)
    (root / "direct_replay_attrition_audit").mkdir(parents=True, exist_ok=True)
    (root / "exact_replay_without_fallback" / "latest.json").write_text("{}\n", encoding="utf-8")
    (root / "direct_replay_attrition_audit" / "latest.json").write_text("{}\n", encoding="utf-8")


def _seed_manifest(root: Path) -> None:
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
                "family_id": "family_a",
                "candidate_id": "candidate_a",
                "missing_fields": "symbol",
                "required_symbol": "AAPL",
                "required_timeframe": "1m",
                "required_regime": "CHOP",
                "required_date_range": "POST_SPLIT_HOLDOUT_OBSERVATION_PERIOD",
                "required_return_window": "EVENT_DEFINED_RETURN_WINDOW_REQUIRED",
                "split_date_requirement": "SOURCE_BACKED_SPLIT_DATE_PRECEDING_HOLDOUT_OBSERVATION_PERIOD",
                "reason_blocked": "seed",
                "acceptable_source": "seed",
                "not_acceptable_source": "synthetic",
                "post_import_validation_command": "do not run replay",
            }
        ],
    )


def test_databento_reconstruction_computes_only_when_timestamp_split_and_future_bar_exist(tmp_path: Path, monkeypatch) -> None:
    _seed_required_dirs(tmp_path)
    _seed_manifest(tmp_path)
    rows = [
        {
            "event_id": "evt_complete",
            "family_id": "family_a",
            "candidate_id": "candidate_a",
            "timestamp": "2022-09-30T14:00:00Z",
            "date": "2022-09-30",
            "symbol": "",
            "timeframe": "1m",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "source": "JOURNAL_EXTRACT",
            "trigger_observed": "true",
            "return_observed": "",
            "return_window": "",
            "split_date": "2022-09-01",
            "data_source": "seed",
            "created_by": "seed",
            "evidence_status": "MATERIALIZED_INCOMPLETE",
            "notes": "seed",
        }
    ]
    _write_csv(tmp_path / "holdout_event_backfill_materializer" / "backfilled_holdout_event_rows.csv", CANONICAL_COLUMNS, rows)
    data_dir = tmp_path / "manual_intraday_import"
    _write_csv(
        data_dir / "AAPL_1m.csv",
        ["timestamp", "open", "high", "low", "close", "volume"],
        [
            {"timestamp": "2022-09-30T14:00:00Z", "open": "100", "high": "100", "low": "100", "close": "100", "volume": "1"},
            {"timestamp": "2022-09-30T14:30:00Z", "open": "102", "high": "102", "low": "102", "close": "102", "volume": "1"},
        ],
    )
    monkeypatch.setattr("constellation_2.common.atlas_v2_research_os.holdout_reconstruction_from_databento.MANUAL_INTRADAY_DIR", data_dir)

    report = build_holdout_reconstruction_from_databento(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["summary"]["rows_reviewed"] == 1
    assert report["summary"]["rows_reconstructed_complete"] == 1
    reconstructed = report["reconstructed_holdout_event_rows"][0]
    assert reconstructed["reconstruction_classification"] == "RECONSTRUCTED_COMPLETE"
    assert reconstructed["symbol"] == "AAPL"
    assert reconstructed["return_window"] == "30m"
    assert reconstructed["return_observed"] == "0.0200000000"
    assert report["authority_boundary"]["holdout_replay_run"] is False


def test_databento_reconstruction_reports_blockers_and_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    _seed_required_dirs(tmp_path)
    _seed_manifest(tmp_path)
    rows = [
        {
            "event_id": "evt_missing_timestamp",
            "family_id": "family_a",
            "candidate_id": "candidate_a",
            "timestamp": "",
            "date": "",
            "symbol": "AAPL",
            "timeframe": "1m",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "source": "JOURNAL_EXTRACT",
            "trigger_observed": "true",
            "return_observed": "",
            "return_window": "",
            "split_date": "2022-09-01",
            "data_source": "seed",
            "created_by": "seed",
            "evidence_status": "MATERIALIZED_INCOMPLETE",
            "notes": "seed",
        },
        {
            "event_id": "evt_missing_symbol",
            "family_id": "family_b",
            "candidate_id": "candidate_b",
            "timestamp": "2022-09-30T14:00:00Z",
            "date": "2022-09-30",
            "symbol": "DIA",
            "timeframe": "1m",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "source": "JOURNAL_EXTRACT",
            "trigger_observed": "true",
            "return_observed": "",
            "return_window": "",
            "split_date": "2022-09-01",
            "data_source": "seed",
            "created_by": "seed",
            "evidence_status": "MATERIALIZED_INCOMPLETE",
            "notes": "seed",
        },
        {
            "event_id": "evt_blocked_split",
            "family_id": "family_a",
            "candidate_id": "candidate_a",
            "timestamp": "2022-09-30T14:00:00Z",
            "date": "2022-09-30",
            "symbol": "AAPL",
            "timeframe": "1m",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "source": "JOURNAL_EXTRACT",
            "trigger_observed": "true",
            "return_observed": "",
            "return_window": "",
            "split_date": "",
            "data_source": "seed",
            "created_by": "seed",
            "evidence_status": "MATERIALIZED_INCOMPLETE",
            "notes": "seed",
        },
    ]
    _write_csv(tmp_path / "holdout_event_backfill_materializer" / "backfilled_holdout_event_rows.csv", CANONICAL_COLUMNS, rows)
    data_dir = tmp_path / "manual_intraday_import"
    _write_csv(data_dir / "AAPL_1m.csv", ["timestamp", "open", "high", "low", "close", "volume"], [])
    monkeypatch.setattr("constellation_2.common.atlas_v2_research_os.holdout_reconstruction_from_databento.MANUAL_INTRADAY_DIR", data_dir)

    report = build_holdout_reconstruction_from_databento(tmp_path, created_at="2026-06-06T00:00:00Z")
    classifications = {row["event_id"]: row["classification"] for row in report["reconstruction_audit_trail"]}

    assert classifications == {
        "evt_missing_timestamp": "RECONSTRUCTED_MISSING_TIMESTAMP",
        "evt_missing_symbol": "RECONSTRUCTED_MISSING_SYMBOL",
        "evt_blocked_split": "RECONSTRUCTED_BLOCKED_SPLIT_DATE",
    }
    assert report["summary"]["rows_reconstructed_complete"] == 0
    assert report["summary"]["split_date_blockers"] == 1
    paths = write_holdout_reconstruction_from_databento(report, tmp_path)
    assert json.loads(paths["latest_json"].read_text(encoding="utf-8"))["summary"]["rows_reviewed"] == 3
    expected = {
        "reconstructed_rows": RECONSTRUCTED_COLUMNS,
        "audit_trail": AUDIT_COLUMNS,
        "unreconstructed_rows": UNRECONSTRUCTED_COLUMNS,
        "lookahead_risk_report": LOOKAHEAD_COLUMNS,
    }
    for key, columns in expected.items():
        with paths[key].open(newline="", encoding="utf-8") as handle:
            assert csv.DictReader(handle).fieldnames == columns
    assert "Holdout replay now possible: False" in paths["latest_summary"].read_text(encoding="utf-8")
