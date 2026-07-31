from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_event_backfill_materializer import (
    AUDIT_TRAIL_COLUMNS,
    STILL_INCOMPLETE_COLUMNS,
    build_holdout_event_backfill_materializer,
    write_holdout_event_backfill_materializer,
)
from constellation_2.common.atlas_v2_research_os.holdout_event_row_materializer import CANONICAL_COLUMNS


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_inputs(root: Path) -> list[Path]:
    materialized = [
        {
            "event_id": "evt_1",
            "family_id": "family_alpha",
            "candidate_id": "candidate_alpha",
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
            "data_source": "seed",
            "created_by": "build_100_holdout_event_row_materializer",
            "evidence_status": "MATERIALIZED_INCOMPLETE",
            "notes": "missing_fields=timestamp,date,symbol,return_observed,return_window,split_date",
        },
        {
            "event_id": "evt_2",
            "family_id": "family_beta",
            "candidate_id": "candidate_beta",
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
            "data_source": "seed",
            "created_by": "build_100_holdout_event_row_materializer",
            "evidence_status": "MATERIALIZED_INCOMPLETE",
            "notes": "missing_fields=symbol,return_observed,split_date",
        },
    ]
    _write_csv(root / "holdout_event_row_materializer" / "materialized_holdout_event_rows.csv", CANONICAL_COLUMNS, materialized)
    _write_csv(
        root / "holdout_event_row_materializer" / "incomplete_event_rows.csv",
        ["event_id", "family_id", "candidate_id", "missing_fields", "evidence_status", "source_artifact", "notes"],
        [
            {"event_id": row["event_id"], "family_id": row["family_id"], "candidate_id": row["candidate_id"], "missing_fields": row["notes"].split("missing_fields=", 1)[1], "evidence_status": row["evidence_status"], "source_artifact": "seed", "notes": row["notes"]}
            for row in materialized
        ],
    )
    source = root / "historical_replay" / "latest.json"
    _write_json(
        source,
        {
            "events": [
                {
                    "family_id": "family_alpha",
                    "candidate_id": "candidate_alpha",
                    "date": "2024-04-01",
                    "symbol": "AAPL",
                    "return_window": "next_5_bars",
                    "return_observed": "0.012",
                    "split_date": "2024-01-01",
                },
                {
                    "family_id": "family_beta",
                    "candidate_id": "candidate_beta",
                    "symbol": "MSFT",
                    "return_observed": "0.8",
                    "split_date": "2024-02-01",
                },
            ]
        },
    )
    _write_csv(
        root / "holdout_event_outcome_backfill_plan" / "backfill_candidates.csv",
        ["family_id", "candidate_id", "field_name", "rows_missing", "recoverable", "recovery_confidence", "source_artifact", "recovery_method"],
        [
            {"family_id": "family_alpha", "candidate_id": "candidate_alpha", "field_name": "date", "rows_missing": "1", "recoverable": "DIRECT_RECOVERY", "recovery_confidence": "HIGH", "source_artifact": "historical_replay/latest.json", "recovery_method": "exact date present on exact keyed record"},
            {"family_id": "family_alpha", "candidate_id": "candidate_alpha", "field_name": "symbol", "rows_missing": "1", "recoverable": "DIRECT_RECOVERY", "recovery_confidence": "HIGH", "source_artifact": "historical_replay/latest.json", "recovery_method": "exact symbol present on exact keyed record"},
            {"family_id": "family_alpha", "candidate_id": "candidate_alpha", "field_name": "return_window", "rows_missing": "1", "recoverable": "INDIRECT_RECOVERY", "recovery_confidence": "MEDIUM", "source_artifact": "historical_replay/latest.json", "recovery_method": "return_window alias present on exact keyed record"},
            {"family_id": "family_alpha", "candidate_id": "candidate_alpha", "field_name": "timestamp", "rows_missing": "1", "recoverable": "PARTIAL_RECOVERY", "recovery_confidence": "LOW", "source_artifact": "historical_replay/latest.json", "recovery_method": "date exists but timestamp remains unavailable"},
            {"family_id": "family_alpha", "candidate_id": "candidate_alpha", "field_name": "return_observed", "rows_missing": "1", "recoverable": "NO_RECOVERY", "recovery_confidence": "NONE", "source_artifact": "", "recovery_method": "not recoverable"},
            {"family_id": "family_alpha", "candidate_id": "candidate_alpha", "field_name": "split_date", "rows_missing": "1", "recoverable": "NO_RECOVERY", "recovery_confidence": "NONE", "source_artifact": "", "recovery_method": "not recoverable"},
            {"family_id": "family_beta", "candidate_id": "candidate_beta", "field_name": "symbol", "rows_missing": "1", "recoverable": "DIRECT_RECOVERY", "recovery_confidence": "HIGH", "source_artifact": "historical_replay/latest.json", "recovery_method": "exact symbol present on exact keyed record"},
            {"family_id": "family_beta", "candidate_id": "candidate_beta", "field_name": "return_observed", "rows_missing": "1", "recoverable": "DIRECT_RECOVERY", "recovery_confidence": "LOW", "source_artifact": "historical_replay/latest.json", "recovery_method": "low confidence return must not be applied"},
        ],
    )
    _write_json(root / "holdout_event_outcome_backfill_plan" / "latest.json", {"backfill_candidates": []})
    return [source, root / "holdout_event_row_materializer" / "materialized_holdout_event_rows.csv"]


def _digest(paths: list[Path]) -> dict[str, str]:
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def test_backfill_materializer_applies_only_high_and_medium_source_values(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_holdout_event_backfill_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")
    rows = {row["event_id"]: row for row in report["backfilled_holdout_event_rows"]}
    audit = {(row["event_id"], row["field_name"]): row for row in report["backfill_audit_trail"]}

    assert rows["evt_1"]["event_id"] == "evt_1"
    assert rows["evt_1"]["date"] == "2024-04-01"
    assert rows["evt_1"]["symbol"] == "AAPL"
    assert rows["evt_1"]["return_window"] == "next_5_bars"
    assert rows["evt_1"]["timestamp"] == ""
    assert rows["evt_1"]["return_observed"] == ""
    assert rows["evt_1"]["split_date"] == ""
    assert rows["evt_2"]["symbol"] == "MSFT"
    assert rows["evt_2"]["return_observed"] == ""
    assert audit[("evt_1", "date")]["recovery_confidence"] == "HIGH"
    assert audit[("evt_1", "return_window")]["recovery_confidence"] == "MEDIUM"
    assert ("evt_1", "timestamp") not in audit
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["holdout_replay_run"] is False
    assert report["authority_boundary"]["promotion_authority"] is False
    assert report["authority_boundary"]["trading_authority"] is False


def test_backfill_materializer_writes_required_reports_and_keeps_sources_unchanged(tmp_path: Path) -> None:
    source_paths = _seed_inputs(tmp_path)
    before = _digest(source_paths)

    report = build_holdout_event_backfill_materializer(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_holdout_event_backfill_materializer(report, tmp_path)

    after = _digest(source_paths)
    assert before == after
    for key in ["latest_json", "latest_summary", "backfilled_rows", "audit_trail", "still_incomplete_rows"]:
        assert paths[key].exists(), key
    with paths["backfilled_rows"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == CANONICAL_COLUMNS
    with paths["audit_trail"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == AUDIT_TRAIL_COLUMNS
    with paths["still_incomplete_rows"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == STILL_INCOMPLETE_COLUMNS
    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "# Build 103 - Holdout Event Backfill Materializer" in summary
    assert "NONE" in summary
    assert "No holdout replay" in summary
