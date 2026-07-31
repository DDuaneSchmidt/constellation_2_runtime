from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_event_outcome_backfill_plan import (
    BACKFILL_CANDIDATE_FIELDS,
    BACKFILL_SOURCE_FIELDS,
    INVENTORY_FIELDS,
    UNRECOVERABLE_FIELDS,
    build_holdout_event_outcome_backfill_plan,
    write_holdout_event_outcome_backfill_plan,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_materializer(root: Path) -> None:
    rows = [
        {
            "event_id": "evt_candidate_direct",
            "family_id": "family_alpha",
            "candidate_id": "candidate_direct",
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
            "event_id": "evt_candidate_none",
            "family_id": "family_beta",
            "candidate_id": "candidate_none",
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
            "notes": "missing_fields=return_observed,split_date",
        },
    ]
    fields = list(rows[0])
    _write_csv(root / "holdout_event_row_materializer" / "materialized_holdout_event_rows.csv", fields, rows)
    _write_csv(
        root / "holdout_event_row_materializer" / "incomplete_event_rows.csv",
        ["event_id", "family_id", "candidate_id", "missing_fields", "evidence_status", "source_artifact", "notes"],
        [
            {
                "event_id": row["event_id"],
                "family_id": row["family_id"],
                "candidate_id": row["candidate_id"],
                "missing_fields": row["notes"].split("missing_fields=", 1)[1],
                "evidence_status": row["evidence_status"],
                "source_artifact": "seed",
                "notes": row["notes"],
            }
            for row in rows
        ],
    )
    _write_json(root / "holdout_event_row_materializer" / "latest.json", {"materialized_holdout_event_rows": rows, "incomplete_event_rows": []})
    _write_csv(
        root / "holdout_event_row_builder_design" / "holdout_event_row_requirements.csv",
        ["family_id", "candidate_id", "required_symbol", "required_return_window", "required_start"],
        [
            {"family_id": "family_alpha", "candidate_id": "candidate_direct", "required_symbol": "SYMBOL_REQUIRED", "required_return_window": "FROZEN_HOLDOUT_RETURN_WINDOW_REQUIRED", "required_start": "UNSPECIFIED_EVENT_IMPORT_START"},
            {"family_id": "family_beta", "candidate_id": "candidate_none", "required_symbol": "SYMBOL_REQUIRED", "required_return_window": "FROZEN_HOLDOUT_RETURN_WINDOW_REQUIRED", "required_start": "UNSPECIFIED_EVENT_IMPORT_START"},
        ],
    )


def _seed_sources(root: Path) -> list[Path]:
    direct = root / "historical_replay" / "latest.json"
    _write_json(
        direct,
        {
            "events": [
                {
                    "family_id": "family_alpha",
                    "candidate_id": "candidate_direct",
                    "timestamp": "2024-04-01T14:30:00Z",
                    "symbol": "AAPL",
                    "return_observed": "0.012",
                    "return_window": "next_5_bars",
                }
            ]
        },
    )
    partial = root / "family_stability_analysis" / "latest.json"
    _write_json(partial, {"family_rows": [{"family_id": "family_alpha", "date": "2024-04-01", "symbol": "AAPL"}]})
    unrelated = root / "direct_candidate_data_validation" / "latest.json"
    _write_json(unrelated, {"candidate_validations": [{"candidate_id": "other", "return_observed": "0.4", "split_date": "2024-01-01"}]})
    return [direct, partial, unrelated]


def _digest(paths: list[Path]) -> dict[str, str]:
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def test_backfill_plan_classifies_recoverable_and_unrecoverable_fields(tmp_path: Path) -> None:
    _seed_materializer(tmp_path)
    _seed_sources(tmp_path)

    report = build_holdout_event_outcome_backfill_plan(tmp_path, created_at="2026-06-06T00:00:00Z")
    rows = {(row["candidate_id"], row["field_name"]): row for row in report["backfill_candidates"]}

    assert report["summary"]["artifacts_scanned"] >= 3
    assert report["summary"]["fields_inventoried"] > 0
    assert rows[("candidate_direct", "return_observed")]["recoverable"] == "DIRECT_RECOVERY"
    assert rows[("candidate_direct", "date")]["recoverable"] in {"DIRECT_RECOVERY", "INDIRECT_RECOVERY"}
    assert rows[("candidate_none", "split_date")]["recoverable"] == "NO_RECOVERY"
    assert any(row["field_name"] == "split_date" and row["candidate_id"] == "candidate_none" for row in report["unrecoverable_fields"])
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["holdout_rows_modified"] is False
    assert report["authority_boundary"]["holdout_replay_run"] is False


def test_backfill_plan_writes_required_reports_and_does_not_modify_sources(tmp_path: Path) -> None:
    _seed_materializer(tmp_path)
    source_paths = _seed_sources(tmp_path)
    before = _digest(source_paths + [tmp_path / "holdout_event_row_materializer" / "materialized_holdout_event_rows.csv"])

    report = build_holdout_event_outcome_backfill_plan(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_holdout_event_outcome_backfill_plan(report, tmp_path)

    after = _digest(source_paths + [tmp_path / "holdout_event_row_materializer" / "materialized_holdout_event_rows.csv"])
    assert before == after
    for key in ["latest_json", "latest_summary", "backfill_candidates", "backfill_sources", "unrecoverable_fields", "artifact_field_inventory"]:
        assert paths[key].exists(), key
    expected = {
        "backfill_candidates": BACKFILL_CANDIDATE_FIELDS,
        "backfill_sources": BACKFILL_SOURCE_FIELDS,
        "unrecoverable_fields": UNRECOVERABLE_FIELDS,
        "artifact_field_inventory": INVENTORY_FIELDS,
    }
    for key, fields in expected.items():
        with paths[key].open(newline="", encoding="utf-8") as handle:
            assert csv.DictReader(handle).fieldnames == fields
    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "# Build 102 — Holdout Event Outcome Backfill Plan" in summary
    assert "## Required New Data" in summary
    assert "NONE" in summary
