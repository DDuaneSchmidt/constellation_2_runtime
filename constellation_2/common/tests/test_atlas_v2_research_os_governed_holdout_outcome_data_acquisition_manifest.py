from __future__ import annotations

import csv
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.governed_holdout_outcome_data_acquisition_manifest import (
    REQUIRED_SCHEMA_FIELDS,
    build_governed_holdout_outcome_data_acquisition_manifest,
    write_governed_holdout_outcome_data_acquisition_manifest,
)

NOW = "2026-06-06T00:00:00Z"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_inputs(root: Path) -> None:
    backfill_materializer = root / "holdout_event_backfill_materializer"
    _write_csv(
        backfill_materializer / "still_incomplete_rows.csv",
        ["event_id", "candidate_id", "family_id", "missing_fields", "blocking_reason", "next_required_data"],
        [
            {
                "event_id": "evt_candidate",
                "candidate_id": "candidate_blocked",
                "family_id": "family_blocked",
                "missing_fields": "return_observed,return_window,split_date",
                "blocking_reason": "SOURCE_EVIDENCE_REQUIRED_FOR_NON_FABRICABLE_FIELDS",
                "next_required_data": "governed source evidence",
            },
            {
                "event_id": "evt_family",
                "candidate_id": "",
                "family_id": "family_only",
                "missing_fields": "timestamp,symbol",
                "blocking_reason": "SOURCE_EVIDENCE_REQUIRED_FOR_NON_FABRICABLE_FIELDS",
                "next_required_data": "governed source evidence",
            },
        ],
    )
    _write_csv(
        backfill_materializer / "backfilled_holdout_event_rows.csv",
        [
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
        ],
        [
            {
                "event_id": "evt_candidate",
                "family_id": "family_blocked",
                "candidate_id": "candidate_blocked",
                "timestamp": "",
                "date": "2025-01-03",
                "symbol": "SPY",
                "timeframe": "30M",
                "mechanism": "BREAKOUT",
                "regime": "CHOP",
                "source": "fixture",
                "trigger_observed": "true",
                "return_observed": "",
                "return_window": "",
                "split_date": "",
                "data_source": "fixture",
                "created_by": "test",
                "evidence_status": "MATERIALIZED_INCOMPLETE",
                "notes": "missing_fields=return_observed,return_window,split_date",
            },
            {
                "event_id": "evt_family",
                "family_id": "family_only",
                "candidate_id": "",
                "timestamp": "",
                "date": "",
                "symbol": "",
                "timeframe": "1H",
                "mechanism": "EVENT_REACTION",
                "regime": "TRENDING",
                "source": "fixture",
                "trigger_observed": "true",
                "return_observed": "",
                "return_window": "",
                "split_date": "",
                "data_source": "fixture",
                "created_by": "test",
                "evidence_status": "MATERIALIZED_INCOMPLETE",
                "notes": "missing_fields=timestamp,symbol,return_observed,return_window,split_date",
            },
        ],
    )
    readiness = root / "holdout_readiness_after_backfill"
    _write_csv(
        readiness / "candidate_holdout_status.csv",
        ["family_id", "candidate_id", "classification", "rows_found", "complete_rows", "remaining_blocker_fields", "eligible_for_dry_run"],
        [
            {
                "family_id": "family_blocked",
                "candidate_id": "candidate_blocked",
                "classification": "BLOCKED_MULTIPLE_FIELDS",
                "rows_found": "1",
                "complete_rows": "0",
                "remaining_blocker_fields": "return_observed,split_date,timestamp",
                "eligible_for_dry_run": "false",
            },
            {
                "family_id": "family_status_only",
                "candidate_id": "candidate_status_only",
                "classification": "BLOCKED_MISSING_RETURN",
                "rows_found": "1",
                "complete_rows": "0",
                "remaining_blocker_fields": "return_observed,return_window,split_date",
                "eligible_for_dry_run": "false",
            },
        ],
    )
    _write_csv(
        readiness / "remaining_holdout_blockers.csv",
        ["family_id", "candidate_id", "classification", "blocker_field", "blocked_rows", "confidence_impact"],
        [
            {
                "family_id": "family_blocked",
                "candidate_id": "candidate_blocked",
                "classification": "BLOCKED_MULTIPLE_FIELDS",
                "blocker_field": "return_observed",
                "blocked_rows": "1",
                "confidence_impact": "NONE",
            },
            {
                "family_id": "family_status_only",
                "candidate_id": "candidate_status_only",
                "classification": "BLOCKED_MISSING_RETURN",
                "blocker_field": "return_window",
                "blocked_rows": "1",
                "confidence_impact": "NONE",
            },
        ],
    )
    _write_csv(
        root / "holdout_event_outcome_backfill_plan" / "unrecoverable_fields.csv",
        ["field_name", "family_id", "candidate_id", "reason_unrecoverable", "new_data_required", "priority"],
        [
            {
                "field_name": "split_date",
                "family_id": "family_status_only",
                "candidate_id": "candidate_status_only",
                "reason_unrecoverable": "not in source",
                "new_data_required": "true",
                "priority": "P0",
            }
        ],
    )
    (root / "holdout_replay_dry_run").mkdir(parents=True, exist_ok=True)
    (root / "holdout_replay_dry_run" / "latest.json").write_text("{}\n", encoding="utf-8")
    (root / "final_evidence_synthesis").mkdir(parents=True, exist_ok=True)
    (root / "final_evidence_synthesis" / "latest.json").write_text("{}\n", encoding="utf-8")


def test_build_112_includes_required_missing_outcome_fields_and_blocked_keys(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_governed_holdout_outcome_data_acquisition_manifest(tmp_path, created_at=NOW)

    rows = report["holdout_outcome_acquisition_manifest"]
    by_pair = {(row["family_id"], row["candidate_id"]): row for row in rows}
    assert ("family_blocked", "candidate_blocked") in by_pair
    assert ("family_status_only", "candidate_status_only") in by_pair
    assert ("family_only", "") in by_pair
    required_missing = set(by_pair[("family_blocked", "candidate_blocked")]["missing_fields"].split(","))
    assert {"return_observed", "return_window", "split_date"}.issubset(required_missing)
    assert report["summary"]["families_requiring_outcome_data"] == 3
    assert report["summary"]["candidates_requiring_outcome_data"] == 2


def test_build_112_emits_schema_contract_governance_and_no_replay_boundary(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_governed_holdout_outcome_data_acquisition_manifest(tmp_path, created_at=NOW)

    contract_fields = [row["field_name"] for row in report["holdout_outcome_schema_contract"]]
    assert contract_fields == REQUIRED_SCHEMA_FIELDS
    rules = "\n".join(report["governance_rules"])
    assert "no synthetic return_observed" in rules
    assert "no synthetic split_date" in rules
    assert "no synthetic timestamps" in rules
    assert "confidence impact NONE" in rules
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["holdout_replay_run"] is False
    assert report["authority_boundary"]["market_outcome_scoring"] is False


def test_build_112_writes_required_files_and_is_deterministic(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    first = build_governed_holdout_outcome_data_acquisition_manifest(tmp_path, created_at=NOW)
    second = build_governed_holdout_outcome_data_acquisition_manifest(tmp_path, created_at=NOW)
    assert first == second

    paths = write_governed_holdout_outcome_data_acquisition_manifest(first, tmp_path)
    for key in [
        "latest_json",
        "latest_summary",
        "holdout_outcome_acquisition_manifest",
        "holdout_outcome_schema_contract",
        "holdout_governance_rules",
        "post_import_validation_steps",
    ]:
        assert paths[key].exists()
    governance = paths["holdout_governance_rules"].read_text(encoding="utf-8")
    assert "no lookahead leakage" in governance
