from __future__ import annotations

from pathlib import Path

from research_lab.portfolio.evidence_inventory import build_evidence_inventory, write_evidence_inventory
from research_lab.sleeves.sleeve_challenger import build_sleeve_challenge, store_sleeve_challenge
from research_lab.sleeves.sleeve_health import build_sleeve_health_snapshot, store_sleeve_health_snapshot
from research_lab.sleeves.sleeve_review import build_sleeve_review, store_sleeve_review
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.tests.test_sleeve_health_snapshot import _write_health_fixture


def _portfolio_fixture(store: Path) -> tuple[str, str]:
    sleeve_id, version_id = _write_health_fixture(store)
    health = build_sleeve_health_snapshot(sleeve_id=sleeve_id, sleeve_version_id=version_id, as_of_date="2024-01-02", store_root=store, created_at="2024-01-02T00:00:00Z")
    store_sleeve_health_snapshot(health, store_root=store)
    challenge = build_sleeve_challenge(sleeve_id=sleeve_id, sleeve_version_id=version_id, sleeve_health_snapshot_id=health["sleeve_health_snapshot_id"], store_root=store, created_at="2024-01-02T00:00:00Z")
    store_sleeve_challenge(challenge, store_root=store)
    review = build_sleeve_review(sleeve_id=sleeve_id, sleeve_version_id=version_id, sleeve_challenge_id=challenge["sleeve_challenge_id"], review_decision="continue", review_reason="fixture", reviewed_by="pytest", store_root=store, reviewed_at="2024-01-03T00:00:00Z")
    store_sleeve_review(review, store_root=store)
    trial_id = "ptr_fixture"
    write_json(
        store / "paper_trials" / trial_id / "paper_trial_summary.json",
        {
            "paper_trial_id": trial_id,
            "sleeve_id": sleeve_id,
            "sleeve_version_id": version_id,
            "status": "active",
            "observation_count": 2,
            "candidate_count_total": 0,
            "zero_candidate_observation_count": 2,
            "measured_outcome_count": 0,
            "latest_review_decision": "continue",
            "next_allowed_actions": ["record-paper-observation"],
            "research_label": "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance.",
            "schema_version": "paper_trial_summary.v1",
        },
        overwrite=False,
    )
    write_json(
        store / "paper_trials" / trial_id / "paper_trial_operations_report.json",
        {
            "paper_trial_id": trial_id,
            "status": "active",
            "observation_count": 2,
            "candidate_count_total": 0,
            "zero_candidate_observation_count": 2,
            "pending_due_outcomes": 0,
            "due_outcomes": 0,
            "measured_due_outcomes": 1,
            "unavailable_due_outcomes": 4,
            "measured_candidate_count": 0,
            "latest_observation_date": "2024-01-02T00:00:00Z",
            "latest_outcome_measurement_date": "2024-01-03T00:00:00Z",
            "latest_review_decision": "continue",
            "recommended_next_action": "record_more_observations",
            "next_allowed_actions": ["record-next-paper-observation"],
            "research_label": "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance.",
            "created_at": "2024-01-03T00:00:00Z",
            "schema_version": "paper_trial_operations_report.v1",
            "content_hash": "a" * 64,
        },
        overwrite=False,
    )
    append_jsonl(
        store / "registries" / "longitudinal_candidate_runs.jsonl",
        {
            "longitudinal_run_id": "lcr_fixture",
            "sleeve_id": sleeve_id,
            "sleeve_version_id": version_id,
            "frequency": "weekly",
            "candidate_count": 82,
            "created_at": "2024-01-03T00:00:00Z",
            "schema_version": "longitudinal_candidate_run.v1",
        },
    )
    append_jsonl(
        store / "registries" / "sleeve_learning_reports.jsonl",
        {
            "sleeve_learning_report_id": "slrn_fixture",
            "sleeve_id": sleeve_id,
            "sleeve_version_id": version_id,
            "ranking_quality_status": "useful",
            "recommended_next_action": "continue_research",
            "created_at": "2024-01-03T00:00:00Z",
            "content_hash": "b" * 64,
            "schema_version": "sleeve_learning_report.v1",
        },
    )
    write_json(
        store / "longitudinal_runs" / "lcr_fixture" / "sleeve_learning_report.json",
        {
            "sleeve_learning_report_id": "slrn_fixture",
            "sleeve_id": sleeve_id,
            "sleeve_version_id": version_id,
            "longitudinal_runs": ["lcr_fixture"],
            "total_candidate_batches": 10,
            "total_candidates": 82,
            "measured_candidates": 65,
            "zero_candidate_batches": 4,
            "ranking_quality_status": "useful",
            "current_learning_status": "learning_sample_available",
            "recommended_next_action": "continue_research",
            "created_at": "2024-01-03T00:00:00Z",
            "schema_version": "sleeve_learning_report.v1",
            "content_hash": "c" * 64,
        },
        overwrite=False,
    )
    return sleeve_id, version_id


def test_evidence_inventory_includes_current_sleeve_refs(tmp_path: Path) -> None:
    store = tmp_path / "store"
    sleeve_id, _ = _portfolio_fixture(store)

    inventory = build_evidence_inventory(store_root=store, created_at="2024-01-04T00:00:00Z")

    assert inventory["sleeve_count"] == 1
    sleeve = inventory["sleeves"][0]
    assert sleeve["sleeve_id"] == sleeve_id
    assert sleeve["event_study_evidence_ids"] == ["ev_event"]
    assert sleeve["backtest_evidence_ids"] == ["ev_bt"]
    assert sleeve["candidate_batch_ids"] == ["cb_fixture"]
    assert sleeve["longitudinal_run_ids"] == ["lcr_fixture"]
    assert sleeve["paper_trial_ids"] == ["ptr_fixture"]
    assert "No broker execution" in inventory["research_label"]


def test_evidence_inventory_reports_missing_optional_artifact(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)
    (store / "paper_trials" / "ptr_fixture" / "paper_trial_operations_report.json").unlink()

    inventory = build_evidence_inventory(store_root=store, created_at="2024-01-04T00:00:00Z")

    assert inventory["sleeves"][0]["missing_artifacts"]


def test_evidence_inventory_write_is_immutable_report_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)
    sleeve_path = store / "sleeves" / "slv_fixture" / "sleeve_definition.json"
    before = sleeve_path.read_text(encoding="utf-8")

    report = write_evidence_inventory(store_root=store)

    assert (store / "portfolio_reports" / "evidence_inventory" / f"{report['evidence_inventory_id']}.json").exists()
    assert sleeve_path.read_text(encoding="utf-8") == before
