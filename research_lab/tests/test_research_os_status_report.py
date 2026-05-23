from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_status_latest_v1,
    research_lab_status_report_v1,
    research_lab_status_reports_v1,
)
from research_lab.integrity.research_store_integrity import write_research_store_integrity_report
from research_lab.status.research_os_status import build_research_os_status_report, write_research_os_status_report
from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import append_jsonl, read_jsonl, write_json
from research_lab.tests.test_research_store_integrity import _fixture_store


def _failed_integrity_fixture(store: Path) -> dict[str, str]:
    ids = _fixture_store(store)
    paper_trial_id = "ptr_status_fixture"
    paper_trial = {
        "paper_trial_id": paper_trial_id,
        "sleeve_id": "slv_etf_drop_reversion_v1",
        "sleeve_version_id": "slvv_slv_etf_drop_reversion_v1_v1_1c36b58d9b",
        "hypothesis_id": "hyp_fixture",
        "dataset_snapshot_id": "ds_fixture",
        "regime_snapshot_id": "rs_fixture",
        "cost_model_snapshot_id": "cm_fixture",
        "source_evidence_package_ids": [],
        "candidate_generator_name": "fixture",
        "candidate_generator_version": "v1",
        "ranking_policy_version": "fixture",
        "threshold": 0.0,
        "observation_frequency": "weekly",
        "outcome_windows": [5],
        "status": "active",
        "started_at": "2026-05-19T00:00:00Z",
        "created_at": "2026-05-19T00:00:00Z",
        "created_by": "test",
        "schema_version": "paper_trial.v1",
        "content_hash": "",
    }
    paper_trial["content_hash"] = content_hash(paper_trial, exclude={"content_hash"}, sort_lists=True)
    write_json(store / "paper_trials" / paper_trial_id / "paper_trial.json", paper_trial, overwrite=False)
    registry = store / "registries" / "paper_trials.jsonl"
    row = {"paper_trial_id": paper_trial_id}
    append_jsonl(registry, row)
    append_jsonl(registry, row)
    integrity = write_research_store_integrity_report(store_root=store)["report"]
    ids["integrity_id"] = integrity["integrity_report_id"]
    return ids


def _store_counts(store: Path) -> dict[str, int]:
    paths = {
        "paper_trials": store / "paper_trials",
        "candidate_batches": store / "candidate_batches",
        "challenger_evidence_batches": store / "challenger_evidence_batches",
        "human_review_decisions": store / "human_review_decisions",
        "status_reports": store / "status_reports",
        "audit": store / "audit_log" / "audit_events.jsonl",
    }
    return {
        key: (len(list(path.rglob("*"))) if path.is_dir() else len(read_jsonl(path)))
        for key, path in paths.items()
        if path.exists()
    }


def test_status_from_failed_integrity_report_is_red_and_surfaces_duplicate(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _failed_integrity_fixture(store)

    report = build_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")

    assert report["overall_status"] == "RED"
    assert report["readiness_level"] == "not_ready"
    assert report["latest_integrity_report_id"] == ids["integrity_id"]
    assert report["integrity_status"] == "FAIL"
    assert report["integrity_error_count"] == 1
    assert report["duplicate_registry_issue_count"] == 1
    assert report["lineage_warning_count"] > 0
    assert any(row.get("registry_name") == "paper_trials.jsonl" for row in report["current_blockers"])
    assert report["paper_trial_summary"]["paper_trial_proposal_eligible"] is False


def test_latest_artifacts_are_resolved_into_status_report(tmp_path: Path) -> None:
    store = tmp_path / "store"
    ids = _failed_integrity_fixture(store)

    report = build_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")

    assert report["latest_challenger_evidence_batch_id"] == ids["batch_id"]
    assert report["latest_challenger_comparison_report_id"] == ids["comparison_id"]
    assert report["latest_human_review_dossier_id"] == ids["dossier_id"]
    assert report["latest_human_review_decision_id"] == ids["decision_id"]
    assert report["materialized_challenger_count"] >= 1
    assert report["human_review_summary"]["latest_decision"] == "request_more_challenger_evidence"


def test_packet23_persistence_registry_audit_api_and_no_lifecycle_mutation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)
    before = _store_counts(store)

    result = write_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:01Z")
    after = _store_counts(store)

    assert after["paper_trials"] == before["paper_trials"]
    assert after["candidate_batches"] == before["candidate_batches"]
    assert after["challenger_evidence_batches"] == before["challenger_evidence_batches"]
    assert after["human_review_decisions"] == before["human_review_decisions"]
    assert after["status_reports"] > before.get("status_reports", 0)
    assert after["audit"] == before["audit"] + 1
    assert read_jsonl(store / "registries" / "research_os_status_report_registry.json")[-1]["research_os_status_report_id"] == result["report"]["research_os_status_report_id"]
    assert result["audit_event"]["action"] == "research_os_status_report_generated"

    listing = research_lab_status_reports_v1(store_root=store)
    latest = research_lab_status_latest_v1(store_root=store)
    detail = research_lab_status_report_v1(research_os_status_report_id=result["report"]["research_os_status_report_id"], store_root=store)
    assert listing["read_only"] is True
    assert latest["status_report"]["research_os_status_report_id"] == result["report"]["research_os_status_report_id"]
    assert detail["status_report"]["research_os_status_report_id"] == result["report"]["research_os_status_report_id"]


def test_status_fingerprint_is_deterministic_except_timestamp_and_report_id(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _failed_integrity_fixture(store)

    first = build_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:00Z")
    second = build_research_os_status_report(store_root=store, generated_at="2026-05-19T00:00:02Z")

    assert first["research_os_status_report_id"] != second["research_os_status_report_id"]
    assert first["immutable_hash"] == second["immutable_hash"]
