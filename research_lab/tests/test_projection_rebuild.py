from __future__ import annotations

from pathlib import Path

from research_lab.projections.projection_builder import rebuild_research_projections
from research_lab.storage.manifest_io import append_jsonl, read_json, write_json
from research_lab.storage.paths import ensure_store_layout


def test_rebuild_command_writes_latest_and_build_record(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    proposal = {"hypothesis_proposal_id": "ehp_test", "title": "Test", "hypothesis": "Test", "proposal_status": "proposed", "event_family_id": "gap_event", "created_at": "2026-05-20T00:00:00Z", "content_hash": "h"}
    write_json(store / "event_intake" / "hypothesis_proposals" / "ehp_test.json", proposal, overwrite=True)
    append_jsonl(store / "registries" / "hypothesis_proposals.jsonl", {"hypothesis_proposal_id": "ehp_test", "proposal_status": "proposed", "created_at": "2026-05-20T00:00:00Z"})
    result = rebuild_research_projections(projection="hypothesis_queue", store_root=store)
    assert result["ok"] is True
    assert (store / "projections" / "hypothesis_queue" / "latest.json").exists()
    assert (store / "registries" / "projection_builds.jsonl").exists()



def test_rebuild_all_writes_operator_projection_and_combined_health(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    for filename in [
        "hypothesis_proposals.jsonl",
        "hypothesis_proposal_reviews.jsonl",
        "research_readiness_assessments.jsonl",
        "proposal_priority_scores.jsonl",
        "research_intake_dossiers.jsonl",
        "research_plans.jsonl",
        "evidence_packages.jsonl",
        "paper_trials.jsonl",
        "paper_trial_operations_reports.jsonl",
        "sleeve_definitions.jsonl",
        "sleeve_reviews.jsonl",
        "sleeve_health_snapshots.jsonl",
    ]:
        (store / "registries" / filename).write_text("", encoding="utf-8")
    result = rebuild_research_projections(projection="all", store_root=store)
    assert result["ok"] is True
    assert (store / "projections" / "hypothesis_queue" / "latest.json").exists()
    assert (store / "projections" / "operator_home" / "latest.json").exists()
    health = read_json(store / "projections" / "projection_health" / "latest.json")
    assert "hypothesis_queue" in health["projections"]
    assert "operator_home" in health["projections"]
