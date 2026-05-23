from __future__ import annotations

from pathlib import Path

from research_lab.projections.hypothesis_queue_projection import build_hypothesis_queue_projection
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


def _proposal(pid: str = "ehp_test") -> dict:
    return {
        "hypothesis_proposal_id": pid,
        "intent_candidate_id": "icand_test",
        "event_cluster_id": "ecl_test",
        "event_family_id": "oil_shock",
        "title": "Oil shock proposal",
        "hypothesis": "Oil shocks may reverse or continue.",
        "proposal_status": "proposed",
        "governance_classification": "experimental",
        "data_requirement_status": "unknown",
        "proposed_universe": ["USO", "XLE"],
        "created_at": "2026-05-20T00:00:00Z",
        "content_hash": "hash",
        "research_label": "RESEARCH_ONLY",
    }


def _store_with_proposal(tmp_path: Path, pid: str = "ehp_test") -> Path:
    store = ensure_store_layout(tmp_path)
    proposal = _proposal(pid)
    write_json(store / "event_intake" / "hypothesis_proposals" / f"{pid}.json", proposal, overwrite=True)
    append_jsonl(store / "registries" / "hypothesis_proposals.jsonl", {k: proposal[k] for k in ["hypothesis_proposal_id", "intent_candidate_id", "event_cluster_id", "event_family_id", "proposal_status", "governance_classification", "data_requirement_status", "content_hash", "created_at"]})
    return store


def test_projection_builder_loads_registry_rows_and_artifacts(tmp_path: Path) -> None:
    store = _store_with_proposal(tmp_path)
    result = build_hypothesis_queue_projection(store_root=store)
    projection = result["projection"]
    assert projection["source_summary"]["hypothesis_proposal_registry_row_count"] == 1
    assert projection["source_summary"]["event_intake_proposal_artifact_count"] == 1
    assert projection["items"][0]["item_id"] == "ehp_test"


def test_oil_shock_proposal_appears_in_default_projection() -> None:
    result = build_hypothesis_queue_projection()
    items = result["projection"]["items"]
    oil = next(item for item in items if item["item_id"] == "ehp_cdbd8fe683acb622")
    assert oil["provenance"]["event_family_id"] == "oil_shock"
    assert oil["priority_bucket"] == "high"
    assert oil["readiness"]["ready_for_research"] is True
    assert oil["blocking_items"] == []


def test_watchlist_review_places_unblocked_proposal_in_watchlist(tmp_path: Path) -> None:
    store = _store_with_proposal(tmp_path)
    review = {"hypothesis_proposal_review_id": "hprev_test", "hypothesis_proposal_id": "ehp_test", "review_decision": "watchlist", "next_status": "watchlist", "reviewed_at": "2026-05-20T01:00:00Z", "content_hash": "rh"}
    write_json(store / "research_intake" / "reviews" / "hprev_test.json", review, overwrite=True)
    append_jsonl(store / "registries" / "hypothesis_proposal_reviews.jsonl", review)
    projection = build_hypothesis_queue_projection(store_root=store)["projection"]
    assert projection["items"][0]["lane"] == "watchlist"


def test_blocking_readiness_places_proposal_in_needs_data(tmp_path: Path) -> None:
    store = _store_with_proposal(tmp_path)
    readiness = {"research_readiness_assessment_id": "rra_test", "hypothesis_proposal_id": "ehp_test", "event_family_id": "oil_shock", "ready_for_research": False, "data_requirement_status": "partially_available", "blocking_items": ["missing_required_symbols"], "missing_symbols": ["USO"], "required_symbols": ["USO"], "next_allowed_actions": ["enable_required_data"], "assessed_at": "2026-05-20T01:00:00Z", "content_hash": "rh"}
    write_json(store / "research_intake" / "readiness_assessments" / "rra_test.json", readiness, overwrite=True)
    append_jsonl(store / "registries" / "research_readiness_assessments.jsonl", readiness)
    projection = build_hypothesis_queue_projection(store_root=store)["projection"]
    assert projection["items"][0]["lane"] == "needs_data"
    assert "blocked" in projection["items"][0]["display_flags"]


def test_missing_artifact_creates_needs_review_item(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    append_jsonl(store / "registries" / "hypothesis_proposals.jsonl", {"hypothesis_proposal_id": "ehp_missing", "proposal_status": "proposed", "created_at": "2026-05-20T00:00:00Z"})
    item = build_hypothesis_queue_projection(store_root=store)["projection"]["items"][0]
    assert item["lane"] == "needs_review"
    assert "missing_artifact" in item["display_flags"]


def test_artifact_without_registry_creates_needs_review_item(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    write_json(store / "event_intake" / "hypothesis_proposals" / "ehp_orphan.json", _proposal("ehp_orphan"), overwrite=True)
    projection = build_hypothesis_queue_projection(store_root=store)["projection"]
    item = projection["items"][0]
    assert item["lane"] == "needs_review"
    assert "integrity_warning" in item["display_flags"]


def test_malformed_review_warns_and_item_remains_visible(tmp_path: Path) -> None:
    store = _store_with_proposal(tmp_path)
    append_jsonl(store / "registries" / "hypothesis_proposal_reviews.jsonl", {"hypothesis_proposal_review_id": "hprev_bad", "hypothesis_proposal_id": "ehp_test", "review_decision": "watchlist", "next_status": "watchlist", "reviewed_at": "2026-05-20T01:00:00Z"})
    (store / "research_intake" / "reviews" / "hprev_bad.json").write_text("{bad json", encoding="utf-8")
    projection = build_hypothesis_queue_projection(store_root=store)["projection"]
    assert projection["items"][0]["item_id"] == "ehp_test"
    assert projection["integrity_status"] == "warning"
    assert projection["lanes"]["watchlist"] or projection["lanes"]["needs_review"]
