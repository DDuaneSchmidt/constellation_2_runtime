from __future__ import annotations

from pathlib import Path

from ops.aegis.research_lab.research_console_v1 import (
    blocked_evidence_v1,
    evidence_v1,
    paper_trials_v1,
    research_backlog_v1,
    research_console_v1,
    research_hypothesis_queue_v1,
    research_plans_v1,
    sleeve_review_center_v1,
)
from research_lab.projections.hypothesis_queue_projection import build_hypothesis_queue_projection
from research_lab.projections.operator_queue_projection import build_operator_queue_projection
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


def test_hypothesis_queue_api_reads_latest_projection_not_registries(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    projection = build_hypothesis_queue_projection(store_root=store)["projection"]
    projection["items"] = [{
        "item_id": "ehp_projection_only",
        "item_type": "Hypothesis Proposal",
        "title": "Projection Only",
        "summary": "Visible because projection says so.",
        "lane": "needs_review",
        "status": "needs_review",
        "priority_bucket": "watchlist",
        "readiness": {"ready_for_research": False, "data_requirement_status": "unknown", "missing_symbols": [], "required_symbols": []},
        "blocking_items": [],
        "recommended_next_action": "inspect_source_artifacts",
        "next_allowed_actions": ["rebuild_projection"],
        "source_refs": [],
        "provenance": {"event_family_id": "gap_event", "research_label_present": True},
        "created_at": "2026-05-20T00:00:00Z",
        "updated_at": "2026-05-20T00:00:00Z",
        "display_flags": ["needs_review"],
        "schema_version": "queue_item.v1",
    }]
    projection["lanes"] = {"proposed": [], "watchlist": [], "needs_data": [], "accepted_for_research": [], "rejected": [], "archived": [], "needs_review": projection["items"]}
    projection["integrity_status"] = "warning"
    write_json(store / "projections" / "hypothesis_queue" / "latest.json", projection, overwrite=True)
    payload = research_hypothesis_queue_v1(store_root=store)
    assert payload["projection_source"] == "latest.json"
    assert payload["queue"][0]["hypothesis_proposal_id"] == "ehp_projection_only"


def test_blocked_work_includes_blocked_hypothesis_from_projection(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    proposal = {"hypothesis_proposal_id": "ehp_test", "title": "Oil", "hypothesis": "Oil", "proposal_status": "proposed", "event_family_id": "oil_shock", "created_at": "2026-05-20T00:00:00Z", "content_hash": "h"}
    write_json(store / "event_intake" / "hypothesis_proposals" / "ehp_test.json", proposal, overwrite=True)
    append_jsonl(store / "registries" / "hypothesis_proposals.jsonl", {"hypothesis_proposal_id": "ehp_test", "proposal_status": "proposed", "created_at": "2026-05-20T00:00:00Z"})
    readiness = {"research_readiness_assessment_id": "rra", "hypothesis_proposal_id": "ehp_test", "ready_for_research": False, "data_requirement_status": "partially_available", "blocking_items": ["missing_required_symbols"], "missing_symbols": ["USO"], "required_symbols": ["USO"], "assessed_at": "2026-05-20T00:00:00Z"}
    write_json(store / "research_intake" / "readiness_assessments" / "rra.json", readiness, overwrite=True)
    append_jsonl(store / "registries" / "research_readiness_assessments.jsonl", readiness)
    build_hypothesis_queue_projection(store_root=store)
    build_operator_queue_projection(store_root=store)
    blocked = blocked_evidence_v1(store_root=store)
    assert blocked["blocked_items"][0]["what_is_blocked"] == "ehp_test"


def test_operator_home_shows_projection_integrity_warnings(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    write_json(store / "event_intake" / "hypothesis_proposals" / "ehp_orphan.json", {"hypothesis_proposal_id": "ehp_orphan", "title": "Orphan", "hypothesis": "Orphan", "proposal_status": "proposed", "event_family_id": "gap_event"}, overwrite=True)
    build_hypothesis_queue_projection(store_root=store)
    home = research_console_v1(store_root=store)
    assert home["projection_summary"]["projection_integrity_status"] == "source_missing"
    assert home["what_needs_attention"]["top_hypothesis_to_review"]["title"] == "Research queue projection needs rebuild/inspection"



def test_operator_console_pages_read_operator_projection_latest(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    build_hypothesis_queue_projection(store_root=store)
    result = build_operator_queue_projection(store_root=store)
    projection = result["projection"]
    projection["queues"]["research_plans"] = [{"research_plan_id": "rp_projection_only", "hypothesis": "Projection plan", "status": "accepted_for_research"}]
    projection["queues"]["evidence"] = [{"evidence_package_id": "ev_projection_only", "tab": "Backtests", "hypothesis": "Projection evidence"}]
    projection["queues"]["paper_trials"] = [{"paper_trial_id": "pt_projection_only", "recommended_next_action": "Measure Outcomes"}]
    projection["queues"]["sleeve_reviews"] = [{"sleeve_id": "slv_projection_only", "sleeve_name": "Projection sleeve"}]
    projection["queues"]["blocked_work"] = [{"what_is_blocked": "projection_blocker", "blocking_items": ["missing_required_symbols"]}]
    projection["queues"]["research_backlog"] = [{"rank": 1, "label": "Projection backlog", "type": "Hypothesis Proposal"}]
    write_json(store / "projections" / "operator_home" / "latest.json", projection, overwrite=True)

    assert research_plans_v1(store_root=store)["research_plans"][0]["research_plan_id"] == "rp_projection_only"
    assert evidence_v1(store_root=store)["evidence"][0]["evidence_package_id"] == "ev_projection_only"
    assert paper_trials_v1(store_root=store)["paper_trials"][0]["paper_trial_id"] == "pt_projection_only"
    assert sleeve_review_center_v1(store_root=store)["sleeves"][0]["sleeve_id"] == "slv_projection_only"
    assert blocked_evidence_v1(store_root=store)["blocked_items"][0]["what_is_blocked"] == "projection_blocker"
    assert research_backlog_v1(store_root=store)["backlog"][0]["label"] == "Projection backlog"


def test_operator_console_pages_do_not_false_empty_without_projection(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    payload = research_plans_v1(store_root=store)
    assert payload["projection_source"] == "missing"
    assert payload["integrity_status"] == "source_missing"
    assert payload["empty_state"]["research_plans"]["trusted_empty"] is False
