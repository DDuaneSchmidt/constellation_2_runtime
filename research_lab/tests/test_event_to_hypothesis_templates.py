from pathlib import Path

import pytest

from research_lab.event_intake.event_intake_registry import (
    convert_hypothesis_proposal_to_research_plan,
    create_event_cluster,
    generate_hypothesis_proposal,
    generate_intent_candidate,
    list_hypothesis_proposals,
    review_hypothesis_proposal,
    seed_event_families,
    store_event_observation,
)
from research_lab.event_intake.event_observation import build_event_observation
from research_lab.storage.manifest_io import read_json, read_jsonl


def _make_chain(store: Path, family: str = "oil_shock") -> dict:
    seed_event_families(store_root=store, actor="Tester")
    observation = build_event_observation(
        event_family_id=family,
        title="Template observation",
        description="Research intake observation.",
        source_type="operator_observation",
        symbols_mentioned=["USO", "XLE", "SPY"],
        suspected_mechanism="headline shock",
        confidence_level="medium",
        observed_at="2026-05-20T00:00:00Z",
    )
    obs_result = store_event_observation(observation, store_root=store, actor="Tester")
    cluster_result = create_event_cluster(event_family_id=family, observation_ids=[observation["event_observation_id"]], cluster_title="Template cluster", store_root=store, actor="Tester")
    intent_result = generate_intent_candidate(event_cluster_id=cluster_result["event_cluster"]["event_cluster_id"], store_root=store, actor="Tester")
    proposal_result = generate_hypothesis_proposal(intent_candidate_id=intent_result["intent_candidate"]["intent_candidate_id"], store_root=store, actor="Tester")
    return {"observation": obs_result["event_observation"], "cluster": cluster_result["event_cluster"], "intent": intent_result["intent_candidate"], "proposal": proposal_result["hypothesis_proposal"]}


def test_oil_shock_template_creates_deterministic_hypothesis_proposal(tmp_path: Path) -> None:
    first = _make_chain(tmp_path / "a", "oil_shock")["proposal"]
    second = _make_chain(tmp_path / "b", "oil_shock")["proposal"]
    assert first["content_hash"] == second["content_hash"]
    assert "oil" in first["hypothesis"].lower()
    assert first["proposal_status"] == "proposed"


def test_volatility_spike_template_creates_deterministic_hypothesis_proposal(tmp_path: Path) -> None:
    first = _make_chain(tmp_path / "a", "volatility_spike")["proposal"]
    second = _make_chain(tmp_path / "b", "volatility_spike")["proposal"]
    assert first["content_hash"] == second["content_hash"]
    assert first["governance_classification"] == "experimental"
    assert "mean-reversion" in first["hypothesis"]


def test_breadth_collapse_proposal_marks_missing_or_future(tmp_path: Path) -> None:
    proposal = _make_chain(tmp_path / "store", "breadth_collapse")["proposal"]
    assert proposal["data_requirement_status"] == "missing_or_future"
    assert proposal["governance_classification"] == "data_required"


def test_proposal_review_is_append_only_and_registries_audit_append(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = _make_chain(store, "oil_shock")
    proposal = chain["proposal"]
    path = store / "event_intake" / "hypothesis_proposals" / f"{proposal['hypothesis_proposal_id']}.json"
    before = read_json(path)
    review_before = len(read_jsonl(store / "registries" / "hypothesis_proposal_reviews.jsonl"))
    audit_before = len(read_jsonl(store / "audit_log" / "audit_events.jsonl"))
    result = review_hypothesis_proposal(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], decision="watchlist", reason="Needs data enablement.", store_root=store, actor="Reviewer")
    assert read_json(path) == before
    assert result["review_row"]["decision"] == "watchlist"
    assert len(read_jsonl(store / "registries" / "hypothesis_proposal_reviews.jsonl")) == review_before + 1
    assert len(read_jsonl(store / "audit_log" / "audit_events.jsonl")) == audit_before + 1
    assert len(list_hypothesis_proposals(status="proposed", store_root=store)) == 1


def test_conversion_to_research_plan_requires_accepted_for_research(tmp_path: Path) -> None:
    proposal = _make_chain(tmp_path / "store", "oil_shock")["proposal"]
    with pytest.raises(RuntimeError, match="accepted_for_research"):
        convert_hypothesis_proposal_to_research_plan(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], approve=True, store_root=tmp_path / "store", actor="Reviewer")


def test_conversion_fails_if_required_data_unavailable(tmp_path: Path) -> None:
    store = tmp_path / "store"
    proposal = _make_chain(store, "oil_shock")["proposal"]
    review_hypothesis_proposal(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], decision="accepted_for_research", reason="Approved for formal research once data exists.", store_root=store, actor="Reviewer")
    with pytest.raises(RuntimeError, match="unavailable required data"):
        convert_hypothesis_proposal_to_research_plan(hypothesis_proposal_id=proposal["hypothesis_proposal_id"], approve=True, store_root=store, actor="Reviewer")


def test_no_broker_live_sleeve_execution_or_ohlcv_mutation_code_added() -> None:
    source_root = Path("research_lab/src/research_lab/event_intake")
    text = "\n".join(path.read_text(encoding="utf-8") for path in source_root.glob("*.py"))
    forbidden_code_markers = ["broker_client", "place_order", "submit_order", "optimizer", "allocate_capital", "sleeve_created\": True", "trade_created\": True"]
    for needle in forbidden_code_markers:
        assert needle not in text
    assert "canonical_daily_bars" not in text
