from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.hypothesis_proposal_promotion_v1 import (
    build_approval_queue_v1,
    SAFETY_STATEMENT,
    approval_events_path_v1,
    build_all_hypothesis_proposal_promotion_v1,
    promotion_pipeline_path_v1,
    record_paper_promotion_approval_event_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _seed_proposal(repo: Path, proposal_id: str, title: str, *, content_hash: str, family: str = "oil_shock", symbols: list[str] | None = None, blockers: list[str] | None = None, ready: bool = True) -> None:
    store = repo / "research_lab" / "research_store"
    proposal = {
        "schema_version": "event_hypothesis_proposal.v1",
        "hypothesis_proposal_id": proposal_id,
        "intent_candidate_id": f"intent-{proposal_id}",
        "event_cluster_id": f"cluster-{proposal_id}",
        "event_family_id": family,
        "title": title,
        "hypothesis": title,
        "proposal_status": "proposed",
        "data_requirement_status": "available",
        "governance_classification": "experimental",
        "content_hash": content_hash,
        "created_at": "2026-06-01T00:00:00Z",
        "research_label": "RESEARCH_ONLY",
    }
    _write_json(store / "event_intake" / "hypothesis_proposals" / f"{proposal_id}.json", proposal)
    _write_json(
        store / "event_intake" / "clusters" / f"cluster-{proposal_id}.json",
        {"event_cluster_id": f"cluster-{proposal_id}", "common_symbols": symbols or ["SPY", "USO", "XLE"], "research_label": "RESEARCH_ONLY"},
    )
    _append_jsonl(
        store / "registries" / "hypothesis_proposals.jsonl",
        {
            "hypothesis_proposal_id": proposal_id,
            "intent_candidate_id": f"intent-{proposal_id}",
            "event_cluster_id": f"cluster-{proposal_id}",
            "event_family_id": family,
            "proposal_status": "proposed",
            "data_requirement_status": "available",
            "governance_classification": "experimental",
            "content_hash": content_hash,
            "created_at": "2026-06-01T00:00:00Z",
            "schema_version": "event_hypothesis_proposal.v1",
            "storage_uri": f"research://event_intake/hypothesis_proposals/{proposal_id}.json",
        },
    )
    readiness_id = f"ready-{proposal_id}"
    _write_json(
        store / "research_intake" / "readiness_assessments" / f"{readiness_id}.json",
        {
            "hypothesis_proposal_id": proposal_id,
            "research_readiness_assessment_id": readiness_id,
            "ready_for_research": ready,
            "intraday_market_data_available": ready,
            "blocking_items": blockers or [],
            "required_symbols": symbols or ["SPY", "USO", "XLE"],
            "research_label": "RESEARCH_ONLY",
        },
    )
    _append_jsonl(
        store / "registries" / "research_readiness_assessments.jsonl",
        {"hypothesis_proposal_id": proposal_id, "research_readiness_assessment_id": readiness_id, "storage_uri": f"research://research_intake/readiness_assessments/{readiness_id}.json"},
    )
    priority_id = f"priority-{proposal_id}"
    _write_json(store / "research_intake" / "priority_scores" / f"{priority_id}.json", {"hypothesis_proposal_id": proposal_id, "score": 9, "priority_bucket": "high"})
    _append_jsonl(store / "registries" / "proposal_priority_scores.jsonl", {"hypothesis_proposal_id": proposal_id, "proposal_priority_score_id": priority_id, "storage_uri": f"research://research_intake/priority_scores/{priority_id}.json"})


def test_viable_proposal_creates_paper_recommendation_and_no_sleeve_before_approval(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-oil", "Oil shock reversals across energy ETFs", content_hash="hash-oil")
    result = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    assert result["summary"]["promotion_recommendations_count"] == 1
    assert result["summary"]["approval_queue_count"] == 1
    state = result["proposal_states"][0]
    assert state["triage_decision"] == "READY_FOR_SHADOW_TRIAL"
    assert state["shadow_validation_result"] == "SHADOW_VALIDATION_PASSED"
    assert state["state"] == "PAPER_PROMOTION_RECOMMENDED"
    assert not (truth / "reports" / "aegis_paper_testing_sleeves_v1").exists()


def test_missing_data_proposal_becomes_needs_data(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-macro", "Macro calendar event dislocation watch", content_hash="hash-macro", family="macro_headline_shock", symbols=["SPY", "QQQ"], blockers=["missing_macro_event_calendar"], ready=False)
    result = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    assert result["summary"]["promotion_recommendations_count"] == 0
    assert result["proposal_states"][0]["state"] == "NEEDS_DATA"


def test_low_sample_rate_is_auto_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-slow", "Sparse macro event", content_hash="hash-slow", family="macro_headline_shock", symbols=["SPY", "QQQ"], ready=True)
    result = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    assert result["proposal_states"][0]["state"] == "REJECTED_LOW_SAMPLE_RATE"
    assert result["summary"]["promotion_recommendations_count"] == 0


def test_failed_shadow_trial_does_not_become_recommendation(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-unstable", "Unstable oil shock reversals", content_hash="hash-unstable")
    result = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    state = result["proposal_states"][0]
    assert state["triage_decision"] == "READY_FOR_SHADOW_TRIAL"
    assert state["shadow_validation_result"] == "SHADOW_VALIDATION_FAILED"
    assert state["state"] == "SHADOW_VALIDATION_FAILED"
    assert result["summary"]["promotion_recommendations_count"] == 0


def test_duplicate_and_untestable_proposals_are_auto_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-a", "Repeated hypothesis", content_hash="hash-a")
    _seed_proposal(repo, "ehp-b", "Repeated hypothesis", content_hash="hash-b")
    _seed_proposal(repo, "ehp-empty", "", content_hash="hash-empty", symbols=[])
    result = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    states = {row["hypothesis_id"]: row["state"] for row in result["proposal_states"]}
    assert states["ehp-b"] == "REJECTED_DUPLICATE"
    assert states["ehp-empty"] == "REJECTED_UNTESTABLE"


def test_rerun_determinism_and_stale_artifact_rejection(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    truth = tmp_path / "truth"
    _seed_proposal(repo, "ehp-stale", "Stale proposal", content_hash="registry-hash")
    artifact = repo / "research_lab/research_store/event_intake/hypothesis_proposals/ehp-stale.json"
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload["content_hash"] = "different-artifact-hash"
    _write_json(artifact, payload)
    first = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    first_hash = json.loads(promotion_pipeline_path_v1(truth_root=truth, day_utc="2026-06-01").read_text(encoding="utf-8"))["content_hash"]
    second = build_all_hypothesis_proposal_promotion_v1(truth_root=truth, day_utc="2026-06-01", repo_root=repo)
    second_hash = json.loads(promotion_pipeline_path_v1(truth_root=truth, day_utc="2026-06-01").read_text(encoding="utf-8"))["content_hash"]
    assert first == second
    assert first_hash == second_hash
    assert first["summary"]["stale_or_rejected_input_count"] == 1
    assert first["summary"]["proposal_count"] == 0


def test_approval_event_is_research_only_and_not_execution(tmp_path: Path) -> None:
    event = record_paper_promotion_approval_event_v1(
        truth_root=tmp_path,
        day_utc="2026-06-01",
        hypothesis_id="ehp-oil",
        proposal_id="ehp-oil",
        promotion_packet_hash="abc123",
        prior_state="PAPER_PROMOTION_RECOMMENDED",
        decision="APPROVED",
        actor="David / operator",
        generated_at_utc="2026-06-01T12:00:00Z",
    )
    assert event["approval_action"] == "APPROVE_PAPER_TEST"
    assert event["approval_decision"] == "APPROVED"
    assert event["actor"] == "David / operator"
    assert event["proposal_id"] == "ehp-oil"
    assert event["promotion_packet_hash"] == "abc123"
    assert event["prior_state"] == "PAPER_PROMOTION_RECOMMENDED"
    assert event["new_state"] == "PAPER_PROMOTION_APPROVED"
    assert event["approval_meaning"] == "Approve this hypothesis for paper research tracking."
    assert event["safety_statement"] == SAFETY_STATEMENT
    assert event["no_broker_execution"] is True
    assert event["no_trade_advice"] is True
    assert event["no_live_trading"] is True
    assert event["no_real_capital"] is True
    assert event["safety"]["trade_advice_allowed"] is False
    assert event["safety"]["broker_execution_allowed"] is False
    assert event["safety"]["live_trading_allowed"] is False
    assert event["safety"]["real_capital_allowed"] is False
    assert event["safety"]["automatic_paper_sleeve_creation_allowed"] is False
    assert approval_events_path_v1(truth_root=tmp_path, day_utc="2026-06-01").exists()


def test_approval_event_updates_queue_state_without_sleeve_or_safety_changes(tmp_path: Path) -> None:
    queue = build_approval_queue_v1({
        "promotion_packets": [{
            "promotion_decision": "PAPER_PROMOTION_RECOMMENDED",
            "hypothesis_id": "ehp-oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "promotion_packet_hash": "packet-hash",
            "shadow_validation_result": "SHADOW_VALIDATION_PASSED",
        }]
    }, truth_root=tmp_path, day_utc="2026-06-01")
    assert queue["summary"]["recommendation_count"] == 1
    record_paper_promotion_approval_event_v1(
        truth_root=tmp_path,
        day_utc="2026-06-01",
        hypothesis_id="ehp-oil",
        proposal_id="ehp-oil",
        promotion_packet_hash="packet-hash",
        prior_state="PAPER_PROMOTION_RECOMMENDED",
        decision="APPROVED",
        generated_at_utc="2026-06-01T12:00:00Z",
    )
    approved = build_approval_queue_v1({
        "promotion_packets": [{
            "promotion_decision": "PAPER_PROMOTION_RECOMMENDED",
            "hypothesis_id": "ehp-oil",
            "hypothesis_name": "Oil shock reversals across energy ETFs",
            "promotion_packet_hash": "packet-hash",
            "shadow_validation_result": "SHADOW_VALIDATION_PASSED",
        }]
    }, truth_root=tmp_path, day_utc="2026-06-01")
    assert approved["approval_queue"][0]["approval_state"] == "PAPER_PROMOTION_APPROVED"
    assert approved["approval_queue"][0]["available_actions"] == []
    assert approved["summary"]["recommendation_count"] == 0
    assert approved["summary"]["approved_count"] == 1
    assert approved["safety"]["broker_execution_allowed"] is False
    assert approved["safety"]["trade_advice_allowed"] is False
    assert approved["safety"]["automatic_paper_sleeve_creation_allowed"] is False
