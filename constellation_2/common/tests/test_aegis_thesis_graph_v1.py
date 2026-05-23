from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.thesis_graph.thesis_graph_v1 import (
    SAFETY_FLAGS,
    apply_capture_outcome_to_thesis_v1,
    attach_investigation_to_thesis_v1,
    build_evidence_item_v1,
    build_thesis_graph_projection_v1,
    create_thesis_from_investigation_v1,
    link_intent_to_thesis_v1,
    replay_thesis_from_artifacts_v1,
    update_thesis_with_evidence_v1,
    write_evidence_artifact_v1,
    write_thesis_artifact_v1,
)


def _investigation(title: str = "AI Infrastructure Leadership") -> dict:
    return {
        "investigation_id": "inv-ai-infra-1",
        "title": title,
        "symbols": ["NVDA", "SMH", "SOXX"],
        "sleeves": ["C2_EVENT_DISLOCATION"],
        "confidence_score": 0.5,
        "input_market_data_snapshot_ids": ["md-snapshot-1"],
    }


def test_creating_thesis_from_investigation_persists_beyond_completion(tmp_path: Path) -> None:
    thesis = create_thesis_from_investigation_v1({**_investigation(), "status": "COMPLETED"}, generated_at="2026-05-22T12:00:00Z")
    paths = write_thesis_artifact_v1(truth_root=tmp_path, thesis=thesis, day_utc="2026-05-22")

    assert thesis["thesis_id"].startswith("ths-ai-infrastructure-leadership")
    assert thesis["thesis_state"] == "ACTIVE"
    assert thesis["linked_investigation_ids"] == ["inv-ai-infra-1"]
    assert Path(paths["json"]).exists()


def test_attaching_new_investigation_to_existing_thesis_is_append_only() -> None:
    thesis = create_thesis_from_investigation_v1(_investigation(), generated_at="2026-05-22T12:00:00Z")
    updated = attach_investigation_to_thesis_v1(thesis, {"investigation_id": "inv-ai-infra-2", "symbols": ["AVGO"]}, generated_at="2026-05-22T13:00:00Z")

    assert thesis["content_hash"] != updated["content_hash"]
    assert updated["linked_investigation_ids"] == ["inv-ai-infra-1", "inv-ai-infra-2"]
    assert "AVGO" in updated["related_symbols"]


def test_supporting_and_contradicting_evidence_move_confidence_deterministically() -> None:
    thesis = create_thesis_from_investigation_v1(_investigation())
    support = build_evidence_item_v1(thesis_id=thesis["thesis_id"], source="event_study", summary="NVDA earnings beat strengthened sector leadership.", evidence_direction="SUPPORTS", confidence_impact=0.15, linked_symbols=["NVDA"])
    strengthened = update_thesis_with_evidence_v1(thesis, [support], generated_at="2026-05-22T13:00:00Z")
    contradiction = build_evidence_item_v1(thesis_id=thesis["thesis_id"], source="volatility_signal", summary="Volatility shock contradicted continuation setup.", evidence_direction="CONTRADICTS", confidence_impact=0.2)
    weakened = update_thesis_with_evidence_v1(strengthened, [contradiction], generated_at="2026-05-22T14:00:00Z")

    assert strengthened["confidence_score"] > thesis["confidence_score"]
    assert strengthened["conviction_trend"] == "STRENGTHENING"
    assert weakened["confidence_score"] < strengthened["confidence_score"]
    assert weakened["conviction_trend"] in {"WEAKENING", "WATCHING"}


def test_intent_links_to_thesis_without_execution_affordance() -> None:
    thesis = create_thesis_from_investigation_v1(_investigation())
    linked = link_intent_to_thesis_v1({"intent_id": "intent-nvda-1", "symbol": "NVDA"}, thesis)

    assert linked["primary_thesis_id"] == thesis["thesis_id"]
    assert linked["thesis_confidence_at_creation"] == thesis["confidence_score"]
    assert linked["safety"]["broker_submit_transmit_allowed"] is False
    assert linked["safety"]["autonomous_execution_allowed"] is False


def test_capture_outcome_updates_thesis_history() -> None:
    thesis = create_thesis_from_investigation_v1(_investigation())
    updated, evidence = apply_capture_outcome_to_thesis_v1(thesis, {"capture_id": "cap-1", "outcome_id": "out-1", "status": "CAPTURE_COMPLETED", "summary": "Manual capture completed and tracked."}, generated_at="2026-05-22T15:00:00Z")

    assert evidence["evidence_type"] == "capture outcome"
    assert "cap-1" in updated["linked_capture_ids"]
    assert "out-1" in updated["linked_outcome_ids"]
    assert updated["confidence_score"] >= thesis["confidence_score"]


def test_thesis_timeline_reconstructs_from_immutable_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    thesis = create_thesis_from_investigation_v1(_investigation())
    support = build_evidence_item_v1(thesis_id=thesis["thesis_id"], source="sleeve_result", summary="Sleeve produced qualified candidate.", evidence_direction="SUPPORTS", confidence_impact=0.1)
    updated = update_thesis_with_evidence_v1(thesis, [support])
    write_thesis_artifact_v1(truth_root=root, thesis=thesis, day_utc="2026-05-22")
    write_evidence_artifact_v1(truth_root=root, evidence=support, day_utc="2026-05-22")
    write_thesis_artifact_v1(truth_root=root, thesis=updated, day_utc="2026-05-22")

    projection = build_thesis_graph_projection_v1(truth_root=root, day_utc="2026-05-22")
    replay = replay_thesis_from_artifacts_v1(prior_thesis=thesis, evidence_items=[support])

    assert projection["thesis_count"] == 1
    assert any(event["event_type"] == "THESIS_CONFIDENCE_RECALCULATION" for event in projection["runtime_timeline_events"])
    assert replay["replay_status"] == "PASS"
    assert replay["replayed_thesis"]["content_hash"] == updated["content_hash"]


def test_mission_control_surfaces_strengthening_weakening_and_blocked_theses(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    base = create_thesis_from_investigation_v1(_investigation("AI Infrastructure Leadership"))
    support = build_evidence_item_v1(thesis_id=base["thesis_id"], source="event_result", summary="Earnings beat supports leadership.", evidence_direction="SUPPORTS", confidence_impact=0.2)
    strengthened = update_thesis_with_evidence_v1(base, [support])
    weak_base = create_thesis_from_investigation_v1({**_investigation("Crowded Momentum Reversal"), "investigation_id": "inv-crowded-1"})
    contra = build_evidence_item_v1(thesis_id=weak_base["thesis_id"], source="market_data_signal", summary="Missing feed and contrary price action block confidence.", evidence_direction="CONTRADICTS", confidence_impact=0.3)
    weakened = update_thesis_with_evidence_v1(weak_base, [contra])
    for thesis in [strengthened, weakened]:
        write_thesis_artifact_v1(truth_root=root, thesis=thesis, day_utc="2026-05-22")
    for evidence in [support, contra]:
        write_evidence_artifact_v1(truth_root=root, evidence=evidence, day_utc="2026-05-22")

    projection = build_thesis_graph_projection_v1(truth_root=root, day_utc="2026-05-22")
    mission = projection["mission_control_thesis_summary"]

    assert mission["strengthening_theses"]
    assert mission["weakening_theses"]
    assert mission["theses_blocked_by_missing_data_or_governance"]
    assert projection["safety"] == SAFETY_FLAGS
