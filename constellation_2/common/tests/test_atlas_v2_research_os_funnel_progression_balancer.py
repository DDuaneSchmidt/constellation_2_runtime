from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.funnel_progression_balancer import select_next_funnel_balanced_item
from constellation_2.common.atlas_v2_research_os.funnel_progression_models import FunnelBalancingPolicy, FunnelStageGroup, mark_same_session_continuation_metadata
from constellation_2.common.atlas_v2_research_os.overnight_research_review import OvernightResearchReviewProfile, run_overnight_research_review
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog
from constellation_2.common.atlas_v2_research_os.historical_replay_engine import create_historical_replay_request, run_historical_replay
from constellation_2.common.atlas_v2_research_os.autonomous_research_execution import _artifact_safe_replay_result

NOW = "2026-06-05T00:00:00Z"


def _question(root: Path, artifact_id: str) -> str:
    ArtifactStore(root).create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.QUESTION.value,
        created_at=NOW,
        created_by="test",
        confidence=0.4,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"question": artifact_id, "mechanism_family": "MEAN_REVERSION"},
        is_root=True,
    )
    return artifact_id


def _claim(root: Path, artifact_id: str) -> str:
    ArtifactStore(root).create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.GENERATED_RESEARCH_CLAIM.value,
        created_at=NOW,
        created_by="test",
        confidence=0.5,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"claim_text": artifact_id, "mechanism_family": "MEAN_REVERSION"},
        is_root=True,
    )
    return artifact_id


def _hypothesis(root: Path, artifact_id: str) -> str:
    claim_id = _claim(root, f"claim-for-{artifact_id}")
    ArtifactStore(root).create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value,
        created_at=NOW,
        created_by="test",
        source_artifact_ids=[claim_id],
        confidence=0.5,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"atlas_component_payload": {"hypothesis_id": artifact_id, "hypothesis_text": "test", "mechanism_family": "MEAN_REVERSION"}},
    )
    return artifact_id



def _replay(root: Path, hypothesis_id: str, artifact_id: str = "replay-1") -> str:
    request = create_historical_replay_request(hypothesis_id=hypothesis_id, mechanism_tags=["MEAN_REVERSION"], source_artifact_ids=[hypothesis_id], replay_id=artifact_id, created_at=NOW)
    replay = run_historical_replay(request, [{"return": value} for value in [0.02, 0.01, 0.015, -0.002, 0.018, 0.012]], created_at=NOW)
    ArtifactStore(root).create_artifact(artifact_id=artifact_id, artifact_type="HistoricalReplayResult", created_at=NOW, created_by="test", source_artifact_ids=[hypothesis_id], confidence=float(replay["metrics"]["historical_replay_score"]), evidence_level="HISTORICAL_REPLAY", labels=["historical_replay"], metadata={"historical_replay_result": _artifact_safe_replay_result(replay), "research_only": True})
    return artifact_id

def _backlog(root: Path, item_id: str, item_type: str, source_ids: list[str], score: float, metadata: dict | None = None):
    return ResearchBacklog(root).create_backlog_item(
        backlog_item_id=item_id,
        item_type=item_type,
        title=item_id,
        description="test",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=source_ids,
        expected_learning_value=score,
        metadata=metadata or {},
    )


def test_claim_stage_cannot_consume_all_runs_when_hypothesis_ready(tmp_path: Path):
    claim_id = _claim(tmp_path, "claim-1")
    question_id = _question(tmp_path, "question-1")
    _backlog(tmp_path, "claim-work", "CLAIM_INVESTIGATION", [question_id], 2.0)
    _backlog(tmp_path, "hyp-work", "HYPOTHESIS_VALIDATION", [claim_id], 0.8)
    result = select_next_funnel_balanced_item(
        ResearchBacklog(tmp_path).get_ready_items(),
        runs_completed=2,
        runs_remaining=2,
        stage_counts_so_far={FunnelStageGroup.CLAIM_STAGE.value: 2},
        root=tmp_path,
    )
    assert result.selected_backlog_item["backlog_item_id"] == "hyp-work"
    assert result.selection_reason in {"RESERVE_HYPOTHESIS_STAGE_CAPACITY", "MAX_CLAIM_STAGE_SHARE_ENFORCED"}


def test_same_session_hypothesis_continuation_gets_priority_boost(tmp_path: Path):
    claim_id = _claim(tmp_path, "claim-1")
    question_id = _question(tmp_path, "question-1")
    _backlog(tmp_path, "claim-work", "CLAIM_INVESTIGATION", [question_id], 5.0)
    metadata = mark_same_session_continuation_metadata({}, execution_id="exec-1", continuation_depth=1)
    _backlog(tmp_path, "hyp-work", "HYPOTHESIS_VALIDATION", [claim_id], 0.1, metadata=metadata)
    result = select_next_funnel_balanced_item(ResearchBacklog(tmp_path).get_ready_items(), runs_completed=1, runs_remaining=3, stage_counts_so_far={FunnelStageGroup.CLAIM_STAGE.value: 1}, root=tmp_path)
    assert result.selected_backlog_item["backlog_item_id"] == "hyp-work"
    assert result.selection_reason == "PREFER_DOWNSTREAM_SAME_SESSION_CONTINUATION"


def test_edge_qualification_selected_after_hypothesis_continuation(tmp_path: Path):
    hypothesis_id = _hypothesis(tmp_path, "hyp-1")
    question_id = _question(tmp_path, "question-1")
    _backlog(tmp_path, "claim-work", "CLAIM_INVESTIGATION", [question_id], 5.0)
    metadata = mark_same_session_continuation_metadata({}, execution_id="exec-2", continuation_depth=2)
    replay_id = _replay(tmp_path, hypothesis_id)
    _backlog(tmp_path, "edge-work", "EDGE_QUALIFICATION_REVIEW", [hypothesis_id, replay_id], 0.1, metadata=metadata)
    result = select_next_funnel_balanced_item(ResearchBacklog(tmp_path).get_ready_items(), runs_completed=2, runs_remaining=2, stage_counts_so_far={FunnelStageGroup.CLAIM_STAGE.value: 1, FunnelStageGroup.HYPOTHESIS_STAGE.value: 1}, root=tmp_path)
    assert result.selected_backlog_item["backlog_item_id"] == "edge-work"
    assert result.stage_group == FunnelStageGroup.QUALIFICATION_STAGE.value


def test_max_claim_stage_share_enforced(tmp_path: Path):
    claim_id = _claim(tmp_path, "claim-1")
    question_id = _question(tmp_path, "question-1")
    _backlog(tmp_path, "claim-work", "CLAIM_INVESTIGATION", [question_id], 10.0)
    _backlog(tmp_path, "hyp-work", "HYPOTHESIS_VALIDATION", [claim_id], 0.2)
    result = select_next_funnel_balanced_item(ResearchBacklog(tmp_path).get_ready_items(), runs_completed=4, runs_remaining=1, stage_counts_so_far={FunnelStageGroup.CLAIM_STAGE.value: 3, FunnelStageGroup.HYPOTHESIS_STAGE.value: 1}, root=tmp_path, policy=FunnelBalancingPolicy(max_claim_stage_share=0.5))
    assert result.selected_backlog_item["backlog_item_id"] == "hyp-work"
    assert result.selection_reason == "MAX_CLAIM_STAGE_SHARE_ENFORCED"


def test_incompatible_items_are_skipped_safely(tmp_path: Path):
    question_id = _question(tmp_path, "question-1")
    _backlog(tmp_path, "bad-hyp", "HYPOTHESIS_VALIDATION", [question_id], 10.0)
    result = select_next_funnel_balanced_item(ResearchBacklog(tmp_path).get_ready_items(), runs_completed=0, runs_remaining=1, root=tmp_path)
    assert result.selected_backlog_item is None
    assert result.selection_reason == "NO_COMPATIBLE_READY_BACKLOG_ITEM"
    assert result.skipped_item_count == 1


def test_no_forbidden_authority_created_by_policy():
    policy = FunnelBalancingPolicy().to_dict()
    assert policy["prefer_downstream_continuations"] is True
    assert "live_trading" not in policy
    assert "capital" not in policy
    assert "broker" not in policy
    assert "candidate_promotion" not in policy


def test_overnight_balancer_reaches_edge_qualification_from_one_claim(tmp_path: Path):
    question_id = _question(tmp_path, "question-1")
    _backlog(tmp_path, "claim-work", "CLAIM_INVESTIGATION", [question_id], 1.0)
    report = run_overnight_research_review(tmp_path, day="2026-06-05", profile=OvernightResearchReviewProfile(max_runs=4))
    assert report["metrics"]["claims_generated"] >= 1
    assert report["metrics"]["hypotheses_generated"] >= 1
    assert report["metrics"]["historical_replays_executed"] >= 1
    assert report["metrics"]["edge_qualifications_attempted"] >= 1
    assert report["metrics"]["paper_trade_candidates_created"] + report["metrics"]["rejected_candidates"] >= 1
    assert report["continuation_items_executed_same_session"] >= 2
    assert report["profile"]["live_trading_allowed"] is False
    assert report["profile"]["capital_authority_allowed"] is False
    assert report["profile"]["broker_execution_allowed"] is False
    assert report["profile"]["candidate_promotion_allowed"] is False
