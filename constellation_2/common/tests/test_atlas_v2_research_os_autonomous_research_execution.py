from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.autonomous_research_execution import (
    _artifact_safe_replay_result,
    dry_run_bounded_research,
    find_compatible_worker_for_backlog_item,
    run_bounded_research_once,
    validate_worker_for_backlog_item,
)
from constellation_2.common.atlas_v2_research_os.failure_observatory import list_failures
from constellation_2.common.atlas_v2_research_os.memory_index import list_memory_objects
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog
from constellation_2.common.atlas_v2_research_os.historical_replay_engine import create_historical_replay_request, run_historical_replay

NOW = "2026-06-05T00:00:00Z"


def _seed_question_backlog(root: Path, *, item_id: str = "bi1", score: float = 1.0, item_type: str = "CLAIM_INVESTIGATION"):
    store = ArtifactStore(root)
    artifact_id = f"q-{item_id}"
    store.create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.QUESTION.value,
        created_at=NOW,
        created_by="test",
        confidence=0.4,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"question": "When source-defined setup is observed, does follow-through separate from baseline?", "mechanism_family": "MEAN_REVERSION"},
        is_root=True,
    )
    item = ResearchBacklog(root).create_backlog_item(
        backlog_item_id=item_id,
        item_type=item_type,
        title="Claim work",
        description="Desc",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=[artifact_id],
        expected_learning_value=score,
    )
    return item, store.get_artifact(artifact_id)


def test_run_selects_highest_priority_ready_item_and_connected_worker(tmp_path: Path):
    _seed_question_backlog(tmp_path, item_id="low", score=0.1)
    high, artifact = _seed_question_backlog(tmp_path, item_id="high", score=2.0)
    worker = find_compatible_worker_for_backlog_item(high, [artifact])
    assert worker.worker_type == "ClaimWorker"
    ok, failures = validate_worker_for_backlog_item(worker, high, [artifact])
    assert ok, failures
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "COMPLETED"
    assert result["selected_backlog_item_id"] == "high"
    assert result["selected_worker_id"] == "atlas_v2_claim_worker_adapter"
    assert result["output_artifact_ids"]
    assert ArtifactStore(tmp_path).get_artifact(result["output_artifact_ids"][0])["artifact_type"] == "GeneratedResearchClaim"
    completed = ResearchBacklog(tmp_path).get("high")
    assert completed["state"] == "COMPLETED"
    assert completed["source_execution_id"] == result["execution_id"]
    assert list_memory_objects(tmp_path)


def test_skips_when_no_ready_backlog(tmp_path: Path):
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "SKIPPED_NO_READY_BACKLOG"


def test_research_question_maps_to_connected_claim_worker(tmp_path: Path):
    item, artifact = _seed_question_backlog(tmp_path, item_id="research-question", item_type="RESEARCH_QUESTION")
    worker = find_compatible_worker_for_backlog_item(item, [artifact])
    assert worker.worker_type == "ClaimWorker"


def test_regime_gap_uses_input_aware_fallback_when_learning_worker_cannot_accept_question(tmp_path: Path):
    item, artifact = _seed_question_backlog(tmp_path, item_id="regime-gap", item_type="REGIME_GAP")
    worker = find_compatible_worker_for_backlog_item(item, [artifact])
    assert worker.worker_type == "ExperimentDesignWorker"
    ok, failures = validate_worker_for_backlog_item(worker, item, [artifact])
    assert ok, failures


def test_skips_and_blocks_when_no_compatible_worker(tmp_path: Path):
    item, _ = _seed_question_backlog(tmp_path, item_id="unsupported", item_type="HYPOTHESIS_VALIDATION")
    assert find_compatible_worker_for_backlog_item(item, []) is None
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "SKIPPED_NO_COMPATIBLE_WORKER"
    assert ResearchBacklog(tmp_path).get("unsupported")["state"] == "BLOCKED"
    assert result["backlog_updates"][0]["state_transition_reason"] == "MISSING_GENERATED_RESEARCH_CLAIM_INPUT"
    failures = list_failures(tmp_path)
    assert any(row.get("error_type") == "MISSING_GENERATED_RESEARCH_CLAIM_INPUT" for row in failures)


def test_dry_run_selects_worker_but_creates_no_durable_research_artifact(tmp_path: Path):
    _seed_question_backlog(tmp_path)
    result = dry_run_bounded_research(tmp_path, day="2026-06-05")
    assert result["status"] == "DRY_RUN_COMPLETED"
    assert result["selected_worker_id"] == "atlas_v2_claim_worker_adapter"
    assert result["output_artifact_ids"] == []
    assert [row["artifact_type"] for row in ArtifactStore(tmp_path).list_artifacts()] == ["Question"]
    assert ResearchBacklog(tmp_path).get("bi1")["state"] == "READY"


def test_forbidden_artifact_blocks_execution(tmp_path: Path):
    _seed_question_backlog(tmp_path)
    (tmp_path / "forbidden.json").write_text('{\"artifact_type\": \"LiveTrade\"}\n', encoding="utf-8")
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "FAILED_SAFETY_GATE"
    assert not result["output_artifact_ids"]


def test_certification_failure_blocks_success(monkeypatch, tmp_path: Path):
    _seed_question_backlog(tmp_path)
    import constellation_2.common.atlas_v2_research_os.autonomous_research_execution as execution

    monkeypatch.setattr(execution, "_certification_result", lambda root, day=None: {"status": "FAIL", "warnings": [], "blockers": ["forced"]})
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "FAILED_SAFETY_GATE"
    assert ResearchBacklog(tmp_path).get("bi1")["state"] == "BLOCKED"


def test_lineage_failure_blocks_success(monkeypatch, tmp_path: Path):
    _seed_question_backlog(tmp_path)
    import constellation_2.common.atlas_v2_research_os.autonomous_research_execution as execution

    monkeypatch.setattr(execution, "_lineage_result", lambda store, worker_payload: {"status": "FAIL", "details": ["forced"]})
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "FAILED_SAFETY_GATE"


def test_governance_failure_blocks_success(monkeypatch, tmp_path: Path):
    _seed_question_backlog(tmp_path)
    import constellation_2.common.atlas_v2_research_os.autonomous_research_execution as execution

    monkeypatch.setattr(execution, "_governance_result", lambda gates, worker_payload: {"status": "FAIL", "details": ["forced"]})
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "FAILED_SAFETY_GATE"


def test_duplicate_signal_does_not_delete_artifacts(tmp_path: Path):
    _seed_question_backlog(tmp_path)
    first = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert ArtifactStore(tmp_path).exists(first["output_artifact_ids"][0])



def _create_claim_artifact(root: Path, artifact_id: str = "claim-1") -> dict:
    store = ArtifactStore(root)
    store.create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.GENERATED_RESEARCH_CLAIM.value,
        created_at=NOW,
        created_by="test",
        confidence=0.5,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"claim_text": "source-defined setup may separate from baseline", "mechanism_family": "MEAN_REVERSION"},
        is_root=True,
    )
    return store.get_artifact(artifact_id)


def _create_hypothesis_artifact(root: Path, artifact_id: str = "hyp-1") -> dict:
    claim = _create_claim_artifact(root, "claim-for-hyp")
    store = ArtifactStore(root)
    store.create_artifact(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.RESEARCH_HYPOTHESIS.value,
        created_at=NOW,
        created_by="test",
        source_artifact_ids=[claim["artifact_id"]],
        confidence=0.5,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"atlas_component_payload": {"hypothesis_id": artifact_id, "hypothesis_text": "testable hypothesis", "mechanism_family": "MEAN_REVERSION"}},
    )
    return store.get_artifact(artifact_id)



def _create_replay_artifact(root: Path, hypothesis: dict, artifact_id: str = "replay-1") -> dict:
    request = create_historical_replay_request(
        hypothesis_id=str(hypothesis["artifact_id"]),
        mechanism_tags=["MEAN_REVERSION"],
        source_artifact_ids=[hypothesis["artifact_id"]],
        replay_id=artifact_id,
        created_at=NOW,
    )
    replay = run_historical_replay(request, [{"return": value} for value in [0.02, 0.01, 0.015, -0.002, 0.018, 0.012]], created_at=NOW)
    store = ArtifactStore(root)
    store.create_artifact(
        artifact_id=artifact_id,
        artifact_type="HistoricalReplayResult",
        created_at=NOW,
        created_by="test",
        source_artifact_ids=[hypothesis["artifact_id"]],
        confidence=float(replay["metrics"]["historical_replay_score"]),
        evidence_level="HISTORICAL_REPLAY",
        labels=["historical_replay"],
        metadata={"historical_replay_result": _artifact_safe_replay_result(replay), "research_only": True},
    )
    return store.get_artifact(artifact_id)

def test_hypothesis_validation_requires_generated_research_claim(tmp_path: Path):
    item, _artifact = _seed_question_backlog(tmp_path, item_id="hv-missing", item_type="HYPOTHESIS_VALIDATION")
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "SKIPPED_NO_COMPATIBLE_WORKER"
    assert ResearchBacklog(tmp_path).get("hv-missing")["blocked_reason"] == "MISSING_GENERATED_RESEARCH_CLAIM_INPUT"


def test_hypothesis_validation_routes_only_generated_claim(tmp_path: Path):
    claim = _create_claim_artifact(tmp_path)
    _question_item, question = _seed_question_backlog(tmp_path, item_id="source-question", item_type="CLAIM_INVESTIGATION")
    ResearchBacklog(tmp_path).create_backlog_item(
        backlog_item_id="hv-valid",
        item_type="HYPOTHESIS_VALIDATION",
        title="Hypothesis",
        description="Create hypothesis",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=[claim["artifact_id"], question["artifact_id"]],
        expected_learning_value=10.0,
    )
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "COMPLETED"
    assert result["selected_worker_id"] == "atlas_v2_hypothesis_worker_adapter"
    assert result["input_artifact_ids"] == [claim["artifact_id"]]
    output = ArtifactStore(tmp_path).get_artifact(result["output_artifact_ids"][0])
    assert output["artifact_type"] == ArtifactType.RESEARCH_HYPOTHESIS.value


def test_claim_output_creates_hypothesis_validation_followup(tmp_path: Path):
    _seed_question_backlog(tmp_path, item_id="claim-source")
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "COMPLETED"
    claim_id = result["output_artifact_ids"][0]
    followups = ResearchBacklog(tmp_path).list_backlog_items(item_type="HYPOTHESIS_VALIDATION")
    assert any(row["state"] == "READY" and row["source_artifact_ids"] == [claim_id] for row in followups)


def test_research_hypothesis_output_creates_historical_replay_review(tmp_path: Path):
    claim = _create_claim_artifact(tmp_path)
    ResearchBacklog(tmp_path).create_backlog_item(
        backlog_item_id="hv-source",
        item_type="HYPOTHESIS_VALIDATION",
        title="Hypothesis",
        description="Create hypothesis",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=[claim["artifact_id"]],
        expected_learning_value=10.0,
    )
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "COMPLETED"
    hypothesis_id = result["output_artifact_ids"][0]
    followups = ResearchBacklog(tmp_path).list_backlog_items(item_type="HISTORICAL_REPLAY_REVIEW")
    assert any(row["state"] == "READY" and row["source_artifact_ids"] == [hypothesis_id] for row in followups)


def test_edge_qualification_consumes_research_hypothesis_and_creates_candidate_report(tmp_path: Path):
    hypothesis = _create_hypothesis_artifact(tmp_path)
    replay = _create_replay_artifact(tmp_path, hypothesis)
    ResearchBacklog(tmp_path).create_backlog_item(
        backlog_item_id="edge-source",
        item_type="EDGE_QUALIFICATION_REVIEW",
        title="Edge qualification",
        description="Qualify edge",
        created_at=NOW,
        created_by="test",
        state="READY",
        source_artifact_ids=[hypothesis["artifact_id"], replay["artifact_id"]],
        expected_learning_value=10.0,
    )
    result = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert result["status"] == "COMPLETED"
    assert result["selected_worker_id"] == "atlas_v2_edge_qualification"
    assert result["metadata"]["edge_qualification_attempted"] is True
    assert result["metadata"]["paper_trade_candidate_created"] is True
    assert (tmp_path / "paper_trade_candidates" / "2026-06-05" / "paper_trade_candidate_report.json").exists()


def test_end_to_end_funnel_reaches_edge_qualification(tmp_path: Path):
    _seed_question_backlog(tmp_path, item_id="claim-step", score=10.0)
    first = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert first["selected_worker_id"] == "atlas_v2_claim_worker_adapter"
    second = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert second["selected_worker_id"] == "atlas_v2_hypothesis_worker_adapter"
    third = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert third["selected_worker_id"] == "atlas_v2_historical_replay"
    assert third["metadata"]["historical_replay_attempted"] is True
    replay_id = third["output_artifact_ids"][0]
    edge_items = ResearchBacklog(tmp_path).list_backlog_items(item_type="EDGE_QUALIFICATION_REVIEW")
    assert any(row["state"] == "READY" and replay_id in row["source_artifact_ids"] for row in edge_items)
    fourth = run_bounded_research_once(tmp_path, day="2026-06-05")
    assert fourth["selected_worker_id"] == "atlas_v2_edge_qualification"
    assert fourth["metadata"]["edge_qualification_attempted"] is True
    assert replay_id in fourth["input_artifact_ids"]
    candidate = fourth["metadata"].get("paper_trade_candidate") or {}
    assert candidate.get("historical_replay_summary")
    assert "generated-only" not in " ".join(candidate.get("disqualification_reasons", [])).lower()
