from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.candidate_quality_evaluation import evaluate_candidate_quality
from constellation_2.common.atlas_v2_research_os.failure_patterns import record_failure_pattern, increment_failure_repetition
from constellation_2.common.atlas_v2_research_os.learning_feedback_engine import (
    apply_memory_priority_adjustments,
    create_backlog_items_from_memory,
    derive_priority_adjustments_from_memory,
    record_candidate_quality_feedback_to_memory,
)
from constellation_2.common.atlas_v2_research_os.memory_index import add_memory_object, list_memory_objects
from constellation_2.common.atlas_v2_research_os.memory_models import MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType, create_memory_object
from constellation_2.common.atlas_v2_research_os.regime_context import create_regime_context
from constellation_2.common.atlas_v2_research_os.research_backlog import ResearchBacklog

NOW = "2026-06-04T00:00:00Z"


def _artifact(root: Path) -> None:
    store = ArtifactStore(root)
    if not store.exists("art1"):
        store.create_artifact(artifact_id="art1", artifact_type=ArtifactType.QUESTION.value, created_at=NOW, created_by="test", is_root=True)


def _memory(root: Path, memory_id: str, *, state: str = "NEW", evidence: str = "HISTORICAL_REPLAY", labels: list[str] | None = None, text: str = "opening range claim", regimes: list[str] | None = None) -> None:
    _artifact(root)
    add_memory_object(create_memory_object(memory_id=memory_id, memory_type=MemoryType.CLAIM_CLUSTER.value, created_at=NOW, source_artifact_ids=["art1"], mechanism_tags=["OPENING_RANGE"], regime_context_ids=regimes or [], evidence_level=evidence, lifecycle_state=state, labels=labels or [], metadata={"canonical_text": text}), root)


def _quality_pair(repeated_baseline: int, repeated_treatment: int, *, comparable: bool = True) -> tuple[dict, dict]:
    window = {"start": "2026-06-01", "end": "2026-06-04"}
    baseline = {"baseline_id": "b", "created_at": NOW, "created_by": "test", "source_artifact_ids": ["art1"], "raw_signals": 100, "generated_candidates": 20, "rejected_candidates": 40, "gate_suppressions": 10, "portfolio_scoring_rejections": 10, "portfolio_scoring_passes": 10, "evidence_levels": ["HISTORICAL_REPLAY"], "hypotheses_tested": 10, "hypotheses_not_falsified": 5, "repeated_failures": repeated_baseline, "failure_categories": ["REGIME_MISMATCH"], "measurement_window": window, "signal_universe_id": "u1", "candidate_factory_version": "cf1"}
    treatment = dict(baseline, treatment_id="t", generated_candidates=25, rejected_candidates=35, portfolio_scoring_passes=12, learning_input_ids=["mem1"], research_os_memory_ids=["mem1"], repeated_failures=repeated_treatment)
    if not comparable:
        treatment["candidate_factory_version"] = "cf2"
    return baseline, treatment


def test_repeated_failure_increases_failure_analysis_priority(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _artifact(root)
    failure = record_failure_pattern(root=root, failure_id="f1", failure_type="REGIME_MISMATCH", source_artifact_ids=["art1"], mechanism_tags=["OPENING_RANGE"], reason="repeat", evidence_level="HISTORICAL_REPLAY", first_seen_at=NOW)
    increment_failure_repetition(root, failure["failure_id"], seen_at=NOW)
    records = derive_priority_adjustments_from_memory(root, created_at=NOW)
    assert any(record["influence_type"] == "FAILURE_ANALYSIS" and record["metadata"]["failure_reduction_score_delta"] > 0 for record in records)


def test_duplicate_lowers_novelty(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _memory(root, "mem1", text="opening range claim")
    _memory(root, "mem2", text="opening drive claim")
    records = derive_priority_adjustments_from_memory(root, created_at=NOW)
    assert any(record["influence_type"] == "DUPLICATE_REVIEW" and record["metadata"]["novelty_score_delta"] < 0 for record in records)


def test_retired_and_quarantined_memory_cannot_influence_priority(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _memory(root, "retired", state=MemoryLifecycleState.RETIRED.value)
    _memory(root, "quarantined", state=MemoryLifecycleState.QUARANTINED.value)
    records = derive_priority_adjustments_from_memory(root, created_at=NOW)
    blocked = [record for record in records if set(record["source_memory_ids"]) & {"retired", "quarantined"}]
    assert blocked
    assert all(record["governance_status"] == "BLOCKED" for record in blocked)


def test_generated_only_influence_remains_labeled(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _memory(root, "mem1", evidence=MemoryEvidenceMaturity.GENERATED_ONLY.value, labels=["generated_only"])
    record = [r for r in derive_priority_adjustments_from_memory(root, created_at=NOW) if "mem1" in r["source_memory_ids"]][0]
    assert record["evidence_level"] == "GENERATED_ONLY"


def test_memory_to_backlog_mappings_and_idempotency(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _artifact(root)
    create_regime_context(root=root, regime_context_id="reg1", labels=["UNKNOWN"], source_artifact_ids=["art1"], created_at=NOW)
    _memory(root, "mem1", regimes=["reg1"])
    _memory(root, "stale", state=MemoryLifecycleState.STALE.value)
    _memory(root, "dup1", text="opening range claim")
    _memory(root, "dup2", text="opening drive claim")
    failure = record_failure_pattern(root=root, failure_id="f1", failure_type="REGIME_MISMATCH", source_artifact_ids=["art1"], mechanism_tags=["OPENING_RANGE"], reason="repeat", evidence_level="HISTORICAL_REPLAY", first_seen_at=NOW)
    increment_failure_repetition(root, failure["failure_id"], seen_at=NOW)
    first = create_backlog_items_from_memory(root, created_at=NOW)
    second = create_backlog_items_from_memory(root, created_at=NOW)
    item_types = {row["item_type"] for row in first}
    assert {"FAILURE_ANALYSIS", "DUPLICATE_REVIEW", "REGIME_GAP", "STALE_LEARNING_REVIEW"} <= item_types
    assert len(ResearchBacklog(root).list_backlog_items()) == len({row["backlog_item_id"] for row in second})


def test_candidate_quality_to_memory_records_reduction_regression_and_non_comparable_review(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _memory(root, "mem1")
    baseline, treatment = _quality_pair(10, 6)
    eval_reduction = evaluate_candidate_quality(baseline, treatment, evaluation_id="cq1", source_artifact_ids=["art1"])
    record = record_candidate_quality_feedback_to_memory(root, eval_reduction, created_at=NOW)
    assert record["metadata"]["repeated_failure_changes"]["status"] == "REDUCTION"
    assert record["metadata"]["certification_result"]["live_use_authorized"] is False
    baseline, treatment = _quality_pair(6, 10)
    eval_regression = evaluate_candidate_quality(baseline, treatment, evaluation_id="cq2", source_artifact_ids=["art1"])
    assert record_candidate_quality_feedback_to_memory(root, eval_regression, created_at=NOW)["metadata"]["repeated_failure_changes"]["status"] == "REGRESSION"
    baseline, treatment = _quality_pair(10, 8, comparable=False)
    eval_review = evaluate_candidate_quality(baseline, treatment, evaluation_id="cq3", source_artifact_ids=["art1"])
    assert record_candidate_quality_feedback_to_memory(root, eval_review, created_at=NOW)["metadata"]["review_backlog_required"] is True
    assert any(row["memory_id"] == "mem-cq-feedback-cq1" for row in list_memory_objects(root))


def test_closed_loop_priority_ranking_and_lineage(tmp_path: Path) -> None:
    root = tmp_path / "store"
    _artifact(root)
    ResearchBacklog(root).create_backlog_item(backlog_item_id="low", item_type="RESEARCH_QUESTION", title="low", description="low", created_at=NOW, created_by="test", source_artifact_ids=["art1"], state="READY", expected_learning_value=0.1)
    failure = record_failure_pattern(root=root, failure_id="f1", failure_type="REGIME_MISMATCH", source_artifact_ids=["art1"], mechanism_tags=["OPENING_RANGE"], reason="repeat", evidence_level="HISTORICAL_REPLAY", first_seen_at=NOW)
    increment_failure_repetition(root, failure["failure_id"], seen_at=NOW)
    create_backlog_items_from_memory(root, created_at=NOW)
    apply_memory_priority_adjustments(root, created_at=NOW)
    top = ResearchBacklog(root).get_top_priority_items(1)[0]
    assert top["item_type"] == "FAILURE_ANALYSIS"
    assert "f1" in top["source_memory_ids"]
