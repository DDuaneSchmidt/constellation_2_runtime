from pathlib import Path

from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.worker_adapters import (
    AtlasClaimWorkerAdapter,
    AtlasEvaluationWorkerAdapter,
    AtlasExperimentDesignWorkerAdapter,
    AtlasHypothesisWorkerAdapter,
    AtlasLearningWorkerAdapter,
)

NOW = "2026-06-05T00:00:00Z"


def _question(store: ArtifactStore):
    return store.create_artifact(
        artifact_id="q1",
        artifact_type=ArtifactType.QUESTION.value,
        created_at=NOW,
        created_by="test",
        confidence=0.4,
        evidence_level="GENERATED_ONLY",
        labels=["generated", "source_label"],
        metadata={
            "question": "When source-defined setup is observed, does follow-through separate from baseline?",
            "mechanism_family": "MEAN_REVERSION",
        },
        is_root=True,
    ).to_dict()


def test_claim_worker_executes_through_adapter_and_preserves_lineage_and_evidence(tmp_path: Path):
    store = ArtifactStore(tmp_path)
    question = _question(store)
    result = AtlasClaimWorkerAdapter().run([question], metadata={"root": str(tmp_path)})
    assert result.status == "COMPLETED"
    claim = store.get_artifact(result.output_artifact_ids[0])
    assert claim["artifact_type"] == "GeneratedResearchClaim"
    assert claim["source_artifact_ids"] == ["q1"]
    assert claim["evidence_level"] == "GENERATED_ONLY"
    assert "source_label" in claim["labels"]


def test_connected_research_worker_chain_preserves_source_references(tmp_path: Path):
    store = ArtifactStore(tmp_path)
    claim_result = AtlasClaimWorkerAdapter().run([_question(store)], metadata={"root": str(tmp_path)})
    claim = store.get_artifact(claim_result.output_artifact_ids[0])

    hyp_result = AtlasHypothesisWorkerAdapter().run([claim], metadata={"root": str(tmp_path)})
    hypothesis = store.get_artifact(hyp_result.output_artifact_ids[0])
    assert hypothesis["source_artifact_ids"] == [claim["artifact_id"]]
    assert hypothesis["metadata"]["source_claim_artifact_id"] == claim["artifact_id"]

    spec_result = AtlasExperimentDesignWorkerAdapter().run([hypothesis], metadata={"root": str(tmp_path)})
    spec = store.get_artifact(spec_result.output_artifact_ids[0])
    assert spec["artifact_type"] == "CheapExperimentSpec"
    assert spec["metadata"]["source_hypothesis_artifact_id"] == hypothesis["artifact_id"]
    assert spec["metadata"]["execution_requested"] is False

    experiment_result = store.create_artifact(
        artifact_id="er1",
        artifact_type=ArtifactType.EXPERIMENT_RESULT.value,
        created_at=NOW,
        created_by="test",
        source_artifact_ids=[spec["artifact_id"]],
        confidence=0.6,
        evidence_level="GENERATED_ONLY",
        labels=["generated"],
        metadata={"actual_learning_value": 0.6, "importance_score": 0.7},
    ).to_dict()
    learning_result = AtlasLearningWorkerAdapter().run([experiment_result], metadata={"root": str(tmp_path)})
    learning = store.get_artifact(learning_result.output_artifact_ids[0])
    assert learning["artifact_type"] == "LearningEstimate"
    assert learning["source_artifact_ids"] == ["er1"]
    assert learning["metadata"]["evidence_maturity_level"] == "GENERATED_ONLY"

    evaluation_result = AtlasEvaluationWorkerAdapter().run([learning, experiment_result], metadata={"root": str(tmp_path)})
    evaluation = store.get_artifact(evaluation_result.output_artifact_ids[0])
    assert evaluation["artifact_type"] == "LearningEstimateEvaluation"
    assert set(evaluation["source_artifact_ids"]) == {learning["artifact_id"], "er1"}
    assert evaluation["metadata"]["auditability_preserved"] is True
