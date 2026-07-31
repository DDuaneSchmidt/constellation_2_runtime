from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType
from constellation_2.common.atlas_v2_research_os.worker_adapters import create_default_worker_adapters


def test_default_worker_adapters_have_expected_connection_statuses():
    adapters = create_default_worker_adapters()
    assert len(adapters) == 8
    statuses = {adapter.worker_id: adapter.adapter_status for adapter in adapters}
    assert statuses["atlas_v2_claim_worker_adapter"] == "CONNECTED_RESEARCH_ONLY"
    assert statuses["atlas_v2_hypothesis_worker_adapter"] == "CONNECTED_RESEARCH_ONLY"
    assert statuses["atlas_v2_experiment_design_worker_adapter"] == "CONNECTED_RESEARCH_ONLY"
    assert statuses["atlas_v2_learning_worker_adapter"] == "CONNECTED_RESEARCH_ONLY"
    assert statuses["atlas_v2_evaluation_worker_adapter"] == "CONNECTED_RESEARCH_ONLY"
    assert statuses["atlas_v2_experiment_execution_worker_adapter"] == "NOT_CONNECTED"
    assert statuses["atlas_v2_attention_worker_adapter"] == "NOT_CONNECTED"
    assert statuses["atlas_v2_memory_curator_worker_adapter"] == "NOT_CONNECTED"
    for adapter in adapters:
        result = adapter.dry_run([], metadata={"report_mode": "worker_interface_audit"})
        assert result.output_artifact_ids == []
        assert result.metadata["design_status"] == "DESIGN_READY"
        assert result.metadata["behavior_changed"] is False


def test_adapter_input_validation_rejects_unsupported_artifacts():
    adapter = create_default_worker_adapters()[0]
    ok, failures = adapter.validate_inputs([{"artifact_id": "h1", "artifact_type": ArtifactType.RESEARCH_HYPOTHESIS.value}])
    assert not ok
    assert "unsupported input artifact type: ResearchHypothesis" in failures


def test_adapter_output_validation_requires_lineage_for_outputs():
    adapter = create_default_worker_adapters()[0]
    input_artifact = {"artifact_id": "q1", "artifact_type": ArtifactType.QUESTION.value, "evidence_level": "GENERATED_ONLY"}
    output_artifact = {"artifact_id": "c1", "artifact_type": ArtifactType.GENERATED_RESEARCH_CLAIM.value, "evidence_level": "GENERATED_ONLY", "source_artifact_ids": []}
    ok, failures = adapter.validate_outputs([output_artifact], [input_artifact])
    assert not ok
    assert any("source_artifact_ids" in failure for failure in failures)
