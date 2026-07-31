from constellation_2.common.atlas_v2_research_os.artifact_models import ArtifactType, EvidenceLevel
from constellation_2.common.atlas_v2_research_os.worker_governance import validate_worker_lineage, validate_worker_output_governance


def test_worker_governance_rejects_forbidden_artifact_types():
    result = validate_worker_output_governance([], [{"artifact_id": "bad", "artifact_type": "CapitalAllocation", "evidence_level": EvidenceLevel.GENERATED_ONLY.value}])
    assert result.status == "FAIL"
    assert any("forbidden artifact type" in violation for violation in result.violations)


def test_worker_governance_rejects_evidence_maturity_escalation():
    input_artifacts = [{"artifact_id": "claim-1", "artifact_type": ArtifactType.GENERATED_RESEARCH_CLAIM.value, "evidence_level": EvidenceLevel.GENERATED_ONLY.value}]
    output_artifacts = [{"artifact_id": "learning-1", "artifact_type": ArtifactType.LEARNING_ESTIMATE.value, "evidence_level": EvidenceLevel.HISTORICAL_REPLAY.value, "source_artifact_ids": ["claim-1"]}]
    result = validate_worker_output_governance(input_artifacts, output_artifacts)
    assert result.status == "FAIL"
    assert any("escalate evidence maturity" in violation for violation in result.violations)


def test_worker_governance_rejects_generated_learning_external_validation():
    result = validate_worker_output_governance(
        [],
        [
            {
                "artifact_id": "learning-1",
                "artifact_type": ArtifactType.LEARNING_ESTIMATE.value,
                "evidence_level": EvidenceLevel.EXTERNALLY_VALIDATED.value,
                "labels": ["generated"],
            }
        ],
    )
    assert result.status == "FAIL"
    assert any("externally validated" in violation for violation in result.violations)


def test_worker_governance_rejects_candidate_or_capital_authority():
    result = validate_worker_output_governance(
        [],
        [{"artifact_id": "signal-1", "artifact_type": ArtifactType.ATTENTION_SIGNAL.value, "evidence_level": EvidenceLevel.GENERATED_ONLY.value, "metadata": {"authorizes_candidate_use": True}}],
    )
    assert result.status == "FAIL"
    assert any("candidate/capital use" in violation for violation in result.violations)


def test_worker_lineage_requires_output_to_preserve_inputs():
    result = validate_worker_lineage(
        [{"artifact_id": "claim-1", "artifact_type": ArtifactType.GENERATED_RESEARCH_CLAIM.value}],
        [{"artifact_id": "hypothesis-1", "artifact_type": ArtifactType.RESEARCH_HYPOTHESIS.value, "source_artifact_ids": []}],
    )
    assert result.status == "FAIL"
    assert any("must preserve" in violation for violation in result.violations)
