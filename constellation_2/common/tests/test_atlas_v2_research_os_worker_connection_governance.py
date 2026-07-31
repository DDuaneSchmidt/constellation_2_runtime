from constellation_2.common.atlas_v2_research_os.worker_adapters import AtlasClaimWorkerAdapter
from constellation_2.common.atlas_v2_research_os.worker_governance import (
    validate_connected_worker_no_authority_escalation,
    validate_connected_worker_outputs,
    validate_worker_connection,
)


def test_validate_worker_connection_passes_for_connected_claim_worker():
    result = validate_worker_connection(AtlasClaimWorkerAdapter())
    assert result.status == "PASS"


def test_forbidden_artifact_rejected():
    result = validate_connected_worker_outputs([
        {"artifact_id": "bad", "artifact_type": "LiveTrade", "source_artifact_ids": ["q1"], "evidence_level": "GENERATED_ONLY"}
    ], [{"artifact_id": "q1", "artifact_type": "Question", "evidence_level": "GENERATED_ONLY"}])
    assert result.status == "FAIL"
    assert any("not allowed" in item or "forbidden" in item for item in result.violations)


def test_authority_escalation_rejected():
    result = validate_connected_worker_no_authority_escalation({
        "artifact_id": "bad",
        "artifact_type": "GeneratedResearchClaim",
        "metadata": {"authorizes_capital_use": True},
    })
    assert result.status == "FAIL"
