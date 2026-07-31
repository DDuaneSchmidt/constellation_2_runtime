from pathlib import Path

from constellation_2.common.atlas_v2_research_os.autonomous_research_governance import (
    validate_autonomous_research_candidate_isolation,
    validate_autonomous_research_execution_allowed,
    validate_autonomous_research_no_authority_escalation,
    validate_autonomous_research_no_forbidden_artifacts,
    validate_autonomous_research_worker_outputs,
)


def test_bounded_execution_allowed_requires_one_shot():
    assert validate_autonomous_research_execution_allowed({"bounded_once": True, "mode": "run_once"})["result"] == "PASS"
    assert validate_autonomous_research_execution_allowed({"bounded_once": False})["result"] == "FAIL"
    assert validate_autonomous_research_execution_allowed({"bounded_once": True, "scheduler_enabled": True})["result"] == "FAIL"


def test_authority_and_candidate_isolation_block_escalation():
    assert validate_autonomous_research_no_authority_escalation({"metadata": {"capital_authorized": True}})["result"] == "FAIL"
    assert validate_autonomous_research_no_authority_escalation({"recommendation": "Trade this."})["result"] == "FAIL"
    assert validate_autonomous_research_candidate_isolation({"metadata": {"candidate_generation_modified": True}})["result"] == "FAIL"


def test_forbidden_artifact_scan_blocks_payload(tmp_path: Path):
    (tmp_path / "bad.json").write_text('{\"artifact_type\": \"LiveTrade\"}\n', encoding="utf-8")
    assert validate_autonomous_research_no_forbidden_artifacts(tmp_path)["result"] == "FAIL"


def test_worker_output_validation_requires_lineage():
    input_artifacts = [{"artifact_id": "q1", "artifact_type": "Question", "evidence_level": "GENERATED_ONLY"}]
    outputs = [{"artifact_id": "c1", "artifact_type": "GeneratedResearchClaim", "evidence_level": "GENERATED_ONLY", "source_artifact_ids": []}]
    result = validate_autonomous_research_worker_outputs(input_artifacts, outputs)
    assert result["result"] == "FAIL"
