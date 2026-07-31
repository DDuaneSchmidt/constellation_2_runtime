from constellation_2.common.atlas_v2_research_os.autonomous_research_models import (
    AutonomousResearchExecutionResult,
    AutonomousResearchExecutionStatus,
    AutonomousResearchExecutionStep,
    AutonomousResearchExecutionRun,
    AutonomousResearchSafetyGateResult,
)


def test_autonomous_research_statuses_include_required_values():
    assert {item.value for item in AutonomousResearchExecutionStatus} == {
        "PENDING",
        "RUNNING",
        "COMPLETED",
        "DRY_RUN_COMPLETED",
        "FAILED",
        "FAILED_SAFETY_GATE",
        "SKIPPED_NO_READY_BACKLOG",
        "SKIPPED_NO_COMPATIBLE_WORKER",
        "SKIPPED_DEPENDENCY_MISSING",
    }


def test_execution_models_serialize_required_fields():
    run = AutonomousResearchExecutionRun(
        execution_id="exec-1",
        created_at="2026-06-05T00:00:00Z",
        started_at="2026-06-05T00:00:00Z",
        completed_at="2026-06-05T00:00:01Z",
        status="COMPLETED",
    ).to_dict()
    result = AutonomousResearchExecutionResult(**{k: run[k] for k in ["execution_id", "created_at", "started_at", "completed_at", "status"]}).to_dict()
    step = AutonomousResearchExecutionStep("select", "PASS", "a", "b").to_dict()
    gate = AutonomousResearchSafetyGateResult("gate", "PASS").to_dict()
    for payload in [run, result]:
        assert "memory_updates" in payload
        assert "backlog_updates" in payload
        assert "certification_result" in payload
        assert "governance_result" in payload
        assert "lineage_result" in payload
    assert step["step_id"] == "select"
    assert gate["result"] == "PASS"
