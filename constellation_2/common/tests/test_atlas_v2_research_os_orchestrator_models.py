from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.orchestrator_models import OrchestratorRunRecord, OrchestratorStatus, SafetyGateId, SafetyGateResult


def test_orchestrator_status_values_are_canonical() -> None:
    assert {item.value for item in OrchestratorStatus} == {
        "PENDING",
        "RUNNING",
        "COMPLETED",
        "FAILED",
        "FAILED_SAFETY_GATE",
        "SKIPPED_ALREADY_RUNNING",
        "DRY_RUN_COMPLETED",
        "AUDIT_ONLY_COMPLETED",
    }


def test_run_record_contains_required_ledger_fields() -> None:
    record = OrchestratorRunRecord(
        run_id="run-1",
        started_at="2026-06-05T00:00:00Z",
        completed_at="2026-06-05T00:00:01Z",
        status=OrchestratorStatus.COMPLETED.value,
        trigger_type="MANUAL",
        trigger_source={"operator": "test"},
    ).to_dict()
    assert set(record) == {
        "run_id",
        "started_at",
        "completed_at",
        "status",
        "trigger_type",
        "trigger_source",
        "selected_backlog_items",
        "workers_invoked",
        "artifacts_created",
        "safety_gate_results",
        "lineage_validation_result",
        "errors",
        "skip_reason",
        "duration_seconds",
    }


def test_safety_gate_result_serializes_required_gate_names() -> None:
    result = SafetyGateResult(gate_id=SafetyGateId.FORBIDDEN_ARTIFACT_AUDIT.value, result="PASS").to_dict()
    assert result["gate_id"] == "forbidden_artifact_audit"
    assert result["gate_version"] == "atlas_v2_research_os_orchestrator_gate_v1"
