from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.orchestrator_governance import run_safety_gates, safety_gates_passed


def test_safety_gates_pass_for_research_only_inputs(tmp_path: Path) -> None:
    results = run_safety_gates(tmp_path / "research_os", selected_backlog_items=[{"backlog_item_id": "bi-1", "labels": ["generated_only"], "evidence_level": "GENERATED_ONLY"}], runtime_truth={"autonomous_execution_allowed": False, "trade_advice_allowed": False}, verified_graph={"graph_status": "READY"})
    assert safety_gates_passed(results) is True


def test_safety_gates_reject_forbidden_artifact_markers(tmp_path: Path) -> None:
    results = run_safety_gates(tmp_path / "research_os", selected_backlog_items=[{"backlog_item_id": "bi-1", "title": "Create CapitalAllocation"}], runtime_truth={"autonomous_execution_allowed": False, "trade_advice_allowed": False}, verified_graph={"graph_status": "READY"})
    assert safety_gates_passed(results) is False
    assert any(row["gate_id"] == "forbidden_artifact_audit" and row["result"] == "FAIL" for row in results)


def test_safety_gates_reject_operator_approved_as_evidence(tmp_path: Path) -> None:
    results = run_safety_gates(tmp_path / "research_os", selected_backlog_items=[{"backlog_item_id": "bi-1", "evidence_level": "OPERATOR_APPROVED"}], runtime_truth={"autonomous_execution_allowed": False, "trade_advice_allowed": False}, verified_graph={"graph_status": "READY"})
    assert any(row["gate_id"] == "evidence_level_validation" and row["result"] == "FAIL" for row in results)
