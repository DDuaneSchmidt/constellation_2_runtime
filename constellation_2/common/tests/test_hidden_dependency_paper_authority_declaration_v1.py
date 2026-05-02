from __future__ import annotations

from pathlib import Path

from ops.tools.run_session_authority_control_plane_v1 import (
    collect_target_day_build_artifact_rows_v1,
)
from ops.tools.run_session_authority_diagnostic_v1 import (
    compute_hidden_dependency_check_result_v1,
)


DAY = "2026-04-23"
ACCOUNT = "DU1234567"


def test_build_rows_declare_paper_session_authority_dependency() -> None:
    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=Path("/tmp/hidden-dependency-declaration-test"),
        target_day=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    matched = [row for row in rows if str(row.get("artifact_id") or "").strip() == "paper_session_authority_v1"]
    assert matched, "paper_session_authority_v1 must be explicitly declared in build inventory"
    assert all(bool(row.get("required")) is False for row in matched)


def test_build_rows_declare_paper_day_and_trade_readiness_decision_dependencies() -> None:
    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=Path("/tmp/hidden-dependency-paper-day-test"),
        target_day=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    for artifact_id in ("paper_trading_day_authority_v1", "trade_readiness_decision_v1", "trade_readiness_presubmit_v1"):
        matched = [row for row in rows if str(row.get("artifact_id") or "").strip() == artifact_id]
        assert matched, f"{artifact_id} must be explicitly declared in build inventory"
        assert all(bool(row.get("required")) is False for row in matched)


def test_build_rows_declare_engine_activity_authorization_dependency() -> None:
    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=Path("/tmp/hidden-dependency-engine-authorization-test"),
        target_day=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    matched = [row for row in rows if str(row.get("artifact_id") or "").strip() == "engine_activity_authorization_v1"]
    assert matched, "engine_activity_authorization_v1 must be explicitly declared in build inventory"
    assert all(bool(row.get("required")) is False for row in matched)


def test_build_rows_declare_session_authority_observed_dependencies() -> None:
    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=Path("/tmp/hidden-dependency-session-authority-test"),
        target_day=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    required_dependencies = {
        "runtime_resilience_authority_v1",
        "safety_state_authority_v1",
        "trading_day_readiness_authority_v1",
    }
    for artifact_id in required_dependencies:
        matched = [row for row in rows if str(row.get("artifact_id") or "").strip() == artifact_id]
        assert matched, f"{artifact_id} must be explicitly declared in build inventory"
        assert all(bool(row.get("required")) is True for row in matched)
        assert all(str(row.get("blocking_reason_code") or "") for row in matched)

    self_check = [row for row in rows if str(row.get("artifact_id") or "").strip() == "target_day_session_authority_v1"]
    assert self_check, "target_day_session_authority_v1 must be explicitly declared in build inventory"
    assert all(bool(row.get("required")) is False for row in self_check)


def test_hidden_dependency_check_passes_when_paper_session_authority_is_declared() -> None:
    result = compute_hidden_dependency_check_result_v1(
        repo_root=Path("/home/node/constellation"),
        artifact_results=[
            {
                "artifact_id": "paper_session_ledger_v1",
                "observed_dependency_artifacts": ["paper_session_authority_v1"],
            },
            {
                "artifact_id": "paper_session_authority_v1",
                "observed_dependency_artifacts": [],
            },
        ],
        source_refs=[],
        downstream_build_cycle_scripts=[],
    )

    assert result["status"] == "PASS"
    assert result["blocking_reason_code"] == ""
    assert result["undeclared_dependency_artifacts"] == []
    assert "paper_session_authority_v1" in result["declared_inventory_artifacts"]


def test_hidden_dependency_check_passes_when_engine_activity_authorization_is_declared() -> None:
    result = compute_hidden_dependency_check_result_v1(
        repo_root=Path("/home/node/constellation"),
        artifact_results=[
            {
                "artifact_id": "submit_boundary_status_v1",
                "observed_dependency_artifacts": ["engine_activity_authorization_v1"],
            },
            {
                "artifact_id": "engine_activity_authorization_v1",
                "result_status": "FAIL",
                "blocking_reason_code": "TARGET_DAY_ARTIFACT_MISSING",
                "observed_dependency_artifacts": [],
            },
        ],
        source_refs=[],
        downstream_build_cycle_scripts=[],
    )

    assert result["status"] == "PASS"
    assert result["blocking_reason_code"] == ""
    assert result["undeclared_dependency_artifacts"] == []
    assert "engine_activity_authorization_v1" in result["declared_inventory_artifacts"]


def test_hidden_dependency_check_passes_for_declared_paper_day_dependencies() -> None:
    result = compute_hidden_dependency_check_result_v1(
        repo_root=Path("/home/node/constellation"),
        artifact_results=[
            {
                "artifact_id": "trading_day_control_plane_v1",
                "observed_dependency_artifacts": [
                    "paper_trading_day_authority_v1",
                    "trade_readiness_decision_v1",
                    "trade_readiness_presubmit_v1",
                ],
            },
            {"artifact_id": "paper_trading_day_authority_v1", "observed_dependency_artifacts": []},
            {"artifact_id": "trade_readiness_decision_v1", "observed_dependency_artifacts": []},
            {"artifact_id": "trade_readiness_presubmit_v1", "observed_dependency_artifacts": []},
        ],
        source_refs=[],
        downstream_build_cycle_scripts=[],
    )

    assert result["status"] == "PASS"
    assert result["blocking_reason_code"] == ""
    assert result["undeclared_dependency_artifacts"] == []


def test_hidden_dependency_check_passes_for_declared_session_authority_observed_dependencies() -> None:
    result = compute_hidden_dependency_check_result_v1(
        repo_root=Path("/home/node/constellation"),
        artifact_results=[
            {
                "artifact_id": "submit_boundary_status_v1",
                "observed_dependency_artifacts": [
                    "runtime_resilience_authority_v1",
                    "safety_state_authority_v1",
                    "target_day_session_authority_v1",
                    "trading_day_readiness_authority_v1",
                ],
            },
            {"artifact_id": "runtime_resilience_authority_v1", "observed_dependency_artifacts": []},
            {"artifact_id": "safety_state_authority_v1", "observed_dependency_artifacts": []},
            {"artifact_id": "target_day_session_authority_v1", "observed_dependency_artifacts": []},
            {"artifact_id": "trading_day_readiness_authority_v1", "observed_dependency_artifacts": []},
        ],
        source_refs=[],
        downstream_build_cycle_scripts=[],
    )

    assert result["status"] == "PASS"
    assert result["blocking_reason_code"] == ""
    assert result["undeclared_dependency_artifacts"] == []


def test_hidden_dependency_check_fail_closed_for_true_undeclared_dependency() -> None:
    result = compute_hidden_dependency_check_result_v1(
        repo_root=Path("/home/node/constellation"),
        artifact_results=[
            {
                "artifact_id": "paper_session_ledger_v1",
                "observed_dependency_artifacts": ["paper_session_authority_v1"],
            }
        ],
        source_refs=[],
        downstream_build_cycle_scripts=[],
    )

    assert result["status"] == "FAIL"
    assert result["blocking_reason_code"] == "HIDDEN_DEPENDENCY_DETECTED"
    assert result["undeclared_dependency_artifacts"] == ["paper_session_authority_v1"]
