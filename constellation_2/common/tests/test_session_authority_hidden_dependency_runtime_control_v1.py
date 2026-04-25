from __future__ import annotations

from pathlib import Path
import sys


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_session_authority_v1 import (  # noqa: E402
    _compute_hidden_dependency_check_result,
    _payload_dependencies,
)


def test_submit_boundary_dependencies_exclude_runtime_control_local_evidence() -> None:
    deps = _payload_dependencies(
        "submit_boundary_status_v1",
        {
            "required_boundary_checks": [
                {"logical_name": "target_day_build_v1"},
                {"logical_name": "target_day_admission_v1"},
                {"logical_name": "runtime_control_record_v1"},
                {"logical_name": "startup_materialization_v1"},
                {"logical_name": "paper_trading_posture_v1"},
                {"logical_name": "global_kill_switch_state_v1"},
                {"logical_name": "trade_submit_readiness_c2_v1"},
            ]
        },
    )

    assert "target_day_build_v1" not in deps
    assert "target_day_admission_v1" not in deps
    assert "runtime_control_record_v1" not in deps
    assert deps == [
        "global_kill_switch_state_v1",
        "paper_trading_posture_v1",
        "startup_materialization_v1",
        "trade_submit_readiness_c2_v1",
    ]


def test_hidden_dependency_check_ignores_submit_boundary_runtime_control_local_evidence() -> None:
    artifact_results = [
        {
            "artifact_id": "submit_boundary_status_v1",
            "observed_dependency_artifacts": _payload_dependencies(
                "submit_boundary_status_v1",
                {
                    "required_boundary_checks": [
                        {"logical_name": "runtime_control_record_v1"},
                        {"logical_name": "startup_materialization_v1"},
                        {"logical_name": "paper_trading_posture_v1"},
                        {"logical_name": "global_kill_switch_state_v1"},
                        {"logical_name": "trade_submit_readiness_c2_v1"},
                    ]
                },
            ),
        },
        {"artifact_id": "startup_materialization_v1", "observed_dependency_artifacts": []},
        {"artifact_id": "paper_trading_posture_v1", "observed_dependency_artifacts": []},
        {"artifact_id": "global_kill_switch_state_v1", "observed_dependency_artifacts": []},
        {"artifact_id": "trade_submit_readiness_c2_v1", "observed_dependency_artifacts": []},
    ]

    result = _compute_hidden_dependency_check_result(artifact_results=artifact_results, source_refs=[])

    assert result["status"] == "PASS"
    assert result["undeclared_dependency_artifacts"] == []
    assert "runtime_control_record_v1" not in result["observed_dependency_artifacts"]
