from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.run_session_authority_control_plane_v1 import (
    collect_target_day_build_artifact_rows_v1,
)
import ops.tools.run_session_authority_orchestration_v1 as session_orchestration_module
from ops.tools.run_session_authority_orchestration_v1 import source_ref_blocks_build_v1
from ops.tools.run_session_authority_v1 import (
    _downstream_build_cycle_scripts_for_environment_v1,
)


DAY = "2026-04-22"
ACCOUNT = "DUO847203"


def _rows_for_artifact(rows: list[dict], artifact_id: str) -> list[dict]:
    return [row for row in rows if str(row.get("artifact_id") or "").strip() == artifact_id]


def test_paper_target_day_build_demotes_non_load_bearing_blockers(tmp_path: Path) -> None:
    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=tmp_path,
        target_day=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    demoted_artifacts = {
        "day_authority_decision_v1",
        "pre_open_bundle_v1",
        "primary_scoped_authorization_gate_verdict_v1",
        "paper_startup_authorization_convergence_v1",
        "startup_materialization_v1",
        "paper_policy_verdict_v1",
        "trade_submit_readiness_c2_v1",
    }
    for artifact_id in demoted_artifacts:
        matched = _rows_for_artifact(rows, artifact_id)
        assert matched, f"missing expected artifact row for {artifact_id}"
        assert all(bool(row.get("required")) is False for row in matched), artifact_id

    assert any(
        str(row.get("artifact_id") or "") == "market_calendar_day" and bool(row.get("required")) is True
        for row in rows
    )
    assert any(
        str(row.get("artifact_id") or "") == "trading_day_intent_generation_v1"
        and bool(row.get("required")) is True
        for row in rows
    )
    assert any(
        str(row.get("artifact_id") or "") == "intents_day_completeness_v1" and bool(row.get("required")) is True
        for row in rows
    )


def test_live_target_day_build_preserves_strict_requirements(tmp_path: Path) -> None:
    rows = collect_target_day_build_artifact_rows_v1(
        truth_root=tmp_path,
        target_day=DAY,
        environment="LIVE",
        ib_account=ACCOUNT,
    )

    strict_artifacts = {
        "day_authority_decision_v1",
        "pre_open_bundle_v1",
        "primary_scoped_authorization_gate_verdict_v1",
        "paper_startup_authorization_convergence_v1",
        "startup_materialization_v1",
        "paper_policy_verdict_v1",
        "trade_submit_readiness_c2_v1",
    }
    for artifact_id in strict_artifacts:
        matched = _rows_for_artifact(rows, artifact_id)
        assert matched, f"missing expected artifact row for {artifact_id}"
        assert all(bool(row.get("required")) is True for row in matched), artifact_id


def test_nonblocking_build_source_refs_are_paper_only() -> None:
    paper_scripts = _downstream_build_cycle_scripts_for_environment_v1("PAPER")
    live_scripts = _downstream_build_cycle_scripts_for_environment_v1("LIVE")

    paper_only_nonblocking = {
        "ops/tools/run_paper_startup_authorization_convergence_v1.py",
        "ops/tools/run_paper_policy_verdict_v1.py",
        "ops/tools/run_startup_materialization_v1.py",
    }
    assert paper_only_nonblocking.issubset(paper_scripts)
    assert paper_only_nonblocking.isdisjoint(live_scripts)

    blocking_ref = {
        "script": "ops/tools/run_paper_policy_verdict_v1.py",
        "return_code": 2,
    }
    assert (
        source_ref_blocks_build_v1(
            ref=blocking_ref,
            downstream_build_cycle_scripts=paper_scripts,
        )
        is False
    )
    assert (
        source_ref_blocks_build_v1(
            ref=blocking_ref,
            downstream_build_cycle_scripts=live_scripts,
        )
        is True
    )


def test_run_tool_v1_times_out_fail_closed(monkeypatch, tmp_path: Path) -> None:
    def _timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout"))

    monkeypatch.setattr(session_orchestration_module.subprocess, "run", _timeout_run)
    monkeypatch.setenv("C2_SESSION_AUTHORITY_TOOL_TIMEOUT_SECONDS", "1")

    result = session_orchestration_module.run_tool_v1(
        repo_root=tmp_path,
        script_relpath="ops/tools/run_some_blocking_tool.py",
        downstream_build_cycle_scripts=(),
        args=("--day_utc", DAY),
    )

    assert result["return_code"] == 124
    assert result["timed_out"] is True
    assert result["timeout_seconds"] == "1"
    assert "SESSION_AUTHORITY_TOOL_TIMEOUT" in str(result["stderr"])
    assert source_ref_blocks_build_v1(ref=result, downstream_build_cycle_scripts=()) is True


def test_run_nested_tool_with_artifacts_times_out_fail_closed(monkeypatch, tmp_path: Path) -> None:
    def _timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout"))

    monkeypatch.setattr(session_orchestration_module.subprocess, "run", _timeout_run)
    monkeypatch.setenv("C2_SESSION_AUTHORITY_TOOL_TIMEOUT_SECONDS", "1")

    result = session_orchestration_module._run_nested_tool_with_artifacts(
        repo_root=tmp_path,
        script_relpath="ops/tools/run_accounting_nav_v2_day_v1.py",
        cmd=["python3", "ops/tools/run_accounting_nav_v2_day_v1.py"],
        sleeve_truth_root=(tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"),
        expected_day_utc=DAY,
    )

    assert result["return_code"] == 124
    assert result["timed_out"] is True
    assert result["timeout_seconds"] == "1"
    assert result["validation_status"] == "MISSING"
    assert "SESSION_AUTHORITY_TOOL_TIMEOUT" in str(result["error_detail"])
