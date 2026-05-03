from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from ops.tools.aegis_artifact_ledger_v1 import write_artifact_ledger_record_v1
from ops.tools import run_aegis_readiness_reconciler_v1 as reconciler


DAY = "2026-04-29"
COMMIT = "a" * 40


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _contract(path: Path) -> dict:
    return {
        "producer_name": "ops/tools/test_producer_v1.py",
        "code_version_git_commit": COMMIT,
        "source_dirty_status": "CLEAN",
        "generated_at_utc": f"{DAY}T14:00:00Z",
        "input_artifacts": [],
        "output_artifacts": [{"path": str(path.resolve())}],
    }


def _control_plane(truth: Path, *, failures: list[dict]) -> Path:
    path = truth / "reports" / "aegis_control_plane_v1" / DAY / "control_plane.v1.json"
    _write(
        path,
        {
            "schema_id": "aegis_control_plane",
            "schema_version": "aegis_control_plane.v1",
            "day_utc": DAY,
            "truth_root": str(truth.resolve()),
            "runtime_root": str(truth.parent.resolve()),
            "runtime_mode": "CANDIDATE" if truth.name == "candidate_truth" else "PRODUCTION",
            "artifact_path": str(path.resolve()),
            "actual_artifact_path": str(path.resolve()),
            "producer_contract_output_artifact_path": str(path.resolve()),
            "final_status": "NOT_READY" if failures else "READY",
            "submit_allowed": False,
            "canonical_blocker": str((failures[0] if failures else {}).get("blocking_reason") or ""),
            "failed_current_domain_dependencies": failures,
            "producer_contract_v1": _contract(path),
        },
    )
    return path


def _dep(tmp_path: Path, *, dependency_id: str = "paper_session_authority_v1", command: str = "python3 ops/tools/test_recover_v1.py") -> dict:
    return {
        "dependency_id": dependency_id,
        "domain_owner": "SESSION_IDENTITY",
        "owning_domain": "SESSION_IDENTITY",
        "required": True,
        "diagnostic_only": False,
        "expected_path": "{truth_root}/reports/" + dependency_id + "/{day_utc}/artifact.json",
        "artifact_path": "{truth_root}/reports/" + dependency_id + "/{day_utc}/artifact.json",
        "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_control_plane.v1.schema.json",
        "producer_command": command,
        "governed_producer": command,
        "recovery_command": command,
        "recovery_action": "Recover test dependency.",
        "blocking_scope": "SESSION_IDENTITY",
    }


@pytest.fixture(autouse=True)
def _stable_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconciler, "git_commit_v1", lambda: COMMIT)
    monkeypatch.setattr(reconciler, "git_dirty_status_v1", lambda: "CLEAN")

    def attach(payload: dict, *, producer_name: str, producer_command: str, input_artifacts=(), output_artifacts=(), schema_versions=None) -> dict:
        payload["producer_contract_v1"] = {
            "producer_name": producer_name,
            "producer_command": producer_command,
            "code_version_git_commit": COMMIT,
            "source_dirty_status": "CLEAN",
            "generated_at_utc": f"{DAY}T14:00:00Z",
            "input_artifacts": [],
            "output_artifacts": [{"path": str(Path(path).resolve())} for path in output_artifacts],
            "schema_versions": schema_versions or {},
        }
        return payload

    monkeypatch.setattr(reconciler, "attach_producer_contract_v1", attach)


def _plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, status: str, blocker: str, dependency_id: str = "paper_session_authority_v1", command: str = "python3 ops/tools/test_recover_v1.py", truth_name: str = "candidate_truth") -> dict:
    truth = tmp_path / truth_name
    dep = _dep(tmp_path, dependency_id=dependency_id, command=command)
    rendered_path = truth / "reports" / dependency_id / DAY / "artifact.json"
    failures = [
        {
            "dependency_id": dependency_id,
            "status": status,
            "blocking_reason": blocker,
            "artifact_path": str(rendered_path.resolve()),
            "recovery_command": command,
        }
    ]
    _control_plane(truth, failures=failures)
    monkeypatch.setattr(reconciler, "_registry_dependencies_v1", lambda: [dep])
    return reconciler.build_reconciliation_plan_v1(target_day=DAY, truth_root=truth, runtime_mode="CANDIDATE" if truth_name == "candidate_truth" else "PRODUCTION")


def test_plan_only_build_does_not_mutate_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path, monkeypatch, status="STALE", blocker="STALE_ARTIFACT_GIT_COMMIT_MISMATCH")
    truth = tmp_path / "candidate_truth"
    assert plan["ordered_actions"]
    assert plan["canonical_blocker"] == "STALE_ARTIFACT_GIT_COMMIT_MISMATCH"
    assert plan["secondary_blockers"][0]["dependency_name"] == "paper_session_authority_v1"
    assert plan["secondary_blockers"][0]["recovery_command"] == "python3 ops/tools/test_recover_v1.py"
    assert plan["promotion_packet_state"]["current_git_commit"] == COMMIT
    assert plan["repo_cleanliness"]["repo_dirty_status"] == "CLEAN"
    assert not (truth / "reports" / "aegis_readiness_reconciliation_plan_v1").exists()
    assert not (truth / "reports" / "aegis_readiness_reconciliation_report_v1").exists()


def test_stale_and_missing_derivable_artifacts_become_recoverable_actions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stale = _plan(tmp_path / "stale", monkeypatch, status="STALE", blocker="STALE_ARTIFACT_GENERATED_AT_DAY_MISMATCH")
    assert stale["ordered_actions"][0]["blocker_class"] == "RECOVERABLE_STALE_ARTIFACT"

    missing = _plan(tmp_path / "missing", monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING")
    assert missing["ordered_actions"][0]["blocker_class"] == "RECOVERABLE_MISSING_ARTIFACT"


def test_nonrecoverable_blocker_classification(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cases = {
        "NON_TRADING_DAY": "NON_RECOVERABLE_NON_TRADING_DAY",
        "OPTIONS_CAPTURE_TIMEOUT": "NON_RECOVERABLE_EXTERNAL_DATA_UNAVAILABLE",
        "HIDDEN_DEPENDENCY_DETECTED": "NON_RECOVERABLE_HIDDEN_DEPENDENCY",
        "SCHEMA_INSTANCE_INVALID": "NON_RECOVERABLE_SCHEMA_INVALID",
    }
    for blocker, expected_class in cases.items():
        plan = _plan(tmp_path / blocker, monkeypatch, status="FAIL", blocker=blocker)
        assert plan["nonrecoverable_blockers"][0]["blocker_class"] == expected_class
        assert plan["ordered_actions"] == []


def test_dirty_repo_blocks_production_commit_promotion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth = tmp_path / "production_truth"
    _control_plane(truth, failures=[])
    _write(truth / "governance" / "production_version.v1.json", {"promoted_commit": "b" * 40})
    monkeypatch.setattr(reconciler, "_registry_dependencies_v1", lambda: [])
    monkeypatch.setattr(reconciler, "git_dirty_status_v1", lambda: "DIRTY")
    plan = reconciler.build_reconciliation_plan_v1(target_day=DAY, truth_root=truth, runtime_mode="PRODUCTION")
    assert plan["nonrecoverable_blockers"][0]["blocker_code"] == "REPO_DIRTY"
    assert plan["nonrecoverable_blockers"][0]["blocker_class"] == "NON_RECOVERABLE_POLICY_DENIAL"


def test_unpromoted_commit_produces_governed_promotion_plan_only_with_approval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth = tmp_path / "production_truth"
    candidate = tmp_path / "candidate_truth"
    _control_plane(truth, failures=[])
    _write(truth / "governance" / "production_version.v1.json", {"promoted_commit": "b" * 40})
    promotion_id = f"aegis-readiness-{DAY}-{COMMIT[:12]}"
    _write(candidate / "governance" / "promotion_approvals" / f"{promotion_id}.json", {"status": "APPROVED"})
    monkeypatch.setattr(reconciler, "_registry_dependencies_v1", lambda: [])
    plan = reconciler.build_reconciliation_plan_v1(target_day=DAY, truth_root=truth, runtime_mode="PRODUCTION")
    assert any(row["blocker_class"] == "RECOVERABLE_UNPROMOTED_COMMIT" for row in plan["dependency_graph"])
    assert plan["ordered_actions"]
    assert "run_aegis_promotion_candidate_v1.py" in plan["ordered_actions"][0]["recovery_command"]


def test_manual_promotion_gate_without_approval_is_nonrecoverable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth = tmp_path / "production_truth"
    _control_plane(truth, failures=[])
    _write(truth / "governance" / "production_version.v1.json", {"promoted_commit": "b" * 40})
    monkeypatch.setattr(reconciler, "_registry_dependencies_v1", lambda: [])
    plan = reconciler.build_reconciliation_plan_v1(target_day=DAY, truth_root=truth, runtime_mode="PRODUCTION")
    assert plan["nonrecoverable_blockers"][0]["blocker_class"] == "NON_RECOVERABLE_MANUAL_APPROVAL_REQUIRED"


def test_execute_runs_only_plan_selected_governed_commands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path, monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING")
    action = plan["ordered_actions"][0]
    expected = Path(action["expected_output_path"])
    calls: list[str] = []

    def fake_run(command: str, **kwargs) -> subprocess.CompletedProcess:
        calls.append(command)
        _write(expected, {"day_utc": DAY, "truth_root": action["expected_truth_root"], "runtime_mode": action["expected_runtime_mode"], "producer_contract_v1": _contract(expected)})
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    monkeypatch.setattr(reconciler.subprocess, "run", fake_run)
    report_path, report = reconciler.execute_reconciliation_plan_v1(plan=plan, truth_root=tmp_path / "candidate_truth")
    assert report_path.exists()
    assert calls == [action["recovery_command"]]
    assert report["executed_actions"][0]["result"] == "PASS"
    assert report["safety_confirmations"]["submit_run"] is False


def test_execute_refuses_unknown_recovery_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path, monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING")
    plan["ordered_actions"][0]["recovery_command"] = "python3 ops/tools/unknown.py"
    plan["ordered_actions"][0]["allowed_to_execute"] = False
    plan["ordered_actions"][0]["reason"] = "RECOVERY_COMMAND_NOT_ALLOWLISTED"
    report_path, report = reconciler.execute_reconciliation_plan_v1(plan=plan, truth_root=tmp_path / "candidate_truth", stop_on_first_nonrecoverable=False)
    assert report_path.exists()
    assert report["failed_actions"][0]["reason"] == "ACTION_NOT_ALLOWED"


def test_execute_refuses_forbidden_submit_or_execution_package_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path, monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING", command="python3 ops/tools/run_aegis_paper_submit_v1.py --dry_run YES")
    assert plan["ordered_actions"] == []
    assert plan["skipped_actions"][0]["reason"].startswith("FORBIDDEN_COMMAND")


def test_execute_refuses_unexpected_output_path_and_wrong_root_or_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path / "missing_output", monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING")
    monkeypatch.setattr(reconciler.subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(command, 0, stdout="", stderr=""))
    _path, report = reconciler.execute_reconciliation_plan_v1(plan=plan, truth_root=tmp_path / "missing_output" / "candidate_truth")
    assert "OUTPUT_MISSING" in report["failed_actions"][0]["reason"]

    plan = _plan(tmp_path / "wrong_mode", monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING")
    action = plan["ordered_actions"][0]
    expected = Path(action["expected_output_path"])

    def wrong_mode(command: str, **kwargs) -> subprocess.CompletedProcess:
        _write(expected, {"day_utc": DAY, "truth_root": action["expected_truth_root"], "runtime_mode": "PRODUCTION", "producer_contract_v1": _contract(expected)})
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(reconciler.subprocess, "run", wrong_mode)
    _path, report = reconciler.execute_reconciliation_plan_v1(plan=plan, truth_root=tmp_path / "wrong_mode" / "candidate_truth")
    assert "RUNTIME_MODE_MISMATCH" in report["failed_actions"][0]["reason"]


def test_execute_stops_on_ledger_write_and_promotion_attestation_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path / "ledger", monkeypatch, status="MISSING", blocker="PAPER_SESSION_AUTHORITY_V1_MISSING")
    monkeypatch.setattr(reconciler, "write_artifact_ledger_record_v1", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("LEDGER_WRITE_FAILED")))
    with pytest.raises(RuntimeError, match="LEDGER_WRITE_FAILED"):
        reconciler.execute_reconciliation_plan_v1(plan=plan, truth_root=tmp_path / "ledger" / "candidate_truth")
    monkeypatch.setattr(reconciler, "write_artifact_ledger_record_v1", write_artifact_ledger_record_v1)

    truth = tmp_path / "promotion" / "production_truth"
    dep = _dep(tmp_path, dependency_id="aegis_control_plane_v1")
    rendered_path = truth / "reports" / "aegis_control_plane_v1" / DAY / "artifact.json"
    _control_plane(
        truth,
        failures=[
            {
                "dependency_id": "aegis_control_plane_v1",
                "status": "FAIL",
                "blocking_reason": "PROMOTION_ATTESTATION_MISSING",
                "artifact_path": str(rendered_path.resolve()),
                "recovery_command": dep["recovery_command"],
            }
        ],
    )
    _write(truth / "governance" / "production_version.v1.json", {"promoted_commit": COMMIT})
    monkeypatch.setattr(reconciler, "_registry_dependencies_v1", lambda: [dep])
    plan = reconciler.build_reconciliation_plan_v1(target_day=DAY, truth_root=truth, runtime_mode="PRODUCTION")
    action = plan["ordered_actions"][0]
    action["blocker_class"] = "RECOVERABLE_PROMOTION_ATTESTATION_MISSING"
    expected = Path(action["expected_output_path"])

    def produce(command: str, **kwargs) -> subprocess.CompletedProcess:
        _write(expected, {"day_utc": DAY, "truth_root": action["expected_truth_root"], "runtime_mode": action["expected_runtime_mode"], "producer_contract_v1": _contract(expected)})
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(reconciler.subprocess, "run", produce)
    monkeypatch.setattr(reconciler, "verify_promotion_attestation_v1", lambda **kwargs: [{"code": "PROMOTION_ATTESTATION_MISSING"}])
    _path, report = reconciler.execute_reconciliation_plan_v1(plan=plan, truth_root=truth)
    assert "PROMOTION_ATTESTATION" in report["failed_actions"][0]["reason"]


def test_execution_package_is_not_created_by_reconciler_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan(tmp_path, monkeypatch, status="MISSING", blocker="EXECUTION_PACKAGE_V1_MISSING", command="python3 ops/tools/run_execution_package_v1.py")
    assert plan["ordered_actions"] == []
    assert plan["skipped_actions"][0]["reason"].startswith("FORBIDDEN_COMMAND")
